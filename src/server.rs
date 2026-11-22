use crate::{
    drpc::{Envelope, Frame, Metadata},
    metadata::{headers, metadata},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use buffa::Message;
use connectrpc::{CodecFormat, ConnectError, Dispatcher, MethodKind, Payload, RequestContext};
use event_listener::Event;
use futures::{
    FutureExt, StreamExt,
    channel::mpsc,
    future::{AbortHandle, Abortable, LocalBoxFuture},
};
use futures_timer::Delay;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    num::NonZeroU32,
    rc::Rc,
    time::Duration,
};

const OPEN: u32 = 1;
const CLOSE: u32 = 2;
const RESET: u32 = 4;
const PING: u32 = 8;
const WINDOW: u32 = 16;
const REQUEST: u32 = OPEN | CLOSE;
const MAX_CALLS: usize = 4096;
const REQUEST_WINDOW: usize = 16;
const MAX_REQUEST_BYTES: usize = 4 << 20;
const PEER_QUEUE_BYTES: usize = 16 << 20;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Incoming {
    data: Vec<u8>,
    size: usize,
    queued: Arc<AtomicUsize>,
}
impl Drop for Incoming {
    fn drop(&mut self) {
        self.queued.fetch_sub(self.size, Ordering::Relaxed);
    }
}

struct Credit(Cell<Option<u32>>);
impl Credit {
    fn new(advertised: u32) -> Self {
        Self(Cell::new(NonZeroU32::new(advertised).map(NonZeroU32::get)))
    }
    fn available(&self) -> bool {
        self.0.get() != Some(0)
    }
    fn spend(&self) {
        self.0.set(self.0.get().map(|n| n - 1));
    }
    fn grant(&self, n: u32) {
        self.0.set(self.0.get().map(|v| v.saturating_add(n)));
    }
}

struct Call {
    sid: u32,
    input: RefCell<Option<mpsc::Sender<Incoming>>>,
    next_input: Cell<u32>,
    seq: Cell<u32>,
    abort: AbortHandle,
    credit: Credit,
}

struct Peer {
    epoch: NonZeroU32,
    client: Cell<Option<NonZeroU32>>,
    closed: Cell<bool>,
    queued: Arc<AtomicUsize>,
    calls: RefCell<HashMap<u32, Rc<Call>>>,
    credit: Credit,
    changed: Event,
    dispatcher: Rc<dyn Dispatcher>,
    send: Box<dyn Fn(Vec<u8>)>,
    spawn: Box<dyn Fn(LocalBoxFuture<'static, ()>)>,
}

/// One session per reliable channel. The adapter must call close on transport death.
/// The send callback preserves message boundaries. The executor schedules tasks locally.
pub struct Session(Rc<Peer>);

impl Session {
    pub fn new(
        epoch: NonZeroU32,
        service: impl Dispatcher,
        send: impl Fn(Vec<u8>) + 'static,
        spawn: impl Fn(LocalBoxFuture<'static, ()>) + 'static,
    ) -> Self {
        Self(Rc::new(Peer {
            epoch,
            client: Cell::new(None),
            closed: Cell::new(false),
            calls: RefCell::default(),
            queued: Default::default(),
            credit: Credit::new(0),
            changed: Event::new(),
            dispatcher: Rc::new(service),
            send: Box::new(send),
            spawn: Box::new(spawn),
        }))
    }

    pub fn receive(&self, bytes: &[u8]) -> Result<(), buffa::DecodeError> {
        if self.0.closed.get() {
            return Ok(());
        }
        if bytes.is_empty() {
            self.close();
            return Ok(());
        }
        for frame in Envelope::decode_from_slice(bytes)?.frames {
            self.0.handle(frame);
        }
        Ok(())
    }

    pub fn close(&self) {
        if !self.0.closed.replace(true) {
            for (_, call) in self.0.calls.borrow_mut().drain() {
                call.abort.abort();
            }
        }
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.close();
    }
}

impl Peer {
    fn write(&self, frame: Frame) {
        if !self.closed.get() {
            (self.send)(
                Envelope {
                    frames: vec![frame],
                    ..Default::default()
                }
                .encode_to_vec(),
            );
        }
    }

    fn reply(&self, sid: u32, seq: u32, flags: u32) -> Frame {
        Frame {
            epoch: self.epoch.get(),
            peer_epoch: self.client.get().map_or(0, NonZeroU32::get),
            sid,
            seq,
            flags,
            ..Default::default()
        }
    }

    fn reset(&self, f: &Frame) {
        self.write(Frame {
            epoch: f.epoch,
            peer_epoch: f.peer_epoch,
            sid: f.sid,
            flags: RESET,
            ..Default::default()
        });
    }

    fn terminal(&self, sid: u32, seq: u32, result: Result<Frame, ConnectError>) {
        let mut frame = self.reply(sid, seq, CLOSE);
        match result {
            Ok(reply) => {
                frame.code = Some(0);
                frame.payload = reply.payload;
                frame.header = reply.header;
                frame.trailer = reply.trailer;
            }
            Err(err) => {
                frame.code = Some(err.code.grpc_code());
                frame.desc = err.message.clone().unwrap_or_default();
                frame.header = metadata(err.response_headers()).into();
                frame.trailer = metadata(err.trailers()).into();
                for detail in err.details {
                    if let Ok(value) = STANDARD.decode(detail.value.unwrap_or_default()) {
                        frame.details.push(buffa_types::google::protobuf::Any {
                            type_url: detail.type_url,
                            value: value.into(),
                            ..Default::default()
                        });
                    }
                }
            }
        }
        if frame.header.as_option().is_none() {
            frame.header = Metadata::default().into();
        }
        self.write(frame)
    }

    fn handle(self: &Rc<Self>, f: Frame) {
        let Some(epoch) = NonZeroU32::new(f.epoch) else {
            return;
        };
        if f.flags == RESET {
            if f.seq == 0
                && epoch == self.epoch
                && (f.peer_epoch == 0
                    || Some(f.peer_epoch) == self.client.get().map(NonZeroU32::get))
            {
                if let Some(call) = self.calls.borrow_mut().remove(&f.sid) {
                    call.abort.abort();
                }
            }
            return;
        }
        if f.peer_epoch != 0 {
            return;
        }
        if self.client.get().is_some_and(|e| e != epoch) {
            return;
        }
        if f.sid == 0 {
            if f.flags == WINDOW && f.seq == 0 && self.client.get().is_some() {
                self.credit.grant(f.window);
                self.changed.notify(usize::MAX);
            }
            return;
        }
        if f.flags & OPEN != 0 {
            if f.seq != 1 || f.method.is_empty() || f.code.is_some() {
                return;
            }
            if self.client.get().is_none() {
                self.client.set(Some(epoch));
                self.credit.0.set(Credit::new(f.conn_window).0.get());
            }
            if self.calls.borrow().contains_key(&f.sid) {
                return;
            }
            if f.flags != OPEN && f.flags != REQUEST {
                let error = if f.flags == (REQUEST | 32) && !f.compressor.is_empty() {
                    ConnectError::unimplemented("unsupported compressor")
                } else {
                    ConnectError::internal("unsupported request flags")
                };
                self.terminal(f.sid, 1, Err(error));
            } else {
                self.start(f);
            }
            return;
        }
        let call = self.calls.borrow().get(&f.sid).cloned();
        let Some(call) = call else {
            if f.flags != WINDOW {
                self.reset(&f)
            };
            return;
        };
        match f.flags {
            WINDOW if f.seq == 0 => {
                call.credit.grant(f.window);
                self.changed.notify(usize::MAX);
            }
            CLOSE if f.code.is_some() && f.seq >= 2 => {
                self.calls.borrow_mut().remove(&f.sid);
                call.abort.abort();
            }
            0 | CLOSE
                if f.seq == call.next_input.get()
                    && call.input.borrow().is_some()
                    && f.code.is_none() =>
            {
                let end = f.flags == CLOSE;
                let status = match f.payload {
                    Some(data) => self.enqueue(&call, data),
                    None if end => Ok(()),
                    None => Err(ConnectError::invalid_argument("missing stream message")),
                };
                if let Err(err) = status {
                    self.calls.borrow_mut().remove(&f.sid);
                    call.abort.abort();
                    self.terminal(f.sid, call.seq.get(), Err(err));
                } else {
                    call.next_input.set(f.seq.saturating_add(1));
                    if end {
                        call.input.borrow_mut().take();
                    }
                }
            }
            PING if f.seq == 0 => {}
            _ => {
                self.calls.borrow_mut().remove(&f.sid);
                call.abort.abort();
                self.terminal(
                    f.sid,
                    call.seq.get(),
                    Err(ConnectError::internal("unexpected call frame")),
                );
            }
        }
    }

    fn enqueue(&self, call: &Call, data: Vec<u8>) -> Result<(), ConnectError> {
        let size = data.capacity();
        if data.len() > MAX_REQUEST_BYTES
            || self
                .queued
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    v.checked_add(size).filter(|n| *n <= PEER_QUEUE_BYTES)
                })
                .is_err()
        {
            return Err(ConnectError::resource_exhausted(
                "request stream receive limit",
            ));
        }
        let input = Incoming {
            data,
            size,
            queued: self.queued.clone(),
        };
        call.input
            .borrow_mut()
            .as_mut()
            .ok_or_else(|| ConnectError::invalid_argument("request stream is closed"))?
            .try_send(input)
            .map_err(|_| ConnectError::resource_exhausted("request receive window exceeded"))
    }

    fn start(self: &Rc<Self>, mut f: Frame) {
        let error = |err| self.terminal(f.sid, 1, Err(err));
        let path = f.method.strip_prefix('/').unwrap_or("");
        let Some(method) = self.dispatcher.lookup(path) else {
            error(ConnectError::unimplemented("unknown method"));
            return;
        };
        let request_stream = matches!(
            method.kind,
            MethodKind::ClientStreaming | MethodKind::BidiStreaming
        );
        if !request_stream && (f.flags != REQUEST || f.payload.is_none()) {
            error(ConnectError::invalid_argument("expected single request"));
            return;
        }
        if !f.compressor.is_empty() {
            error(ConnectError::unimplemented("unsupported compressor"));
            return;
        }
        let format = match f.codec.as_str() {
            "" | "proto" => CodecFormat::Proto,
            "json" => CodecFormat::Json,
            _ => {
                error(ConnectError::unimplemented("unsupported codec"));
                return;
            }
        };
        let ctx = match headers(&f.header) {
            Ok(h) => RequestContext::new(h)
                .with_spec(method.spec)
                .with_path(f.method.clone()),
            Err(e) => {
                error(e);
                return;
            }
        };
        let timeout = match f.timeout.as_option() {
            None => None,
            Some(d) if d.seconds >= 0 && (0..1_000_000_000).contains(&d.nanos) => {
                Some(Duration::new(d.seconds as u64, d.nanos as u32))
            }
            Some(_) => {
                error(ConnectError::invalid_argument("invalid timeout"));
                return;
            }
        };
        if self.calls.borrow().len() >= MAX_CALLS {
            error(ConnectError::resource_exhausted("too many active calls"));
            return;
        }
        let (abort, reg) = AbortHandle::new_pair();
        let (input, requests) = mpsc::channel(REQUEST_WINDOW - 1);
        let call = Rc::new(Call {
            input: RefCell::new(request_stream.then_some(input)),
            next_input: Cell::new(2),
            sid: f.sid,
            seq: Cell::new(1),
            abort,
            credit: Credit::new(f.window),
        });
        self.calls.borrow_mut().insert(f.sid, call.clone());
        if request_stream {
            if let Some(data) = f.payload.take()
                && let Err(err) = self.enqueue(&call, data)
            {
                self.calls.borrow_mut().remove(&f.sid);
                error(err);
                return;
            }
            if f.flags & CLOSE != 0 {
                call.input.borrow_mut().take();
            }
        }
        if request_stream || method.kind == MethodKind::ServerStreaming {
            let mut header = self.reply(f.sid, 1, 0);
            if request_stream {
                header.window = REQUEST_WINDOW as u32;
            }
            self.write(header);
            call.seq.set(2);
        }
        let peer = self.clone();
        let path = path.to_owned();
        let data = f.payload.take().unwrap_or_default().into();
        let (consumed, mut grants) = mpsc::unbounded();
        if request_stream {
            let weak = Rc::downgrade(self);
            let sid = f.sid;
            (self.spawn)(
                async move {
                    while grants.next().await.is_some() {
                        let Some(peer) = weak.upgrade() else { break };
                        if !peer.calls.borrow().contains_key(&sid) {
                            break;
                        }
                        let mut frame = peer.reply(sid, 0, WINDOW);
                        frame.window = 1;
                        peer.write(frame);
                    }
                }
                .boxed_local(),
            );
        }
        let requests: connectrpc::dispatcher::RequestStream =
            Box::pin(requests.map(move |mut item| {
                let _ = consumed.unbounded_send(());
                Ok(std::mem::take(&mut item.data).into())
            }));
        let work = async move {
            let dispatch = async {
                if matches!(method.kind, MethodKind::Unary | MethodKind::ClientStreaming) {
                    let res = if request_stream {
                        peer.dispatcher
                            .call_client_streaming(&path, ctx, requests, format)
                            .await?
                    } else {
                        peer.dispatcher
                            .call_unary(&path, ctx, Payload::new(data, format), format)
                            .await?
                    };
                    return Ok(Frame {
                        payload: Some(res.body.to_vec()),
                        header: metadata(&res.headers).into(),
                        trailer: metadata(&res.trailers).into(),
                        ..Default::default()
                    });
                }
                let mut res = if request_stream {
                    peer.dispatcher
                        .call_bidi_streaming(&path, ctx, requests, format)
                        .await?
                } else {
                    peer.dispatcher
                        .call_server_streaming(&path, ctx, data, format)
                        .await?
                };
                let md = metadata(&res.headers);
                // A response can signal subscription readiness before its first item.
                // H is a sequenced, payload-free frame and consumes no message credit.
                let mut first = md.entries.is_empty();
                if !first {
                    let seq = call.seq.get();
                    let mut header = peer.reply(call.sid, seq, 0);
                    call.seq.set(
                        seq.checked_add(1).ok_or_else(|| {
                            ConnectError::resource_exhausted("sequence exhausted")
                        })?,
                    );
                    header.header = md.clone().into();
                    peer.write(header);
                }
                while let Some(item) = res.body.next().await {
                    let bytes = item?;
                    let wait = async {
                        loop {
                            let change = peer.changed.listen();
                            if peer.credit.available() && call.credit.available() {
                                break;
                            }
                            change.await;
                        }
                    };
                    futures::select_biased! {
                        _=wait.fuse()=>(),
                        _=Delay::new(Duration::from_secs(30)).fuse()=>return Err(ConnectError::unavailable("flow control stalled")),
                    }
                    peer.credit.spend();
                    call.credit.spend();
                    let seq = call.seq.get();
                    call.seq.set(
                        seq.checked_add(1).ok_or_else(|| {
                            ConnectError::resource_exhausted("sequence exhausted")
                        })?,
                    );
                    let mut frame = peer.reply(call.sid, seq, 0);
                    if first {
                        frame.header = md.clone().into();
                        first = false;
                    }
                    frame.payload = Some(bytes.to_vec());
                    peer.write(frame);
                }
                Ok(Frame {
                    header: md.into(),
                    trailer: metadata(&res.trailers).into(),
                    ..Default::default()
                })
            };
            let result = match timeout {
                Some(duration) => futures::select_biased! {
                    _=sleep(duration).fuse()=>Err(ConnectError::deadline_exceeded("call deadline exceeded")),
                    result=dispatch.fuse()=>result,
                },
                None => dispatch.await,
            };
            peer.calls.borrow_mut().remove(&call.sid);
            peer.terminal(call.sid, call.seq.get(), result);
        };
        (self.spawn)(
            async move {
                let _ = Abortable::new(work, reg).await;
            }
            .boxed_local(),
        );
    }
}

// Browser timers accept signed 32-bit milliseconds. Keep long budgets intact.
async fn sleep(mut duration: Duration) {
    let max = Duration::from_millis(i32::MAX as u64);
    while duration > max {
        Delay::new(max).await;
        duration -= max;
    }
    if !duration.is_zero() {
        Delay::new(duration).await;
    }
}
