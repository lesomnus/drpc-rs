use buffa::Message;
use buffa_types::Empty;
use connectrpc::{Response, Router, handler_fn};
use drpc_rs::{
    Session,
    drpc::{Envelope, Frame},
};
use futures::{executor::LocalPool, task::LocalSpawnExt};
use std::{cell::RefCell, rc::Rc};

fn request(sid: u32, method: &str) -> Frame {
    Frame {
        epoch: 7,
        sid,
        seq: 1,
        flags: 3,
        method: method.into(),
        payload: Some(Vec::new()),
        ..Default::default()
    }
}
fn send(session: &Session, frames: Vec<Frame>) {
    session
        .receive(
            &Envelope {
                frames,
                ..Default::default()
            }
            .encode_to_vec(),
        )
        .unwrap();
}
#[test]
fn empty_message_and_error_can_share_an_input_envelope() {
    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    let replies = Rc::new(RefCell::new(Vec::new()));
    let sink = replies.clone();
    let router = Router::new().route(
        "unit.Service",
        "Echo",
        handler_fn(|_, _: Empty| async { Response::ok(Empty::default()) }),
    );
    let server = Session::new(
        19.try_into().unwrap(),
        router,
        move |b| {
            sink.borrow_mut()
                .extend(Envelope::decode_from_slice(&b).unwrap().frames)
        },
        move |task| spawner.spawn_local(task).unwrap(),
    );
    send(
        &server,
        vec![
            request(1, "/unit.Service/Echo"),
            request(2, "/unit.Service/Missing"),
        ],
    );
    pool.run_until_stalled();
    let mut frames = replies.borrow_mut();
    frames.sort_by_key(|f| f.sid);
    assert_eq!(frames.len(), 2);
    assert_eq!(
        (
            frames[0].epoch,
            frames[0].peer_epoch,
            frames[0].seq,
            frames[0].flags
        ),
        (19, 7, 1, 2)
    );
    assert_eq!(
        (frames[0].code, frames[0].payload.as_deref()),
        (Some(0), Some(&[][..]))
    );
    assert_eq!(
        (frames[1].code, frames[1].payload.as_deref()),
        (Some(12), None)
    );
}
#[test]
fn client_abort_and_port_close_drop_pending_calls() {
    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    let router = Router::new().route(
        "unit.Service",
        "Wait",
        handler_fn(|_, _: Empty| async {
            futures::future::pending::<()>().await;
            Response::ok(Empty::default())
        }),
    );
    let server = Session::new(
        19.try_into().unwrap(),
        router,
        |_| panic!("canceled handler emitted a response"),
        move |task| spawner.spawn_local(task).unwrap(),
    );
    send(
        &server,
        vec![
            request(1, "/unit.Service/Wait"),
            request(2, "/unit.Service/Wait"),
        ],
    );
    pool.run_until_stalled();
    send(
        &server,
        vec![Frame {
            epoch: 7,
            sid: 1,
            seq: 2,
            flags: 2,
            code: Some(1),
            ..Default::default()
        }],
    );
    server.receive(&[]).unwrap();
    pool.run(); // fails to finish if either handler still owns its pending future
}

#[test]
fn request_streams_half_close_and_live_responses() {
    use buffa_types::Timestamp;
    use connectrpc::{ServiceStream, bidi_streaming_handler_fn, client_streaming_handler_fn};
    use futures::StreamExt;
    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    let replies = Rc::new(RefCell::new(Vec::new()));
    let sink = replies.clone();
    let router = Router::new()
        .route_client_stream(
            "unit.Service",
            "Sum",
            client_streaming_handler_fn(|_, mut input: ServiceStream<Timestamp>| async move {
                let mut n = 0;
                while let Some(v) = input.next().await {
                    n += v?.seconds;
                }
                Response::ok(Timestamp {
                    seconds: n,
                    ..Default::default()
                })
            }),
        )
        .route_bidi_stream(
            "unit.Service",
            "Double",
            bidi_streaming_handler_fn(|_, input: ServiceStream<Timestamp>| async move {
                Response::ok(
                    input
                        .map(|v| {
                            v.map(|mut n| {
                                n.seconds *= 2;
                                n
                            })
                        })
                        .boxed(),
                )
            }),
        );
    let server = Session::new(
        19.try_into().unwrap(),
        router,
        move |b| {
            sink.borrow_mut()
                .extend(Envelope::decode_from_slice(&b).unwrap().frames)
        },
        move |task| spawner.spawn_local(task).unwrap(),
    );
    for (sid, method) in [(1, "Sum"), (2, "Double")] {
        let mut open = request(sid, &format!("/unit.Service/{method}"));
        open.flags = 1;
        open.payload = None;
        send(&server, vec![open]);
        pool.run_until_stalled();
        assert!(
            replies
                .borrow()
                .iter()
                .any(|f| f.sid == sid && f.seq == 1 && f.window > 0)
        );
        for i in 1..=35 {
            send(
                &server,
                vec![Frame {
                    epoch: 7,
                    sid,
                    seq: 1 + i,
                    payload: Some(
                        Timestamp {
                            seconds: i as i64,
                            ..Default::default()
                        }
                        .encode_to_vec(),
                    ),
                    ..Default::default()
                }],
            );
            pool.run_until_stalled();
            if sid == 2 {
                let frames = replies.borrow();
                assert!(
                    frames.iter().any(|f| f.sid == 2
                        && f.payload
                            .as_ref()
                            .is_some_and(|b| Timestamp::decode_from_slice(b).unwrap().seconds
                                == (i * 2) as i64))
                );
            }
        }
        send(
            &server,
            vec![Frame {
                epoch: 7,
                sid,
                seq: 37,
                flags: 2,
                ..Default::default()
            }],
        );
        pool.run_until_stalled();
        let frames = replies.borrow();
        let last = frames
            .iter()
            .find(|f| f.sid == sid && f.flags == 2)
            .unwrap();
        assert_eq!(last.code.unwrap_or(0), 0);
        if sid == 1 {
            assert_eq!(
                Timestamp::decode_from_slice(last.payload.as_ref().unwrap())
                    .unwrap()
                    .seconds,
                630
            );
        }
    }
}
