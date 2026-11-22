use crate::Session;
use connectrpc::Dispatcher;
use js_sys::Uint8Array;
use std::{num::NonZeroU32, rc::Rc};
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use web_sys::{MessageEvent, MessagePort};

/// Owns the event listeners and one server session. Keep this value alive while serving.
pub struct PortServer {
    port: MessagePort,
    session: Rc<Session>,
    _receive: Closure<dyn FnMut(MessageEvent)>,
    _failed: Closure<dyn FnMut()>,
}
impl PortServer {
    pub fn bind(port: MessagePort, epoch: NonZeroU32, service: impl Dispatcher) -> Self {
        let tx = port.clone();
        let session = Rc::new_cyclic(|weak: &std::rc::Weak<Session>| {
            let weak = weak.clone();
            Session::new(
                epoch,
                service,
                move |bytes| {
                    if tx
                        .post_message(&Uint8Array::from(bytes.as_slice()))
                        .is_err()
                    {
                        if let Some(session) = weak.upgrade() {
                            session.close();
                        }
                    }
                },
                wasm_bindgen_futures::spawn_local,
            )
        });
        let rx = session.clone();
        let receive = Closure::new(move |e: MessageEvent| {
            let data = e.data();
            if data.is_instance_of::<Uint8Array>() || data.is_instance_of::<js_sys::ArrayBuffer>() {
                let _ = rx.receive(&Uint8Array::new(&data).to_vec());
            }
        });
        let rx = session.clone();
        let failed = Closure::new(move || rx.close());
        port.set_onmessage(Some(receive.as_ref().unchecked_ref()));
        port.set_onmessageerror(Some(failed.as_ref().unchecked_ref()));
        let _ = port.add_event_listener_with_callback("close", failed.as_ref().unchecked_ref());
        port.start();
        Self {
            port,
            session,
            _receive: receive,
            _failed: failed,
        }
    }
}
impl Drop for PortServer {
    fn drop(&mut self) {
        self.session.close();
        self.port.set_onmessage(None);
        self.port.set_onmessageerror(None);
        let _ = self
            .port
            .remove_event_listener_with_callback("close", self._failed.as_ref().unchecked_ref());
        let _ = self
            .port
            .post_message(&JsValue::from(Uint8Array::new_with_length(0)));
        self.port.close();
    }
}
