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
