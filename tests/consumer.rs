extern crate faktory;
extern crate mockstream;
extern crate serde_json;
extern crate url;

mod mock;

use faktory::*;
use std::io;
use std::thread;
use std::time::Duration;

#[test]
fn terminate() {
    let mut s = mock::Stream::new(2);
    let mut c = ConsumerBuilder::default();
    c.wid("wid".to_string());
    c.register("foobar", |_| -> io::Result<()> {
        loop {
            thread::sleep(Duration::from_secs(5));
        }
    });
    let mut c = c.connect_with(s.clone(), None).unwrap();
    s.ignore(0);

    s.push_bytes_to_read(
        1,
        b"$186\r\n\
        {\
        \"jid\":\"forever\",\
        \"queue\":\"default\",\
        \"jobtype\":\"foobar\",\
        \"args\":[],\
        \"created_at\":\"2017-11-01T21:02:35.772981326Z\",\
        \"enqueued_at\":\"2017-11-01T21:02:35.773318394Z\",\
        \"reserve_for\":600,\
        \"retry\":25\
        }\r\n",
    );

    let jh = thread::spawn(move || c.run(&["default"]));

    // the running thread won't ever return, because the job never exits. the heartbeat thingy is
    // going to eventually send a heartbeat, and we want to respond to that with a "terminate"
    s.push_bytes_to_read(0, b"+{\"state\":\"terminate\"}\r\n");

    // at this point, c.run() should immediately return with Ok(1) indicating that one job is still
    // running.
    assert_eq!(jh.join().unwrap().unwrap(), 1);

    // heartbeat should have seen one beat (terminate) and then send FAIL
    let written = s.pop_bytes_written(0);
    let beat = b"BEAT {\"wid\":\"wid\"}\r\nFAIL ";
    assert_eq!(&written[0..beat.len()], &beat[..]);
    assert!(written.ends_with(b"\r\nEND\r\n"));
    println!(
        "{}",
        std::str::from_utf8(&written[beat.len()..(written.len() - b"\r\nEND\r\n".len())]).unwrap()
    );
    let written: serde_json::Value =
        serde_json::from_slice(&written[beat.len()..(written.len() - b"\r\nEND\r\n".len())])
            .unwrap();
    assert_eq!(
        written
            .as_object()
            .and_then(|o| o.get("jid"))
            .and_then(|v| v.as_str()),
        Some("forever")
    );

    // worker should have just fetched once
    let written = s.pop_bytes_written(1);
    let msgs = "\r\n\
                FETCH default\r\n";
    assert_eq!(
        std::str::from_utf8(&written[(written.len() - msgs.len())..]).unwrap(),
        msgs
    );
}
