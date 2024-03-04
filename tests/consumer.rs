extern crate faktory;
extern crate mockstream;

mod mock;

use faktory::*;
use std::io;
use std::thread;

#[test]
fn terminate() {
    let mut s = mock::Stream::new(2);
    let mut c = ConsumerBuilder::default();
    c.wid("wid".to_string());
    c.register("foobar", |_| -> io::Result<()> {
        loop {
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

    s.push_bytes_to_read(0, b"+{\"state\":\"terminate\"}\r\n");

    let _ = jh.join();
}
