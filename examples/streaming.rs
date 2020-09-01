use promi::stream::{
    buffer,
    observer::Observer,
    stats::{Counter, StreamStats},
    xes, Log, StreamSink,
};
use std::fs::File;
use std::io::{stdout, BufReader};
use std::path::Path;

/// Stream XES string to stdout
///
/// XES string > XesReader | XesWriter > byte buffer > stdout
///
fn example_1() {
    let s = r#"
        <?xml version="1.0" encoding="UTF-8"?>
        <!-- XES test file for promi -->
        <log xes.version="1849.2016" xes.features="">
            <list key="foo">
                <values>
                    <string key="foo" value="bar"/>
                    <string key="foo" value="bar"/>
                    <string key="foo" value="bar"/>
                </values>
            </list>
            <trace>
                <string key="concept:name" value="Case2.0"/>
                <event>
                    <string key="concept:name" value="a"/>
                </event>
                <event>
                    <string key="concept:name" value="c"/>
                </event>
                <event>
                    <string key="concept:name" value="b"/>
                </event>
                <event>
                    <string key="concept:name" value="d"/>
                </event>
            </trace>
        </log>
    "#;

    let buffer: Vec<u8> = Vec::new();
    let mut reader = xes::XesReader::from(BufReader::new(s.as_bytes()));
    let mut writer = xes::XesWriter::new(buffer, None, None);

    println!("parse XES string and render it to byte buffer");
    writer.consume(&mut reader).unwrap();

    println!("{}", std::str::from_utf8(writer.inner()).unwrap());
    println!("\n");
}

/// Store XES file stream in log, convert log to stream buffer and stream it to stdout
///
/// file > XesReader > Log | Buffer > XesWriter > stdout
///
fn example_2() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/xes/book/L1.xes");
    let file = BufReader::new(File::open(&path).unwrap());

    print!("read {:?} to log and count elements: ", &path);
    let mut log = Log::default();
    let reader = xes::XesReader::from(file);
    let mut counter = Counter::new(reader);
    log.consume(&mut counter).unwrap();
    println!("{:?}", &log);

    print!("convert log to stream buffer: ");
    let buffer: buffer::Buffer = log.into();
    println!("{:?}", &buffer);

    println!("stream buffer via observer to stdout:");
    let mut observer = Observer::new(buffer);
    observer.register(StreamStats::default());

    let mut writer = xes::XesWriter::new(stdout(), None, None);
    writer.consume(&mut observer).unwrap();

    println!("\n");
    println!("{}", counter);
    println!("{}", observer.release().unwrap());
}

fn main() {
    example_1();
    example_2();
}
