use promi::stream::stats::StreamStats;
use promi::stream::{buffer, stats, xes, Log, Observer, StreamSink};
use promi::util::expand_static;
use std::fs;
use std::io;

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
    let mut reader = xes::XesReader::from(io::BufReader::new(s.as_bytes()));
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
    let path = expand_static(&["xes", "book", "L1.xes"]);
    let f = io::BufReader::new(fs::File::open(&path).unwrap());

    print!("read {:?} to log and count elements: ", &path);
    let mut log = Log::default();
    let reader = xes::XesReader::from(f);
    let mut counter = stats::Counter::new(reader);
    log.consume(&mut counter).unwrap();
    println!("{:?}", &log);

    print!("convert log to stream buffer: ");
    let buffer: buffer::Buffer = log.into();
    println!("{:?}", &buffer);

    println!("stream buffer via observer to stdout:");
    let mut observer = Observer::new(buffer);
    observer.register(StreamStats::default());

    let mut writer = xes::XesWriter::new(io::stdout(), None, None);
    writer.consume(&mut observer).unwrap();

    println!("\n");
    println!("{}", counter);
    println!("{}", observer.release().unwrap());
}

fn main() {
    example_1();
    example_2();
}
