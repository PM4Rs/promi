use std::fs::File;
use std::io::{BufReader, stdout};
use std::path::Path;

use promi::stream::{
    AnyArtifact, buffer, StreamSink, xes,
};
use promi::stream::log::Log;
use promi::stream::observer::{Handler, Observer};
use promi::stream::stats::{Statistics, StatsCollector};
use promi::stream::validator::Validator;
use promi::stream::void::consume;

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

    print!("read {:?} to log and count components: ", &path);
    let mut log = Log::default();
    let mut reader = xes::XesReader::from(file);
    log.consume(&mut reader).unwrap();

    print!("convert log to stream buffer: ");
    let buffer: buffer::Buffer = log.into();

    println!("stream buffer via observer to stdout:");
    let mut observer = Observer::new(buffer);
    observer.register(StatsCollector::default());

    let mut writer = xes::XesWriter::new(stdout(), None, None);
    let artifacts = writer.consume(&mut observer).unwrap();

    println!(
        "\n\n{}",
        AnyArtifact::find::<Statistics>(&mut artifacts.iter().flatten()).unwrap()
    )
}

/// Store XES file stream in log, convert log to stream buffer and stream it to stdout
///
/// file > XesReader > Validator
///
fn example_3() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("static/xes/book/bigger-example.xes");
    let file = BufReader::new(File::open(&path).unwrap());

    let reader = xes::XesReader::from(file);
    let mut validator = Validator::default().into_observer(reader);

    consume(&mut validator).unwrap();
}

fn main() {
    example_1();
    example_2();
    example_3();
}
