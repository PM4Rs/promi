use promi::stream::{buffer, stats, xes, StreamSink};
use promi::util::expand_static;
use promi::Log;
use std::fs;
use std::io;
use std::path;

/// Stream XES string to stdout
///
/// XES string > XesReader | XesWriter > stdout
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

    let mut reader = xes::XesReader::from(io::BufReader::new(s.as_bytes()));
    let mut writer = xes::XesWriter::new(io::stdout(), None, None);

    println!("read XES string to stdout");
    writer.consume(&mut reader).unwrap();

    println!("\n");
}

/// Store XES file stream in log, convert log to stream buffer and stream it to stdout
///
/// file > XesReader > Log | Buffer > XesWriter > stdout
///
fn example_2() {
    let path = expand_static(&["xes", "book", "L1.xes"]);
    let f = io::BufReader::new(fs::File::open(&path).unwrap());

    print!("read {:?} to log and gather statistics: ", &path);
    let mut log = Log::default();
    let reader = xes::XesReader::from(f);
    let mut stats = stats::StreamStats::new(reader);
    log.consume(&mut stats).unwrap();
    println!("{:?}", &log);

    print!("convert log to stream buffer: ");
    let mut buffer: buffer::Buffer = log.into();
    println!("{:?}", &buffer);

    println!("stream buffer to stdout:");
    let mut writer = xes::XesWriter::new(io::stdout(), None, None);
    writer.consume(&mut buffer).unwrap();

    println!("\n");
    println!("{}", stats)
}

fn main() {
    example_1();
    example_2();
}
