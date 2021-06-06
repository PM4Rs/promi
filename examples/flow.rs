use std::fs;

use simple_logger::SimpleLogger;

use promi::stream::flow::{Graph, Segment, ThreadExecutor};
use promi::stream::stats::Statistics;
use promi::stream::Attribute;
use promi::Result;

#[rustfmt::skip]
fn via_builder() -> Result<Graph> {
    let mut fg = Graph::default();

    fg.source(
        "Train",
        Segment::new("XesReader").attribute(Attribute::new("path", "static/xes/book/bigger-example.xes")),
    )
    .stream(Segment::new("Repair"))?
    .stream(Segment::new("Validator"))?
    .stream(Segment::new("Statistics").emit_artifact("raw_stats"))?
    .stream(
        Segment::new("Sample")
            .attribute(Attribute::new("ratio", 0.1))
            .attribute(Attribute::new("seed", 0)),
    )?
    .stream(Segment::new("Statistics").emit_artifact("sample_stats"))?
    .stream(
        Segment::new("Split")
            .attribute(Attribute::new("ratio", 0.8))
            .attribute(Attribute::new("seed", 0))
            .emit_stream("test"),
    )?
    .stream(Segment::new("Statistics").emit_artifact("train_stats"))?
    .sink(
        Segment::new("XesWriter")
            .attribute(Attribute::new("path", "/tmp/train.xes"))
            .attribute(Attribute::new("indent", 1)),
    )?;

    fg.source("Test", Segment::new("Receiver").acquire_stream("test"))
        .stream(Segment::new("Statistics").emit_artifact("test_stats"))?
        .sink(Segment::new("XesWriter").attribute(Attribute::new("path", "/tmp/test.xes")))?;

    Ok(fg)
}

fn via_yaml() -> Result<Graph> {
    let buffer = fs::read("static/flow/flow.yml").unwrap();
    Ok(serde_yaml::from_slice(&buffer).unwrap())
}

fn via_json() -> Result<Graph> {
    let buffer = fs::read("static/flow/flow.json").unwrap();
    Ok(serde_json::from_slice(&buffer).unwrap())
}

fn main() -> Result<()> {
    SimpleLogger::new().init().unwrap();

    let fg_ref = via_builder()?;
    let fg_j = via_json()?;
    let mut fg_y = via_yaml()?;

    assert_eq!(&fg_ref.pipes, &fg_j.pipes);
    assert_eq!(&fg_y.pipes, &fg_j.pipes);

    fg_y.execute(&mut ThreadExecutor::default())?;

    let params = &["raw_stats", "sample_stats", "train_stats", "test_stats"];

    for name in params.iter() {
        let stats = fg_y
            .artifacts
            .get(*name)
            .unwrap()
            .downcast_ref::<Statistics>()
            .unwrap();
        println!("{}\n{}", name, stats);
    }

    Ok(())
}
