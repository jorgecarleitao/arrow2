use std::io::Cursor;

use avro_rs::types::Record;
use criterion::*;

use arrow2::error::Result;
use arrow2::io::avro::read;
use avro_rs::*;
use avro_rs::{Codec, Schema as AvroSchema};

fn schema() -> AvroSchema {
    let raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "string"}
        ]
    }
"#;
    AvroSchema::parse_str(raw_schema).unwrap()
}

fn write(size: usize, has_codec: bool) -> Result<Vec<u8>> {
    let avro = schema();
    // a writer needs a schema and something to write to
    let mut writer: Writer<Vec<u8>>;
    if has_codec {
        writer = Writer::with_codec(&avro, Vec::new(), Codec::Deflate);
    } else {
        writer = Writer::new(&avro, Vec::new());
    }

    (0..size).for_each(|_| {
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", "foo");
        writer.append(record).unwrap();
    });

    Ok(writer.into_inner().unwrap())
}

fn read_batch(buffer: &[u8], size: usize) -> Result<()> {
    let mut file = Cursor::new(buffer);

    let (avro_schema, schema, codec, file_marker) = read::read_metadata(&mut file)?;

    let reader = read::Reader::new(
        read::Decompressor::new(
            read::BlockStreamIterator::new(&mut file, file_marker),
            codec,
        ),
        avro_schema,
        schema.fields,
    );

    let mut rows = 0;
    for maybe_batch in reader {
        let batch = maybe_batch?;
        rows += batch.len();
    }
    assert_eq!(rows, size);
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("avro_read");

    for log2_size in (10..=20).step_by(2) {
        let size = 2usize.pow(log2_size);
        let buffer = write(size, false).unwrap();

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("utf8", log2_size), &buffer, |b, buffer| {
            b.iter(|| read_batch(buffer, size).unwrap())
        });
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
