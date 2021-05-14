use flatbuffers::FlatBufferBuilder;

use crate::datatypes::*;

use super::super::{convert, gen};
use super::MetadataVersion;

/// Converts
pub fn schema_to_bytes(schema: &Schema, version: MetadataVersion) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    let schema = {
        let fb = convert::schema_to_fb_offset(&mut fbb, schema);
        fb.as_union_value()
    };

    let mut message = gen::Message::MessageBuilder::new(&mut fbb);
    message.add_version(version);
    message.add_header_type(gen::Message::MessageHeader::Schema);
    message.add_bodyLength(0);
    message.add_header(schema);
    // TODO: custom metadata
    let data = message.finish();
    fbb.finish(data, None);

    fbb.finished_data().to_vec()
}
