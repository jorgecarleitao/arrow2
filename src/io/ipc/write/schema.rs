use arrow_format::ipc;
use arrow_format::ipc::flatbuffers::FlatBufferBuilder;

use crate::datatypes::*;

use super::super::convert;

/// Converts
pub fn schema_to_bytes(schema: &Schema) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    let schema = {
        let fb = convert::schema_to_fb_offset(&mut fbb, schema);
        fb.as_union_value()
    };

    let mut message = ipc::Message::MessageBuilder::new(&mut fbb);
    message.add_version(ipc::Schema::MetadataVersion::V5);
    message.add_header_type(ipc::Message::MessageHeader::Schema);
    message.add_bodyLength(0);
    message.add_header(schema);
    // TODO: custom metadata
    let data = message.finish();
    fbb.finish(data, None);

    fbb.finished_data().to_vec()
}
