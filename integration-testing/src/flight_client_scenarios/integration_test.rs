// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{read_json_file, ArrowFile};

use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::*,
    io::ipc::{
        read::{self, Dictionaries},
        write, IpcSchema,
    },
    io::{
        flight::{self, deserialize_batch, serialize_batch},
        ipc::IpcField,
    },
};
use arrow_format::flight::service::flight_service_client::FlightServiceClient;
use arrow_format::ipc;
use arrow_format::{
    flight::data::{
        flight_descriptor::DescriptorType, FlightData, FlightDescriptor, Location, Ticket,
    },
    ipc::planus::ReadAsRoot,
};
use futures::{channel::mpsc, sink::SinkExt, stream, StreamExt};
use tonic::{Request, Streaming};

use std::sync::Arc;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

type Client = FlightServiceClient<tonic::transport::Channel>;

type ChunkArc = Chunk<Arc<dyn Array>>;

pub async fn run_scenario(host: &str, port: u16, path: &str) -> Result {
    let url = format!("http://{}:{}", host, port);

    let client = FlightServiceClient::connect(url).await?;

    let ArrowFile {
        schema,
        batches,
        fields,
        ..
    } = read_json_file(path)?;
    let ipc_schema = IpcSchema {
        fields,
        is_little_endian: true,
    };

    let schema = Arc::new(schema);

    let mut descriptor = FlightDescriptor::default();
    descriptor.set_type(DescriptorType::Path);
    descriptor.path = vec![path.to_string()];

    upload_data(
        client.clone(),
        &schema,
        &ipc_schema.fields,
        descriptor.clone(),
        batches.clone(),
    )
    .await?;
    verify_data(client, descriptor, &schema, &ipc_schema, &batches).await?;

    Ok(())
}

async fn upload_data(
    mut client: Client,
    schema: &Schema,
    fields: &[IpcField],
    descriptor: FlightDescriptor,
    original_data: Vec<ChunkArc>,
) -> Result {
    let (mut upload_tx, upload_rx) = mpsc::channel(10);

    let options = write::WriteOptions { compression: None };

    let mut schema = flight::serialize_schema(schema, Some(fields));
    schema.flight_descriptor = Some(descriptor.clone());
    upload_tx.send(schema).await?;

    let mut original_data_iter = original_data.iter().enumerate();

    if let Some((counter, first_batch)) = original_data_iter.next() {
        let metadata = counter.to_string().into_bytes();
        // Preload the first batch into the channel before starting the request
        send_batch(&mut upload_tx, &metadata, first_batch, fields, &options).await?;

        let outer = client.do_put(Request::new(upload_rx)).await?;
        let mut inner = outer.into_inner();

        let r = inner
            .next()
            .await
            .expect("No response received")
            .expect("Invalid response received");
        assert_eq!(metadata, r.app_metadata);

        // Stream the rest of the batches
        for (counter, batch) in original_data_iter {
            let metadata = counter.to_string().into_bytes();
            send_batch(&mut upload_tx, &metadata, batch, fields, &options).await?;

            let r = inner
                .next()
                .await
                .expect("No response received")
                .expect("Invalid response received");
            assert_eq!(metadata, r.app_metadata);
        }
        drop(upload_tx);
        assert!(
            inner.next().await.is_none(),
            "Should not receive more results"
        );
    } else {
        drop(upload_tx);
        client.do_put(Request::new(upload_rx)).await?;
    }

    Ok(())
}

async fn send_batch(
    upload_tx: &mut mpsc::Sender<FlightData>,
    metadata: &[u8],
    batch: &ChunkArc,
    fields: &[IpcField],
    options: &write::WriteOptions,
) -> Result {
    let (dictionary_flight_data, mut batch_flight_data) = serialize_batch(batch, fields, options);

    upload_tx
        .send_all(&mut stream::iter(dictionary_flight_data).map(Ok))
        .await?;

    // Only the record batch's FlightData gets app_metadata
    batch_flight_data.app_metadata = metadata.to_vec();
    upload_tx.send(batch_flight_data).await?;
    Ok(())
}

async fn verify_data(
    mut client: Client,
    descriptor: FlightDescriptor,
    expected_schema: &Schema,
    ipc_schema: &IpcSchema,
    expected_data: &[ChunkArc],
) -> Result {
    let resp = client.get_flight_info(Request::new(descriptor)).await?;
    let info = resp.into_inner();

    assert!(
        !info.endpoint.is_empty(),
        "No endpoints returned from Flight server",
    );
    for endpoint in info.endpoint {
        let ticket = endpoint
            .ticket
            .expect("No ticket returned from Flight server");

        assert!(
            !endpoint.location.is_empty(),
            "No locations returned from Flight server",
        );
        for location in endpoint.location {
            consume_flight_location(
                location,
                ticket.clone(),
                expected_data,
                expected_schema,
                ipc_schema,
            )
            .await?;
        }
    }

    Ok(())
}

async fn consume_flight_location(
    location: Location,
    ticket: Ticket,
    expected_data: &[ChunkArc],
    schema: &Schema,
    ipc_schema: &IpcSchema,
) -> Result {
    let mut location = location;
    // The other Flight implementations use the `grpc+tcp` scheme, but the Rust http libs
    // don't recognize this as valid.
    location.uri = location.uri.replace("grpc+tcp://", "grpc://");

    let mut client = FlightServiceClient::connect(location.uri).await?;
    let resp = client.do_get(ticket).await?;
    let mut resp = resp.into_inner();

    // We already have the schema from the FlightInfo, but the server sends it again as the
    // first FlightData. Ignore this one.
    let _schema_again = resp.next().await.unwrap();

    let mut dictionaries = Default::default();

    for (counter, expected_batch) in expected_data.iter().enumerate() {
        let data =
            receive_batch_flight_data(&mut resp, &schema.fields, ipc_schema, &mut dictionaries)
                .await
                .unwrap_or_else(|| {
                    panic!(
                        "Got fewer batches than expected, received so far: {} expected: {}",
                        counter,
                        expected_data.len(),
                    )
                });

        let metadata = counter.to_string().into_bytes();
        assert_eq!(metadata, data.app_metadata);

        let actual_batch = deserialize_batch(&data, &schema.fields, ipc_schema, &dictionaries)
            .expect("Unable to convert flight data to Arrow batch");

        assert_eq!(expected_batch.columns().len(), actual_batch.columns().len());
        assert_eq!(expected_batch.len(), actual_batch.len());
        for (i, (expected, actual)) in expected_batch
            .columns()
            .iter()
            .zip(actual_batch.columns().iter())
            .enumerate()
        {
            let field_name = &schema.fields[i].name;
            assert_eq!(expected, actual, "Data for field {}", field_name);
        }
    }

    assert!(
        resp.next().await.is_none(),
        "Got more batches than the expected: {}",
        expected_data.len(),
    );

    Ok(())
}

async fn receive_batch_flight_data(
    resp: &mut Streaming<FlightData>,
    fields: &[Field],
    ipc_schema: &IpcSchema,
    dictionaries: &mut Dictionaries,
) -> Option<FlightData> {
    let mut data = resp.next().await?.ok()?;
    let mut message =
        ipc::MessageRef::read_as_root(&data.data_header).expect("Error parsing first message");

    while let ipc::MessageHeaderRef::DictionaryBatch(batch) = message
        .header()
        .expect("Header to be valid flatbuffers")
        .expect("Header to be present")
    {
        let mut reader = std::io::Cursor::new(&data.data_body);
        read::read_dictionary(batch, fields, ipc_schema, dictionaries, &mut reader, 0)
            .expect("Error reading dictionary");

        data = resp.next().await?.ok()?;
        message = ipc::MessageRef::read_as_root(&data.data_header).expect("Error parsing message");
    }

    Some(data)
}
