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
    array::*,
    datatypes::*,
    io::flight::{self, deserialize_batch, serialize_batch},
    io::ipc::{read, write},
    record_batch::RecordBatch,
};
use arrow_format::flight::data::{
    flight_descriptor::DescriptorType, FlightData, FlightDescriptor, Location, Ticket,
};
use arrow_format::flight::service::flight_service_client::FlightServiceClient;
use arrow_format::ipc;
use arrow_format::ipc::Message::MessageHeader;
use futures::{channel::mpsc, sink::SinkExt, stream, StreamExt};
use tonic::{Request, Streaming};

use std::{collections::HashMap, sync::Arc};

type ArrayRef = Arc<dyn Array>;
type SchemaRef = Arc<Schema>;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

type Client = FlightServiceClient<tonic::transport::Channel>;

pub async fn run_scenario(host: &str, port: &str, path: &str) -> Result {
    let url = format!("http://{}:{}", host, port);

    let client = FlightServiceClient::connect(url).await?;

    let ArrowFile {
        schema, batches, ..
    } = read_json_file(path)?;

    let schema = Arc::new(schema);

    let mut descriptor = FlightDescriptor::default();
    descriptor.set_type(DescriptorType::Path);
    descriptor.path = vec![path.to_string()];

    upload_data(
        client.clone(),
        schema.clone(),
        descriptor.clone(),
        batches.clone(),
    )
    .await?;
    verify_data(client, descriptor, schema, &batches).await?;

    Ok(())
}

async fn upload_data(
    mut client: Client,
    schema: SchemaRef,
    descriptor: FlightDescriptor,
    original_data: Vec<RecordBatch>,
) -> Result {
    let (mut upload_tx, upload_rx) = mpsc::channel(10);

    let options = write::WriteOptions { compression: None };

    let mut schema = flight::serialize_schema(&schema);
    schema.flight_descriptor = Some(descriptor.clone());
    upload_tx.send(schema).await?;

    let mut original_data_iter = original_data.iter().enumerate();

    if let Some((counter, first_batch)) = original_data_iter.next() {
        let metadata = counter.to_string().into_bytes();
        // Preload the first batch into the channel before starting the request
        send_batch(&mut upload_tx, &metadata, first_batch, &options).await?;

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
            send_batch(&mut upload_tx, &metadata, batch, &options).await?;

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
    batch: &RecordBatch,
    options: &write::WriteOptions,
) -> Result {
    let (dictionary_flight_data, mut batch_flight_data) = serialize_batch(batch, options);

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
    expected_schema: SchemaRef,
    expected_data: &[RecordBatch],
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
                expected_schema.clone(),
            )
            .await?;
        }
    }

    Ok(())
}

async fn consume_flight_location(
    location: Location,
    ticket: Ticket,
    expected_data: &[RecordBatch],
    schema: SchemaRef,
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
        let data = receive_batch_flight_data(&mut resp, schema.clone(), &mut dictionaries)
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

        let actual_batch = deserialize_batch(&data, schema.clone(), true, &dictionaries)
            .expect("Unable to convert flight data to Arrow batch");

        assert_eq!(expected_batch.schema(), actual_batch.schema());
        assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
        assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
        let schema = expected_batch.schema();
        for i in 0..expected_batch.num_columns() {
            let field = schema.field(i);
            let field_name = field.name();

            let expected_data = expected_batch.column(i);
            let actual_data = actual_batch.column(i);

            assert_eq!(expected_data, actual_data, "Data for field {}", field_name);
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
    schema: SchemaRef,
    dictionaries: &mut HashMap<usize, Arc<dyn Array>>,
) -> Option<FlightData> {
    let mut data = resp.next().await?.ok()?;
    let mut message =
        ipc::Message::root_as_message(&data.data_header[..]).expect("Error parsing first message");

    while message.header_type() == MessageHeader::DictionaryBatch {
        let mut reader = std::io::Cursor::new(&data.data_body);
        read::read_dictionary(
            message
                .header_as_dictionary_batch()
                .expect("Error parsing dictionary"),
            &schema,
            true,
            dictionaries,
            &mut reader,
            0,
        )
        .expect("Error reading dictionary");

        data = resp.next().await?.ok()?;
        message =
            ipc::Message::root_as_message(&data.data_header[..]).expect("Error parsing message");
    }

    Some(data)
}
