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

use std::env;
use std::fs::File;

use arrow2::error::Result;
use arrow2::io::ipc::read;
use arrow2::io::ipc::write;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let filename = &args[1];
    let mut f = File::open(filename)?;
    let metadata = read::read_file_metadata(&mut f)?;
    let mut reader = read::FileReader::new(f, metadata.clone(), None);

    let options = write::WriteOptions { compression: None };
    let mut writer = write::StreamWriter::new(std::io::stdout(), options);

    writer.start(&metadata.schema, &metadata.ipc_schema.fields)?;

    reader.try_for_each(|batch| {
        let batch = batch?;
        writer.write(&batch, &metadata.ipc_schema.fields)
    })?;
    writer.finish()?;

    Ok(())
}
