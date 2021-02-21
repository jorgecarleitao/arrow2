"""
Adds licenses haders to all Rust source files (.rs). This script
is idenpotent and errors if any file has more than one license.
"""
import os

license = """// Licensed to the Apache Software Foundation (ASF) under one
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
// under the License."""

for root, subdirs, files in os.walk('src/'):
    for filename in files:
        if filename.endswith(".rs"):
            file_path = os.path.join(root, filename)
            to_add = False
            with open(file_path, "r") as f:
                contents = f.read()
                if contents[:len(license)] != license:
                    to_add = True
                if "Licensed to the Apache Software Foundation (ASF)" in contents[len(license):]:
                    print(f"File '{file_path}' contains more than one header")
                    exit(1)
            if to_add:
                with open(file_path, "w") as f:
                    f.write(license + '\n\n' + contents)
