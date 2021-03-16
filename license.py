"""
Adds licenses haders to all Rust source files (.rs). This script
is idempotent and errors if any file has more than one license.
"""
import os

# Because the code was copied to a large extent from the arrow project,
# it makes sense to keep the original license when applicable.
asf_license = """// Licensed to the Apache Software Foundation (ASF) under one
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

with open('LICENSE', "r") as f:
    own_license = f.read()

for root, subdirs, files in os.walk('src/'):
    for filename in files:
        if filename.endswith(".rs"):
            file_path = os.path.join(root, filename)
            add_license = False
            with open(file_path, "r") as f:
                contents = f.read()
                if contents[:len(own_license)] == own_license:
                    add_license = True
                elif contents[:len(asf_license)] != asf_license:
                    pass
                elif asf_license[:10] in contents[len(asf_license):] or own_license[:10] in contents[len(own_license):]:
                    print(f"File '{file_path}' contains more than one header")
                    exit(1)
            if add_license:
                print(f"Adding license to {file_path}")
                with open(file_path, "w") as f:
                    f.write(own_license + '\n\n' + contents)
