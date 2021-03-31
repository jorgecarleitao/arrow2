"""
Adds licenses haders to all Rust source files (.rs). This script
is idempotent and errors if any file has more than one license.
"""
import os

# Because some code was copied from the arrow project,
# we should keep the original license on those portions.
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
own_license = '// ' + own_license.replace('\n', '\n// ')[:-3]


own_compute = [
    "src/compute/arity.rs",
    "src/compute/hash.rs",
    "src/compute/boolean_kleene.rs",
    "src/compute/merge_sort",
]

def should_be_own_license(path: str) -> bool:
    return path.startswith("src/array") or \
        path.startswith("src/buffer") or \
        path.startswith("src/bitmap") or \
        path.startswith("src/error") or \
        path.startswith("src/lib.rs") or \
        path.startswith("src/types") or \
        path.startswith("src/docs") or \
        path.startswith("src/bits") or \
        path == "src/temporal_conversions.rs" or \
        path.startswith("src/io/csv") or \
        any(path.startswith(x) for x in own_compute)


def should_be_asf_license(path: str) -> bool:
    return path.startswith("src/alloc") or \
        (path.startswith("src/compute") and not any(path.startswith(x) for x in own_compute)) or \
        path.startswith("src/io") and not path.startswith("src/io/csv") or \
        path == "src/record_batch.rs" or \
        path.startswith("src/datatypes/") or path.startswith("src/ffi/") or \
        path.startswith("src/util/")


for root, subdirs, files in os.walk('src/'):
    for filename in files:
        if filename.endswith(".rs"):
            file_path = os.path.join(root, filename)
            has_own_license = False
            has_asf_license = False
            with open(file_path, "r") as f:
                contents = f.read()

            if contents[:len(own_license)] == own_license:
                has_own_license = True
            if contents[:len(asf_license)] == asf_license:
                has_asf_license = True

            should_be_own = should_be_own_license(file_path)
            should_be_asf = should_be_asf_license(file_path)
            if not should_be_own and not should_be_asf:
                print(f"needs some: {file_path}")
                exit(1)
            if should_be_own and should_be_asf:
                print(f"needs both: {file_path}")
                exit(1)

            if should_be_own:
                if has_own_license:
                    print(f"Removing own license from {file_path}")
                    contents = contents[len(own_license):]
                elif has_asf_license:
                    print(f"Removing asf license from {file_path}")
                    contents = contents[len(asf_license):]
                if has_asf_license or has_own_license:
                    with open(file_path, "w") as f:
                        f.write(contents)
            elif should_be_asf:
                if has_own_license:
                    contents = contents[len(own_license):]
                if not has_asf_license or has_own_license:
                    print(f"Adding ASF license to {file_path}")
                    with open(file_path, "w") as f:
                        f.write(asf_license + '\n\n' + contents)
