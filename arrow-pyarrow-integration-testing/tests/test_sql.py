# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest

import pyarrow
import arrow_pyarrow_integration_testing


class TestCase(unittest.TestCase):
    def setUp(self):
        self.old_allocated_rust = arrow_pyarrow_integration_testing.total_allocated_bytes()
        self.old_allocated_cpp = pyarrow.total_allocated_bytes()

    def tearDown(self):
        # No leak of Rust
        self.assertEqual(self.old_allocated_rust, arrow_pyarrow_integration_testing.total_allocated_bytes())

        # No leak of C++ memory
        self.assertEqual(self.old_allocated_cpp, pyarrow.total_allocated_bytes())

    def test_primitive_python(self):
        """
        Python -> Rust -> Python
        """
        a = pyarrow.array([1, 2, 3])
        b = arrow_pyarrow_integration_testing.double(a)
        self.assertEqual(b, pyarrow.array([2, 4, 6]))

    def test_primitive_rust(self):
        """
        Rust -> Python -> Rust
        """
        def double(array):
            array = array.to_pylist()
            return pyarrow.array([x * 2 if x is not None else None for x in array])

        is_correct = arrow_pyarrow_integration_testing.double_py(double)
        self.assertTrue(is_correct)

    def test_import_primitive(self):
        """
        Python -> Rust
        """
        old_allocated = pyarrow.total_allocated_bytes()

        a = pyarrow.array([2, None, 6])

        is_correct = arrow_pyarrow_integration_testing.import_primitive(a)
        self.assertTrue(is_correct)
        # No leak of C++ memory
        del a
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())

    def test_export_primitive(self):
        """
        Python -> Rust
        """
        old_allocated = pyarrow.total_allocated_bytes()

        expected = pyarrow.array([2, None, 6])

        result = arrow_pyarrow_integration_testing.export_primitive()
        self.assertEqual(expected, result)
        # No leak of C++ memory
        del expected
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())

    def test_string_roundtrip(self):
        """
        Python -> Rust -> Python
        """
        a = pyarrow.array(["a", None, "ccc"])
        b = arrow_pyarrow_integration_testing.round_trip(a)
        c = pyarrow.array(["a", None, "ccc"])
        self.assertEqual(b, c)

    def test_string_python(self):
        """
        Python -> Rust -> Python
        """
        a = pyarrow.array(["a", None, "ccc"])
        b = arrow_pyarrow_integration_testing.substring(a, 1)
        self.assertEqual(b, pyarrow.array(["", None, "cc"]))

    def test_time32_python(self):
        """
        Python -> Rust -> Python
        """
        a = pyarrow.array([None, 1, 2], pyarrow.time32('s'))
        b = arrow_pyarrow_integration_testing.concatenate(a)
        expected = pyarrow.array([None, 1, 2] + [None, 1, 2], pyarrow.time32('s'))
        self.assertEqual(b, expected)

    def test_list_array(self):
        """
        Python -> Rust -> Python
        """
        for _ in range(2):
            a = pyarrow.array([[], None, [1, 2], [4, 5, 6]], pyarrow.list_(pyarrow.int64()))
            b = arrow_pyarrow_integration_testing.round_trip(a)

            b.validate(full=True)
            assert a.to_pylist() == b.to_pylist()
            assert a.type == b.type

    def test_struct_array(self):
        """
        Python -> Rust -> Python
        """
        fields = [
            ('f1', pyarrow.int32()),
            ('f2', pyarrow.string()),
        ]
        a = pyarrow.array([
            {"f1": 1, "f2": "a"},
            None,
            {"f1": 3, "f2": None},
            {"f1": None, "f2": "d"},
            {"f1": None, "f2": None},
        ], pyarrow.struct(fields))
        b = arrow_pyarrow_integration_testing.round_trip(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type
