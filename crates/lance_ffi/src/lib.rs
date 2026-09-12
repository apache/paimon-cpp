// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![deny(unsafe_op_in_unsafe_fn)]

mod error;
mod reader;
mod runtime;
mod util;
mod writer;

pub use error::paimon_lance_last_error;
pub use reader::{
    paimon_lance_batch_reader_free, paimon_lance_batch_reader_next,
    paimon_lance_reader_export_schema, paimon_lance_reader_free, paimon_lance_reader_num_rows,
    paimon_lance_reader_open, paimon_lance_reader_open_stream, PaimonLanceBatchReader,
    PaimonLanceReader,
};
pub use writer::{
    paimon_lance_writer_finish, paimon_lance_writer_free, paimon_lance_writer_open,
    paimon_lance_writer_tell, paimon_lance_writer_write, PaimonLanceWriter,
};
