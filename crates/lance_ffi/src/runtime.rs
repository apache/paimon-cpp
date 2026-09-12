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

use std::future::Future;
use std::sync::OnceLock;

use tokio::runtime::{Handle, Runtime};

static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();

pub(crate) fn runtime() -> Result<&'static Runtime, String> {
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("create Tokio runtime: {error}"))
        })
        .as_ref()
        .map_err(Clone::clone)
}

pub(crate) fn block_on<F, T>(future: F) -> Result<T, String>
where
    F: Future<Output = Result<T, String>> + Send,
    T: Send,
{
    let runtime = runtime()?;
    if Handle::try_current().is_err() {
        return runtime.block_on(future);
    }
    std::thread::scope(|scope| {
        scope
            .spawn(move || runtime.block_on(future))
            .join()
            .map_err(|_| "Lance runtime worker panicked".to_string())?
    })
}

#[cfg(test)]
mod tests {
    #[tokio::test(flavor = "current_thread")]
    async fn block_on_from_runtime_thread() {
        assert_eq!(super::block_on(async { Ok(42) }).unwrap(), 42);
    }
}
