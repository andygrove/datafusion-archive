// Copyright 2018 Grove Enterprises LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate etcd;
extern crate tokio_core;

use self::etcd::Client as EtcdClient;
use self::etcd::{kv, Response};
use self::etcd::kv::KeyValueInfo;
use self::tokio_core::reactor::Core;

/// Retrieve a list of DataFusion worker endpoints from etcd
pub fn get_worker_list(etcd_address: &str) -> Result<Vec<String>, String> {

    // create futures event loop
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    // get list of workers from etcd
    let endpoints = &[etcd_address];

    match EtcdClient::new(&handle, endpoints, None) {
        Ok(etcd) => match core.run(kv::get(&etcd, "/datafusion/workers/", kv::GetOptions::default())) {
            Ok(Response { ref data, .. }) =>  match data {
                &KeyValueInfo { ref node, .. } => match &node.nodes {
                    &Some(ref workers) => {
                        Ok(workers.iter()
                            .flat_map(|w| w.value.clone())
                            .collect())
                    },
                    _ => Ok(vec![])
                }
            }
            Err(e) => {
                Err(format!("Etcd request failed ({:?}): {:?}", endpoints, e))
            }
        },
        Err(e) => Err(format!("Etcd connection failed ({:?}): {:?}", endpoints, e))
    }

}