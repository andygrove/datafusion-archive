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

extern crate hyper;
extern crate futures;
extern crate serde;
extern crate serde_json;

use futures::future::Future;
use futures::Stream;

use hyper::{Method, StatusCode, Chunk};
use hyper::server::{Http, Request, Response, Service};

extern crate datafusion;
use datafusion::rel::*;

struct Worker;

fn handle_request(chunk: Chunk) -> Response {
    let body_bytes = chunk.iter()
        .cloned()
        .collect::<Vec<u8>>();

    match String::from_utf8(body_bytes.clone()) {
        Ok(json_str) => {
            // for initial testing, just receive a relational plan directly but this should
            // really be an execution plan
            let rel: Rel = serde_json::from_str(&json_str).unwrap();

            println!("Plan: {:?}", rel);

            Response::new()
                .with_body(body_bytes)
        },
        _ => panic!()
    }
}

impl Service for Worker {
    // boilerplate hooking up hyper's server types
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    // The future representing the eventual Response your call will
    // resolve to. This can change to whatever Future you need.
    type Future = Box<Future<Item=Self::Response, Error=Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {

        match req.method() {
            &Method::Post => {
                Box::new(
                    req.body()
                        .concat2()
                        .map(handle_request)
                )
            },
            _ => {
                Box::new(futures::future::ok(
                    Response::new().with_status(StatusCode::NotFound)
                ))
            }

        }

//        // We're currently ignoring the Request
//        // And returning an 'ok' Future, which means it's ready
//        // immediately, and build a Response with the 'PHRASE' body.
//        Box::new(futures::future::ok(
//            Response::new()
//                .with_header(ContentLength(PHRASE.len() as u64))
//                .with_body(PHRASE)
//        ))
    }


}

fn main() {
    let addr = "127.0.0.1:8080".parse().unwrap();

    println!("Worker listening on {}", addr);

    let server = Http::new().bind(&addr, || Ok(Worker)).unwrap();
    server.run().unwrap();
}