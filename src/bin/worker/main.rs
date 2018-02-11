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

use std::fs::File;
use std::io::prelude::*;
use uuid::Uuid;

extern crate clap;
extern crate datafusion;
extern crate etcd;
extern crate futures;
extern crate hyper;
extern crate serde;
extern crate serde_json;
extern crate tokio_core;
extern crate uuid;

use clap::{Arg, App};
use etcd::Client;
use etcd::kv;
use datafusion::exec::*;
use futures::future::Future;
use futures::Stream;
use hyper::{Method, StatusCode, Chunk};
use hyper::header::{ContentLength};
use hyper::server::{Http, Request, Response, Service};
use tokio_core::reactor::Core;

fn main() {

    let matches = App::new("DataFusion Worker Node")
        .version("0.1.4") //TODO get dynamically based on crate version
        .arg(Arg::with_name("ETCD")
            .help("etcd endpoints")
            .short("e")
            .long("etcd")
            .value_name("URL")

            .takes_value(true))
        .arg(Arg::with_name("BIND")
            .short("b")
            .long("bind")
            .help("IP address and port to bind to")
            .default_value("0.0.0.0:8080")
            .takes_value(true))
        .arg(Arg::with_name("WEBROOT")
            .short("w")
            .long("webroot")
            .help("Location of HTML files")
            .default_value("./src/bin/worker/")
            .takes_value(true))
        .get_matches();

    let uuid = Uuid::new_v5(&uuid::NAMESPACE_DNS, "datafusion");
    let bind_addr_str = matches.value_of("BIND").unwrap();

    let bind_addr = bind_addr_str.parse().unwrap();
    let www_root = matches.value_of("WEBROOT").unwrap().to_string();

    let etcd_endpoints = matches.value_of("ETCD").unwrap();
    register(&[etcd_endpoints], &uuid, bind_addr_str);

    println!("Worker {} listening on {} and serving content from {}", uuid, bind_addr, www_root);

    let server = Http::new()
        .bind(&bind_addr, move|| Ok(Worker { www_root: www_root.clone() })).unwrap();
    server.run().unwrap();
}

/// Worker struct to store state
struct Worker {
    www_root: String
}

impl Worker {

    fn load_static_file(&self, filename: &str) -> String {

        let mut f = File::open(&filename).expect("file not found");

        let mut contents = String::new();
        f.read_to_string(&mut contents)
            .expect("something went wrong reading the file");

        contents
    }

}

fn handle_request(chunk: Chunk) -> Response {

    println!("handle_request()");

    // collect the entire body
    let body_bytes = chunk.iter()
        .cloned()
        .collect::<Vec<u8>>();


    match String::from_utf8(body_bytes.clone()) {
        Ok(json_str) => {
            println!("Received request");
            //println!("Request: {}", json_str);

            // this is a crude POC that demonstrates the worker receiving a plan, executing it,
            // and returning a result set

            //TODO: should stream results to client, not build a result set in memory
            //TODO: decide on a more appropriate format to return than csv
            //TODO: should not create a new execution context each time

            match serde_json::from_str(&json_str) {
                Ok(plan) => {
                    //println!("Plan: {:?}", plan);

                    // create execution context
                    let mut ctx = ExecutionContext::new();

                    match plan {
                        ExecutionPlan::Interactive { plan } => {
                            match ctx.create_execution_plan(&plan) {
                                Ok(exec) => {
                                    let it = exec.scan(&ctx);
                                    let mut result_set = "".to_string();

                                    it.for_each(|t| {
                                        match t {
                                            Ok(tuple) => {
                                                result_set += &tuple.to_string()
                                            },
                                            Err(e) => {
                                                result_set += &format!("ERROR: {:?}", e)
                                            }
                                        }
                                        result_set += "\n";
                                    });

                                    Response::new()
                                        .with_status(StatusCode::Ok)
                                        .with_header(ContentLength(result_set.len() as u64))
                                        .with_body(result_set)
                                },
                                Err(e) => {
                                    let msg = format!("Failed to create execution plan: {:?}", e);
                                    Response::new()
                                        .with_status(StatusCode::BadRequest)
                                        .with_header(ContentLength(msg.len() as u64))
                                        .with_body(msg)
                                }
                            }

                        },
                        _ => {
                            let msg = format!("Unsupported execution plan");
                            Response::new()
                                .with_status(StatusCode::BadRequest)
                                .with_header(ContentLength(msg.len() as u64))
                                .with_body(msg)
                        }
                    }

                },
                Err(e) => {
                    let msg = format!("Failed to parse execution plan: {:?}", e);
                    Response::new()
                        .with_status(StatusCode::BadRequest)
                        .with_header(ContentLength(msg.len() as u64))
                        .with_body(msg)

                }
            }

        },
        _ => {
            let msg = format!("Failed to parse request JSON");
            Response::new()
                .with_status(StatusCode::BadRequest)
                .with_header(ContentLength(msg.len() as u64))
                .with_body(msg)
        }
    }
}


impl Service for Worker {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item=Self::Response, Error=Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {

        match req.method() {
            &Method::Get => { // all UI calls are GET

                //println!("path={:?}", req.path());

                // is this a known file and/or valid path?
                let filename = match req.path() {
                    "/" => Some("/index.html"),
                    "/css/main.css" => Some("/css/main.css"),
                    _ => None
                };

                // server page, or a 404 not found error
                match filename {
                    Some(f) => {
                        let fqpath = format!("{}/{}", self.www_root, f);
                        let content = self.load_static_file(&fqpath);
                        Box::new(futures::future::ok(
                            Response::new()
                                .with_header(ContentLength(content.len() as u64))
                                .with_body(content)))
                    }
                    _ => {
                        let fqpath = format!("{}/{}", self.www_root, "/404.html");
                        let content = self.load_static_file(&fqpath);
                        Box::new(futures::future::ok(
                            Response::new()
                                .with_status(StatusCode::NotFound)
                                .with_header(ContentLength(content.len() as u64))
                                .with_body(content)))
                    }
                }
            }
            &Method::Post => { // all REST calls are POST
                Box::new(
                    req.body()
                        .concat2()
                        .map(handle_request)
                )
            }
            _ => {
                Box::new(futures::future::ok(
                    Response::new().with_status(StatusCode::NotFound)
                ))
            }

        }
    }

}

fn register(etcd_endpoints: &[&str], uuid: &Uuid, bind_address: &str) {

    println!("Registering with etcd at {:?}", etcd_endpoints);

    // Create a `Core`, which is the event loop which will drive futures to completion.
    let mut core = Core::new().unwrap();
    // Get a "handle" to the event loop that the client can use to schedule work.
    let handle = core.handle();

    // Create a client to access a single cluster member. Addresses of multiple cluster
    // members can be provided and the client will try each one in sequence until it
    // receives a successful response.
    let client = Client::new(&handle, etcd_endpoints, None).unwrap();

    let key = format!("/datafusion/workers/{}", uuid);

    // Set the key "/foo" to the value "bar" with no expiration.
    let work = kv::set(&client, &key, &bind_address, None)
        .and_then(|_| {
            println!("Registered with etcd");
            Ok(())
        });

    // Start the event loop, driving the asynchronous code to completion.
    core.run(work).unwrap();
}

