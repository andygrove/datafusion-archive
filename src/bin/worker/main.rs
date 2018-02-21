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
use std::io::{Error, ErrorKind};
use std::time::Duration;
use std::str;
use std::thread;

extern crate clap;
extern crate datafusion;
extern crate etcd;
extern crate futures;
extern crate futures_timer;
extern crate hyper;
extern crate serde;
extern crate serde_json;
extern crate tokio_core;
extern crate uuid;

use clap::{Arg, App};
use etcd::Client;
use etcd::kv;
use datafusion::exec::*;
use futures::future::{ok, loop_fn, Future, Loop};
use futures::Stream;
use hyper::{Method, StatusCode};
use hyper::client::HttpConnector;
use hyper::header::{ContentLength};
use hyper::server::{Http, Request, Response, Service};
use tokio_core::reactor::Core;
use uuid::Uuid;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {

    let matches = App::new("DataFusion Worker Node")
        .version(VERSION)
        .arg(Arg::with_name("ETCD")
            .help("etcd endpoints")
            .long("etcd")
            .value_name("URL")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("BIND")
            .long("bind")
            .help("IP address and port to bind to")
            .default_value("0.0.0.0:8080")
            .takes_value(true))
        .arg(Arg::with_name("DATADIR")
            .long("data_dir")
            .help("Location of data files")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("WEBROOT")
            .long("webroot")
            .help("Location of HTML files")
            .default_value("./src/bin/worker/")
            .takes_value(true))
        .get_matches();

    let uuid = Uuid::new_v5(&uuid::NAMESPACE_DNS, "datafusion");

    let bind_addr_str = matches.value_of("BIND").unwrap().to_string();
    let bind_addr = bind_addr_str.parse().unwrap();

    let www_root = matches.value_of("WEBROOT").unwrap().to_string();
    let data_dir = matches.value_of("DATADIR").unwrap().to_string();

    let etcd_endpoints = matches.value_of("ETCD").unwrap();

    // create futures event loop
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    println!("Worker {} listening on {} and serving content from {}", uuid, bind_addr, www_root);


    thread::spawn(move || {
        let server = Http::new()
            .bind(&bind_addr, move|| Ok(Worker {
                www_root: www_root.clone(), data_dir: data_dir.clone()
            })).unwrap();
        server.run().unwrap();
    });

    // start a loop to register with etcd every 5 seconds with a ttl of 10 seconds
    match Client::new(&handle, &[etcd_endpoints], None) {
        Ok(etcd) => {
            let heartbeat_loop = loop_fn(Membership::new(etcd, uuid, bind_addr_str), |client| {
                client.register()
                    .and_then(|(client, done)| {
                        if done {
                            Ok(Loop::Break(client))
                        } else {
                            Ok(Loop::Continue(client))
                        }
                    })
            });
            match core.run(heartbeat_loop) {
                Ok(_) => println!("Heartbeat loop finished"),
                Err(e) => println!("Heartbeat loop failed: {:?}", e),
            }

        }
        Err(e) => println!("Failed to connect to etcd: {:?}", e)
    }

}

/// Worker struct to store state
struct Worker {
    www_root: String,
    data_dir: String
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

                let data_dir = self.data_dir.clone();

                Box::new(req.body().concat2()
                    .and_then(move |body| {
                        let json = str::from_utf8(&body).unwrap();
                        println!("{}", json);

                        println!("Received request");
                        //println!("Request: {}", json_str);

                        // this is a crude POC that demonstrates the worker receiving a plan, executing it,
                        // and returning a result set

                        //TODO: should stream results to client, not build a result set in memory
                        //TODO: decide on a more appropriate format to return than csv
                        //TODO: should not create a new execution context each time

                        ok(match serde_json::from_str(&json) {
                            Ok(plan) => {
                                //println!("Plan: {:?}", plan);

                                // create execution context
                                let mut ctx = ExecutionContext::local(data_dir.clone());

                                match plan {
                                    PhysicalPlan::Interactive { plan } => {
                                        match ctx.create_execution_plan(data_dir.clone(), &plan) {
                                            Ok(exec) => {
                                                let it = exec.scan(&ctx);
                                                let mut result_set = "".to_string();

                                                it.for_each(|t| {
                                                    match t {
                                                        Ok(row) => {
                                                            result_set += &row.to_string()
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
                                            Err(e) => error_response(format!("Failed to create execution plan: {:?}", e))
                                        }

                                    },
                                    PhysicalPlan::Write { plan, filename } => {
                                        println!("Writing dataframe to {}", filename);
                                        let df = DF { plan: plan };
                                        match ctx.write(Box::new(df), &filename) {
                                            Ok(count) => {
                                                println!("Wrote {} rows to {}", count, filename);
                                                Response::new().with_status(StatusCode::Ok)
                                            },
                                            Err(e) => error_response(format!("Failed to create execution plan: {:?}", e))
                                        }
                                    }
                                    //_ => error_response(format!("Unsupported execution plan"))
                                }

                            },
                            Err(e) => error_response(format!("Failed to parse execution plan: {:?}", e))
                        })
                    }))

            }
            _ => {
                Box::new(futures::future::ok(
                    Response::new().with_status(StatusCode::NotFound)
                ))
            }

        }
    }

}

fn error_response(msg: String) -> Response {
    Response::new()
        .with_status(StatusCode::BadRequest)
        .with_header(ContentLength(msg.len() as u64))
        .with_body(msg)
}

struct Membership {
    etcd: Client<HttpConnector>,
    uuid: Uuid,
    bind_address: String
}

impl Membership {

    fn new(etcd: Client<HttpConnector>, uuid: Uuid, bind_address: String) -> Self {
        Membership { etcd, uuid, bind_address }
    }

    fn register(self) -> Box<Future<Item=(Self,bool),Error=Error>> {

        let key = format!("/datafusion/workers/{}", self.uuid);

        Box::new(kv::set(&self.etcd, &key, &self.bind_address, Some(10))
            .and_then(|_etcd_response| {
                println!("Registered with etcd: {} -> {}", self.uuid, self.bind_address);
                thread::sleep(Duration::from_millis(5000));
                ok((self,false))
            })
            .map_err(|_| Error::from(ErrorKind::NotFound)))
    }

}

