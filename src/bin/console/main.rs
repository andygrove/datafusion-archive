extern crate clap;
extern crate datafusion;
extern crate etcd;
extern crate futures;
extern crate hyper;
extern crate rprompt;
extern crate serde;
extern crate serde_json;
extern crate tokio_core;

use std::str;
use std::time::Instant;

use clap::{Arg, App};
use etcd::Client as EtcdClient;
use etcd::{kv, Response};
use etcd::kv::KeyValueInfo;
use datafusion::exec::*;
use datafusion::parser::*;
use datafusion::sql::ASTNode::*;
use futures::{Future, Stream};
use hyper::Client;
use tokio_core::reactor::Core;
use hyper::{Method, Request};
use hyper::header::{ContentLength, ContentType};

fn main() {

    let matches = App::new("DataFusion Console")
        .version("0.1.4") //TODO get dynamically based on crate version
        .arg(Arg::with_name("ETCD")
            .help("etcd endpoints")
            .short("e")
            .long("etcd")
            .value_name("URL")
            .required(true)
            .takes_value(true))
        .get_matches();

    println!("DataFusion Console");

    let etcd_endpoints = matches.value_of("ETCD").unwrap();

    // create futures event loop
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    // get list of workers from etcd
    let workers : Option<Vec<String>> = if let Ok(etcd) = EtcdClient::new(&handle, &[etcd_endpoints], None) {
        match core.run(kv::get(&etcd, "/datafusion/workers/", kv::GetOptions::default())) {
            Ok(Response { ref data, .. }) =>  match data {
                &KeyValueInfo { ref node, .. } => match &node.nodes {
                    &Some(ref workers) => {
                        Some(workers.iter()
                            .flat_map(|w| w.value.clone())
                            .collect())
                    },
                    _ => None
                }
            }
            _ => {
                println!("Failed to find workers in etcd");
                None
            }
        }
    } else {
        println!("Failed to connect to etcd");
        None
    };

    match workers {
        Some(ref list) if list.len() > 0 => {

            let mut console = Console::new(format!("http://{}", list[0]));

            loop {
                match rprompt::prompt_reply_stdout("$ ") {
                    Ok(command) => match command.as_ref() {
                        "exit" | "quit" => break,
                        _ => console.execute(&command)
                    },
                    Err(e) => println!("Error parsing command: {:?}", e)
                }
            }

        }
        _ => {
            println!("Could not locate a worker node in etcd")

        }
    }


}


/// Interactive SQL console
struct Console {
    ctx: ExecutionContext,
    worker_addr: String
}

impl Console {

    fn new(worker_addr: String) -> Self {
        println!("Connecting to worker node at {}", worker_addr);
        // pass an empty data path since the client doesn't access data directly
        Console { ctx: ExecutionContext::new("".to_string()), worker_addr: worker_addr }
    }

    /// Execute a SQL statement or console command
    fn execute(&mut self, sql: &str) {

        println!("Executing query ...");

        let timer = Instant::now();

        // parse the SQL
        match Parser::parse_sql(String::from(sql)) {
            Ok(ast) => match ast {
                SQLCreateTable { .. } => {
                    self.ctx.sql(&sql).unwrap();
                    println!("Registered schema with execution context");
                    ()
                },
                _ => self.execute_in_worker(&sql)
            }
            Err(e) => println!("Error: {:?}", e)
        }

        let elapsed = timer.elapsed();
        let elapsed_seconds = elapsed.as_secs() as f64
            + elapsed.subsec_nanos() as f64 / 1000000000.0;
        println!("Query executed in {} seconds", elapsed_seconds);
    }

    fn execute_in_worker(&mut self, sql: &str) {

        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let uri = self.worker_addr.parse().unwrap();

        match self.ctx.create_logical_plan(&sql) {
            Ok(logical_plan) => {
                let execution_plan = ExecutionPlan::Interactive { plan: logical_plan };

                // serialize plan to JSON
                match serde_json::to_string(&execution_plan) {
                    Ok(json) => {
                        let mut req = Request::new(Method::Post, uri);
                        req.headers_mut().set(ContentType::json());
                        req.headers_mut().set(ContentLength(json.len() as u64));
                        req.set_body(json);

                        let post = client.request(req).and_then(|res| {
                            //println!("POST: {}", res.status());
                            res.body().concat2()
                        });

                        match core.run(post) {
                            Ok(result) => {
                                //TODO: show response as formatted table
                                let result = str::from_utf8(&result).unwrap();
                                println!("{}", result);
                            }
                            Err(e) => println!("Failed to serialize plan: {:?}", e)
                        }
                    }
                    Err(e) => println!("Failed to serialize plan: {:?}", e)
                }
            }
            Err(e) => println!("Query failed: {:?}", e)
        }
    }
}

