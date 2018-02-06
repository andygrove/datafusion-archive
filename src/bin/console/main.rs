extern crate futures;
extern crate hyper;
extern crate tokio_core;
extern crate serde;
extern crate serde_json;

use std::io::Read;
use std::str;

use std::io::{self, Write};
use futures::{Future, Stream};
use hyper::Client;
use tokio_core::reactor::Core;
use hyper::{Method, Request};
use hyper::header::{ContentLength, ContentType};

extern crate rprompt;
extern crate datafusion;

use datafusion::exec::*;

/// Interactive SQL console
struct Console {
    ctx: ExecutionContext
}

impl Console {

    fn execute(&mut self, command: &str) {
        println!("Executing: {}", command);
        match self.ctx.sql(command) {
            Ok(_ /* ref df */ ) => {
                //TODO: show data from df
                println!("Executed OK");
            },
            Err(e) => println!("Error: {:?}", e)
        }
    }
}

fn main() {

    println!("DataFusion Console");

    let mut console = Console { ctx: ExecutionContext::new() };

    loop {
        match rprompt::prompt_reply_stdout("$ ") {
            Ok(command) => match command.to_lowercase().as_ref() {
                "exit" | "quit" => break,
                _ => {
                    let mut core = Core::new().unwrap();
                    let client = Client::new(&core.handle());

                    let uri = "http://localhost:8080".parse().unwrap();

//                    let ctx = ExecutionContext::new();
//                    let plan = ctx.create_logical_plan(&command).unwrap();

                    // serialize plan to JSON
//                    let json = serde_json::to_string(&plan).unwrap();

                    let json = command.clone();

                    let mut req = Request::new(Method::Post, uri);
                    req.headers_mut().set(ContentType::json());
                    req.headers_mut().set(ContentLength(json.len() as u64));
                    req.set_body(json);

                    let post = client.request(req).and_then(|res| {
                        println!("POST: {}", res.status());
                        res.body().concat2()
                    });

                    let result = core.run(post).unwrap();

                    println!("response: {:?}", str::from_utf8(&result));

                    //console.execute(&command)
                }
            },
            Err(e) => println!("Error parsing command: {:?}", e)
        }


    }
}
