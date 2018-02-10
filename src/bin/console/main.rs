extern crate futures;
extern crate hyper;
extern crate tokio_core;
extern crate serde;
extern crate serde_json;

use std::str;
use std::time::Instant;

use futures::{Future, Stream};
use hyper::Client;
use tokio_core::reactor::Core;
use hyper::{Method, Request};
use hyper::header::{ContentLength, ContentType};

extern crate rprompt;
extern crate datafusion;

use datafusion::exec::*;
use datafusion::parser::*;
use datafusion::sql::ASTNode::*;

fn main() {

    /*

    sample queries to run:

    CREATE EXTERNAL TABLE uk_cities (name VARCHAR(100) NOT NULL, lat DOUBLE NOT NULL, lng DOUBLE NOT NULL)
    SELECT lat, lng, name FROM uk_cities WHERE lat < 51 ORDER BY lat, lng
    SELECT lat, lng, name FROM uk_cities WHERE lat < 51 ORDER BY lat DESC, lng DESC

    */

    println!("DataFusion Console");

    let mut console = Console { ctx: ExecutionContext::new() };

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


/// Interactive SQL console
struct Console {
    ctx: ExecutionContext
}

impl Console {

    /// Execute a SQL statement or console command
    fn execute(&mut self, sql: &str) {

        println!("Executing: {}", sql);

        // parse the SQL
        match Parser::parse_sql(String::from(sql)) {
            Ok(ast) => match ast {
                SQLCreateTable { .. } => {
                    self.ctx.sql(&sql).unwrap();
                    ()
                },
                _ => self.execute_in_worker(&sql)
            }
            Err(e) => println!("Error: {:?}", e)
        }
    }

    fn execute_in_worker(&mut self, sql: &str) {

        let timer = Instant::now();

        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let uri = "http://localhost:8080".parse().unwrap();

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
                                let elapsed = timer.elapsed();
                                let elapsed_seconds = elapsed.as_secs() as f64
                                    + elapsed.subsec_nanos() as f64 / 1000000000.0;
                                println!("Query executed in {} seconds",
                                         elapsed_seconds);
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

