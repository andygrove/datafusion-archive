extern crate clap;
extern crate datafusion;
extern crate rprompt;
extern crate serde;
extern crate serde_json;

use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::str;
use std::time::Instant;

use clap::{Arg, App};
use datafusion::exec::*;
use datafusion::parser::*;
use datafusion::sql::ASTNode::SQLCreateTable;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {

    println!("DataFusion Console");

    let cmdline = App::new("DataFusion Console")
        .version(VERSION)
        .arg(Arg::with_name("ETCD")
            .help("etcd endpoints")
            .short("e")
            .long("etcd")
            .value_name("URL")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("SCRIPT")
            .help("SQL script to run")
            .short("s")
            .long("script")
            .required(false)
            .takes_value(true))
        .get_matches();

    // parse args
    let etcd_endpoints = cmdline.value_of("ETCD").unwrap();
    let mut console = Console::new(etcd_endpoints.to_string());

    match cmdline.value_of("SCRIPT") {
        Some(filename) => {
            match File::open(filename) {
                Ok(f) => {
                    let mut reader = BufReader::new(&f);
                    for line in reader.lines() {
                        match line {
                            Ok(cmd) => console.execute(&cmd),
                            Err(e) => println!("Error: {}", e)
                        }
                    }
                },
                Err(e) => println!("Could not open file {}: {}", filename, e)
            }
        },
        None => {
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
    }

}


/// Interactive SQL console
struct Console {
    ctx: ExecutionContext
}

impl Console {

    fn new(etcd: String) -> Self {
        Console { ctx: ExecutionContext::remote(etcd) }
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
                _ => {
                    match self.ctx.create_logical_plan(sql) {
                        Ok(logical_plan) => {

                            let physical_plan = PhysicalPlan::Interactive {
                                plan: logical_plan
                            };

                            let result = self.ctx.execute(&physical_plan);

                            match result {
                                Ok(result) => {

                                    let elapsed = timer.elapsed();
                                    let elapsed_seconds = elapsed.as_secs() as f64
                                        + elapsed.subsec_nanos() as f64 / 1000000000.0;

                                    match result {
                                        ExecutionResult::Unit => {
                                            println!("Query executed in {} seconds", elapsed_seconds);
                                        }
                                        ExecutionResult::Count(n) => {
                                            println!("Query executed in {} seconds and updated {} rows", elapsed_seconds, n);
                                        }
                                    }
                                },
                                Err(e) => println!("Error: {:?}", e)
                            }
                        }
                        Err(e) => println!("Error: {:?}", e)
                    }
                }
            }
            Err(e) => println!("Error: {:?}", e)
        }
    }

}

