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

extern crate clap;
extern crate datafusion;

#[cfg(target_family = "unix")]
extern crate liner;

use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::rc::Rc;
use std::str;
use std::time::Instant;

use clap::{App, Arg};
use datafusion::exec::*;
use datafusion::functions::geospatial::st_astext::*;
use datafusion::functions::geospatial::st_point::*;
use datafusion::functions::math::*;
use datafusion::sqlast::ASTNode::SQLCreateTable;
use datafusion::sqlparser::*;

mod linereader;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[cfg(target_family = "unix")]
fn setup_console(cmdline: clap::ArgMatches) {
    //parse args
    //let etcd_endpoints = cmdline.value_of("ETCD").unwrap();
    let mut console = Console::new(/*etcd_endpoints.to_string()*/);

    match cmdline.value_of("SCRIPT") {
        Some(filename) => match File::open(filename) {
            Ok(f) => {
                let mut cmd_buffer = String::new();
                let mut reader = BufReader::new(&f);
                for line in reader.lines() {
                    match line {
                        Ok(cmd) => {
                            cmd_buffer.push_str(&cmd);
                            if cmd_buffer.as_str().ends_with(";") {
                                console.execute(&cmd_buffer[0..cmd_buffer.len() - 2]);
                                cmd_buffer = String::new();
                            }
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                }
                if cmd_buffer.as_str().ends_with(";") {
                    console.execute(&cmd_buffer[0..cmd_buffer.len() - 2]);
                }
            }
            Err(e) => println!("Could not open file {}: {}", filename, e),
        },
        _ => {
            let mut reader = linereader::LineReader::new();
            loop {
                let result = reader.read_lines();
                match result {
                    Some(line) => match line {
                        linereader::LineResult::Break => break,
                        linereader::LineResult::Input(command) => console.execute(&command),
                    },
                    None => (),
                }
            }
        }
    }
}

#[cfg(target_family = "windows")]
fn setup_console(cmdline: clap::ArgMatches) {
    panic!("Console is not supported on windows!")
}

fn main() {
    println!("DataFusion Console");
    //    println!("");
    //    println!("Enter SQL statements terminated with semicolon, or 'quit' to leave.");
    //    println!("");

    let cmdline = App::new("DataFusion Console")
            .version(VERSION)
//            .arg(
//                Arg::with_name("ETCD")
//                    .help("etcd endpoints")
//                    .short("e")
//                    .long("etcd")
//                    .value_name("URL")
//                    .required(true)
//                    .takes_value(true),
//            )
            .arg(
                Arg::with_name("SCRIPT")
                    .help("SQL script to run")
                    .short("s")
                    .long("script")
                    .required(false)
                    .takes_value(true),
            )
            .get_matches();
    setup_console(cmdline);
}

/// Interactive SQL console
struct Console {
    ctx: ExecutionContext,
}

impl Console {
    /// Create a new instance of the console
    fn new() -> Self {
        let mut ctx = ExecutionContext::local();
        ctx.register_scalar_function(Rc::new(STPointFunc {}));
        ctx.register_scalar_function(Rc::new(STAsText {}));
        ctx.register_scalar_function(Rc::new(SqrtFunction {}));
        Console { ctx }
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
                    //println!("Registered schema with execution context");
                    ()
                }
                _ => match self.ctx.create_logical_plan(sql) {
                    Ok(logical_plan) => {
                        let physical_plan = PhysicalPlan::Interactive {
                            plan: logical_plan.clone(),
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
                                        println!(
                                            "Query executed in {} seconds and updated {} rows",
                                            elapsed_seconds, n
                                        );
                                    }
                                    ExecutionResult::Str(_) => {
                                        println!("Query executed in {} seconds", elapsed_seconds);
                                    }
                                }
                            }
                            Err(e) => println!("Error: {:?}", e),
                        }
                    }
                    Err(e) => println!("Error: {:?}", e),
                },
            },
            Err(e) => println!("Error: {:?}", e),
        }
    }
}
