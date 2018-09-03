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

//! # DataFusion
//!
//! DataFusion is a SQL parser, planner, and query execution library for Rust. It is data source
//! agnostic but provides implementations for CSV and Apache Parquet file formats. A DataFrame API
//! is also provided.

extern crate arrow;
extern crate byteorder;
extern crate bytes;
extern crate clap;
extern crate csv;
extern crate datafusion_rustyline;
extern crate fnv;
extern crate json;
extern crate parquet;
extern crate sqlparser;

#[macro_use]
extern crate lazy_static;

pub mod dataframe;
pub mod datasources;
#[macro_use]
pub mod errors;
pub mod exec;
pub mod functions;
pub mod logical;
pub mod relations;
pub mod sqlplanner;
pub mod types;
