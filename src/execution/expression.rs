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

use std::rc::Rc;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use super::error::Result;
use super::value::Value;

/// Compiled Expression (basically just a closure to evaluate the expression at runtime)
pub type CompiledExpr = Rc<Fn(&RecordBatch) -> Result<Value>>;

pub type CompiledCastFunction = Rc<Fn(&Value) -> Result<Value>>;

pub enum AggregateType {
    Min,
    Max,
    Sum,
    Count,
    Avg,
    //CountDistinct()
}

/// Runtime expression
pub enum RuntimeExpr {
    Compiled {
        f: CompiledExpr,
        t: DataType,
    },
    AggregateFunction {
        f: AggregateType,
        args: Vec<CompiledExpr>,
        t: DataType,
    },
}