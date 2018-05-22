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

#[cfg(target_family = "unix")]
use liner::Context;
use std::io;

const DEFAULT_PROMPT: &'static str = "datafusion> ";
const CONTINUE_PROMPT: &'static str = "> ";

#[cfg(target_family = "unix")]
pub enum LineResult {
    Break,
    Input(String),
}

#[cfg(target_family = "unix")]
pub struct LineReader<'a> {
    reader: Context,
    prompt: &'a str,
}

#[cfg(target_family = "unix")]
impl<'a> LineReader<'a> {
    pub fn new() -> Self {
        LineReader {
            reader: Context::new(),
            prompt: DEFAULT_PROMPT,
        }
    }

    pub fn set_prompt(&mut self, prompt: &'a str) {
        self.prompt = prompt;
    }

    pub fn read_lines(&mut self) -> Option<LineResult> {
        let mut result = String::new();
        loop {
            let line = self.reader.read_line(self.prompt, &mut |_| {});

            match line {
                Ok(i) => {
                    result.push_str(i.as_str());

                    match i.as_str() {
                        "quit" | "exit" => {
                            return Some(LineResult::Break);
                        }
                        _ => {
                            // Handle the two types of statements, Default and Continue.
                            // CONTINUE: are statements that don't end with a semicolon
                            // DEFAULT: are statements that end with a semicolon
                            // and can be returned to being executed.
                            if i.as_str().ends_with(';') {
                                self.set_prompt(DEFAULT_PROMPT);
                                break;
                            } else {
                                self.set_prompt(CONTINUE_PROMPT);
                                result.push_str(" ");
                                continue;
                            }
                        }
                    }
                }
                Err(e) => {
                    match e.kind() {
                        // ctrl-c pressed
                        io::ErrorKind::Interrupted => {}
                        // ctrl-d pressed
                        io::ErrorKind::UnexpectedEof => {
                            return Some(LineResult::Break);
                        }
                        _ => {
                            // Ensure that all writes to the history file
                            // are written before exiting.
                            self.reader.history.commit_history();
                            panic!("error: {:?}", e)
                        }
                    }
                }
            };
        }

        if !result.trim().is_empty() {
            self.reader.history.push(result.clone().into()).unwrap();
        }

        // Return the command without semicolon
        Some(LineResult::Input(result[..result.len() - 1].to_string()))
    }
}
