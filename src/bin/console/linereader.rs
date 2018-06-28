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

extern crate rustyline;
use self::rustyline::error::ReadlineError;
use self::rustyline::Editor;

const DEFAULT_PROMPT: &'static str = "datafusion> ";
const CONTINUE_PROMPT: &'static str = "> ";

#[cfg(target_family = "unix")]
pub enum LineResult {
    Break,
    Input(String),
}

#[cfg(target_family = "unix")]
pub struct LineReader<'a> {
    reader: Editor<()>,
    prompt: &'a str,
}

#[cfg(target_family = "unix")]
impl<'a> LineReader<'a> {
    pub fn new() -> Self {
        LineReader {
            reader: Editor::<()>::new(),
            prompt: DEFAULT_PROMPT,
        }
    }

    pub fn set_prompt(&mut self, prompt: &'a str) {
        self.prompt = prompt;
    }

    pub fn read_lines(&mut self) -> Option<LineResult> {
        let mut result = String::new();

        //        if rl.load_history("history.txt").is_err() {
        //            println!("No previous history.");
        //        }
        loop {
            let line = self.reader.readline(self.prompt);

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
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    break;
                }
                Err(ReadlineError::Eof) => {
                    println!("CTRL-D");
                    return Some(LineResult::Break);
                }
                Err(err) => {
                    println!("Error: {:?}", err);
                    break;
                }
            };
        }
        //self.reader.save_history("history.txt").unwrap();

        if !result.trim().is_empty() {
            self.reader.add_history_entry(&result);
        }

        // Return the command without semicolon
        Some(LineResult::Input(result[..result.len() - 1].to_string()))
    }
}
