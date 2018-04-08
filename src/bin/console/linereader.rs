//use liner::Context;
//use std::io;
//
//const DEFAULT_PROMPT: &'static str = "datafusion> ";
//const CONTINUE_PROMTP: &'static str = "> ";
//
//pub enum LineResult {
//    Break,
//    Input(String),
//}
//
//pub struct LineReader<'a> {
//    reader: Context,
//    prompt: &'a str,
//}
//
//impl<'a> LineReader<'a> {
//    pub fn new() -> Self {
//        LineReader {
//            reader: Context::new(),
//            prompt: DEFAULT_PROMPT,
//        }
//    }
//
//    pub fn set_prompt(&mut self, prompt: &'a str) {
//        self.prompt = prompt;
//    }
//
//    pub fn read_lines(&mut self) -> Option<LineResult> {
//        let mut result = String::new();
//        loop {
//            let line = self.reader.read_line(self.prompt, &mut |_| {});
//
//            match line {
//                Ok(i) => {
//                    result.push_str(i.as_str());
//
//                    match i.as_str() {
//                        "quit" | "exit" => {
//                            return Some(LineResult::Break);
//                        }
//                        _ => {
//                            // Handle the two types of statements, Default and Continue.
//                            // CONTINUE: are statements that don't end with a semicolon
//                            // DEFAULT: are statements that end with a semicolon
//                            // and can be returned to being executed.
//                            if i.as_str().ends_with(';') {
//                                self.set_prompt(DEFAULT_PROMPT);
//                                break;
//                            } else {
//                                self.set_prompt(CONTINUE_PROMTP);
//                                result.push_str(" ");
//                                continue;
//                            }
//                        }
//                    }
//                }
//                Err(e) => {
//                    match e.kind() {
//                        // ctrl-c pressed
//                        io::ErrorKind::Interrupted => {}
//                        // ctrl-d pressed
//                        io::ErrorKind::UnexpectedEof => {
//                            return Some(LineResult::Break);
//                        }
//                        _ => {
//                            // Ensure that all writes to the history file
//                            // are written before exiting.
//                            self.reader.history.commit_history();
//                            panic!("error: {:?}", e)
//                        }
//                    }
//                }
//            };
//        }
//
//        if !result.trim().is_empty() {
//            self.reader.history.push(result.clone().into()).unwrap();
//        }
//
//        // Return the command without semicolon
//        Some(LineResult::Input(result[..result.len() - 1].to_string()))
//    }
//}
