use std::fmt;
use std::io;
use liner::Context;

const DEFAULT_PROMPT: &'static str = "datafusion> ";
const CONTINUE_PROMTP: &'static str = "> ";

#[derive(Debug)]
pub enum LineReaderError {
    Command(String),
}

impl fmt::Display for LineReaderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            LineReaderError::Command(ref err) => write!(f, "no such command: `{}`", err),
        }
    }
}

pub enum LineResult {
    Break,
    Input(String),
}

pub struct LineReader<'a> {
    reader: Context,
    prompt: &'a str,
}

impl<'a> LineReader<'a> {
    pub fn new() -> Self {
        let reader = Context::new();
        LineReader {
            reader,
            prompt: DEFAULT_PROMPT,
        }
    }

    pub fn set_prompt(&mut self, prompt: &'a str) {
        self.prompt = prompt;
    }

    pub fn read_lines(&mut self) -> Result<LineResult, LineReaderError> {
        let mut result = String::new();
        loop {
            let line = self.reader.read_line(self.prompt, &mut |_| {});

            match line {
                Ok(i) => {
                    result.push_str(i.as_str());

                    // Handles commands if the input starts
                    // whit a colon.
                    if i.as_str().starts_with(':') {
                        match i.as_str() {
                            ":quit" | ":exit" => {
                                return Ok(LineResult::Break);
                            }
                            _ => return Err(LineReaderError::Command(i)),
                        }
                    }

                    // Handle the two types of statements, Default and Continue.
                    // CONTINUE: are statements that don't end with a semicolon
                    // DEFAULT: are statements that end with a semicolon
                    // and can be returned to being executed.
                    if i.as_str().ends_with(';') {
                        self.set_prompt(DEFAULT_PROMPT);
                        break;
                    } else {
                        self.set_prompt(CONTINUE_PROMTP);
                        result.push_str(" ");
                        continue;
                    }
                }
                Err(e) => {
                    match e.kind() {
                        // ctrl-c pressed
                        io::ErrorKind::Interrupted => {}
                        // ctrl-d pressed
                        io::ErrorKind::UnexpectedEof => {
                            return Ok(LineResult::Break);
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
        Ok(LineResult::Input(result[..result.len() - 1].to_string()))
    }
}
