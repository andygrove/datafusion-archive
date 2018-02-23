use std::fmt;
use linefeed::{DefaultTerminal, ReadResult, Reader};

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

pub struct LineReader {
    reader: Reader<DefaultTerminal>,
}

impl LineReader {
    pub fn new() -> Self {
        let mut reader = Reader::new("datafusion").unwrap();
        reader.set_prompt(DEFAULT_PROMPT);

        LineReader { reader }
    }

    pub fn read_lines(&mut self) -> Result<LineResult, LineReaderError> {
        let mut result = String::new();
        loop {
            let line = self.reader.read_line().unwrap();

            match line {
                ReadResult::Input(i) => {
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
                        self.reader.set_prompt(DEFAULT_PROMPT);
                        break;
                    } else {
                        self.reader.set_prompt(CONTINUE_PROMTP);
                        result.push_str(" ");
                        continue;
                    }
                }
                _ => {}
            };
        }

        if !result.trim().is_empty() {
            self.reader.add_history(result.clone());
        }

        // Return the command without semicolon
        Ok(LineResult::Input(result[..result.len()-1].to_string()))
    }
}
