use linefeed::{DefaultTerminal, ReadResult, Reader};

const DEFAULT_PROMPT: &'static str = "datafusion> ";
const CONTINUE_PROMTP: &'static str = "> ";

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

    pub fn read_lines(&mut self) -> LineResult {
        let mut result = String::new();
        loop {
            let line = self.reader.read_line().unwrap();

            match line {
                ReadResult::Input(i) => {
                    result.push_str(i.as_str());

                    // Handle Commands
                    if i.as_str().starts_with(':') {
                        match i.as_str() {
                            ":quit" | ":exit" => {
                                return LineResult::Break;
                            }
                            _ => continue,
                        }
                    }

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
        LineResult::Input(result[..result.len()-1].to_string())
    }
}
