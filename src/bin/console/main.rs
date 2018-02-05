extern crate rprompt;
extern crate datafusion;

use datafusion::rel::*;
use datafusion::exec::*;

/// Interactive SQL console
struct Console {
    ctx: ExecutionContext
}

impl Console {
    fn execute(&mut self, command: &str) {
        println!("Executing: {}", command);
        match self.ctx.sql(command) {
            Ok(_ /* ref df */ ) => {
                //TODO: show data from df
                println!("Executed OK");
            },
            Err(e) => println!("Error: {:?}", e)
        }
    }
}

fn main() {

    println!("DataFusion Console");

    let mut console = Console { ctx: ExecutionContext::new() };

    loop {
        let command = rprompt::prompt_reply_stdout("$ ").unwrap();

        match command.to_lowercase().as_ref() {
            "exit" | "quit" => break,
            _ => console.execute(&command)
        }

    }
}
