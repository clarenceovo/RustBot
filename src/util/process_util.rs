use std::process;
pub struct ProcessUtils;

impl ProcessUtils {

    pub fn kill_program() {
        println!("Killing the program");
        process::exit(0);
    }
    
}