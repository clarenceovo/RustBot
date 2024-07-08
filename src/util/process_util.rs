use std::process;
pub struct ProcessUtils;

impl ProcessUtils {

    fn kill_program() {
        println!("Killing the program");
        process::exit(0);
    }
    
}