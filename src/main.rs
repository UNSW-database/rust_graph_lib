extern crate serde_json;

use serde_json::json;
use serde_json::Number;

trait MyJson {
    fn process(&self);
}

impl MyJson for f64 {
    fn process(&self) {
        println!("I'm f64")
    }
}

impl MyJson for u64 {
    fn process(&self) {
        println!("I'm u64")
    }
}

impl MyJson for &str {
    fn process(&self) {
        println!("I'm string")
    }
}

impl MyJson for bool {
    fn process(&self) {
        println!("I'm bool")
    }
}

macro_rules! my_json {
    ($e:expr) => {
        MyJson::process(&$e)
    };
}

fn main() {
    println!("{:?}", json!(5.5).as_u64());
}
