mod frame;
use mylib::Error;
fn main() {
    let e: frame::Error = "string".into();
    match e {
        frame::Error::Incomplete => println!("aaa"),
        frame::Error::Other(e) => println!("{e} 1111"),
    }
}
