fn main() {
    //Passing environment variable to compiler to prompt the static and shared library path
    println!("cargo:rustc-link-search=native=src/io/hdfs/hdfs_lib/src/native");
}