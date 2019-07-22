use std::fmt;
use std::fmt::{Error, Formatter};

/// Errors which can occur during accessing Hdfs cluster
pub enum HdfsErr{
  Unknown,
  /// file path
  FileNotFound(String),
  /// file path           
  FileAlreadyExists(String),
  /// namenode address      
  CannotConnectToNameNode(String),
  /// URL 
  InvalidUrl(String) 
}

impl std::fmt::Debug for HdfsErr{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let mut error;
      match self {
        HdfsErr::Unknown=>error= String::from("Unknow"),
        HdfsErr::FileNotFound(e)=>error= String::from("FileNotFound"),
        HdfsErr::FileAlreadyExists(e)=>error= String::from("FileAlreadyExists"),
        HdfsErr::CannotConnectToNameNode(e)=>error= String::from("CannotConnectToNameNode"),
        HdfsErr::InvalidUrl(e)=>error= String::from("InvalidUrl"),
      }
      write!(f, "{}",error)
    }
}

impl fmt::Display for HdfsErr{
  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
    Ok(())
  }
}

impl std::error::Error for HdfsErr{

}