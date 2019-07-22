use std::ffi::{CString, CStr};
use std::str;
use libc::{c_char, c_int};

use err::HdfsErr;
use native::*;
use dfs::HdfsFs;

pub fn str_to_chars(s: &mut String) -> *const c_char {
  s.push('\0');
  s.as_bytes().as_ptr() as *const i8
}

pub fn chars_to_str<'a>(chars: *const c_char) -> &'a str {
  let slice = unsafe { CStr::from_ptr(chars) }.to_bytes();
  str::from_utf8(slice).unwrap()
}

pub fn bool_to_c_int(val: bool) -> c_int {
  if val { 1 } else { 0 }
}

/// Hdfs Utility
pub struct HdfsUtil;

/// HDFS Utility
impl HdfsUtil {

  /// Copy file from one filesystem to another.
  ///
  /// #### Params
  /// * ```srcFS``` - The handle to source filesystem.
  /// * ```src``` - The path of source file.
  /// * ```dstFS``` - The handle to destination filesystem.
  /// * ```dst``` - The path of destination file.
  pub fn copy(src_fs: &HdfsFs, src: &str, dst_fs: &HdfsFs, dst: &str)
      -> Result<bool, HdfsErr> {
    let res = unsafe {
      let mut s_src = String::from(src);
      let mut s_dst = String::from(dst);
      hdfsCopy(src_fs.raw(), str_to_chars(&mut s_src), dst_fs.raw(), str_to_chars(&mut s_dst))
    };

    if res == 0 {
      Ok(true)
    } else {
      Err(HdfsErr::Unknown)
    }
  }

  /// Move file from one filesystem to another.
  ///
  /// #### Params
  /// * ```srcFS``` - The handle to source filesystem.
  /// * ```src``` - The path of source file.
  /// * ```dstFS``` - The handle to destination filesystem.
  /// * ```dst``` - The path of destination file.
  pub fn mv(src_fs: &HdfsFs, src: &str, dst_fs: &HdfsFs, dst: &str)
      -> Result<bool, HdfsErr> {

    let res = unsafe {
      let mut s_src = String::from(src);
      let mut s_dst = String::from(dst);
      hdfsMove(src_fs.raw(), str_to_chars(&mut s_src), dst_fs.raw(), str_to_chars(&mut s_dst))
    };

    if res == 0 {
      Ok(true)
    } else {
      Err(HdfsErr::Unknown)
    }
  }
}