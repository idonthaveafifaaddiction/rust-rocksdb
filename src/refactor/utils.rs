// Copyright 2018. Starry, Inc. All Rights Reserved.
//
// FIXME based off prior work...
// Copyright 2016 Alex Regueiro
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Software written by Steven Sloboda <ssloboda@starry.com>.

use std::ffi::{CStr, CString};
use std::path::Path;

use libc::{self, c_char, c_void};

use refactor::common::DatabaseVector;
use refactor::errors::Error;

macro_rules! try_ffi {
    ( $($function:ident)::*( $( $arg:expr),* ) ) => ({
        let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
        let result = $($function)::*($($arg),*, &mut err);
        if !err.is_null() {
            return Err(Error::new($crate::refactor::utils::error_message(err)));
        }
        result
    })
}

pub fn error_message(ptr: *const c_char) -> String {
    let cstr = unsafe { CStr::from_ptr(ptr as *const _) };
    let string = String::from_utf8_lossy(cstr.to_bytes()).into_owned();
    unsafe {
        libc::free(ptr as *mut c_void);
    }
    string
}

pub fn c_buf_to_opt_dbvec(value: *mut u8, length: usize) -> Option<DatabaseVector> {
    if value.is_null() {
        None
    } else {
        unsafe {
            Some(DatabaseVector::from_c(value, length))
        }
    }
}

pub fn pathref_to_cstring<P>(path: P) -> Result<CString, Error>
    where P: AsRef<Path>
{
    match CString::new(path.as_ref().to_string_lossy().as_bytes()) {
        Ok(cpath) => Ok(cpath),
        Err(err) => {
            Err(Error::new(
                format!("Failed to convert path to CString: {:?}", err).into()
            ))
        }
    }
}

// pub fn opt_bytes_to_ptr(opt: Option<&[u8]>) -> *const c_char {
//     match opt {
//         Some(v) => v.as_ptr() as *const c_char,
//         None => ptr::null(),
//     }
// }
