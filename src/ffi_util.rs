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

use libc::{self, c_char, c_void};
use std::ffi::CStr;

pub fn error_message(ptr: *const c_char) -> String {
    let cstr = unsafe { CStr::from_ptr(ptr as *const _) };
    let s = String::from_utf8_lossy(cstr.to_bytes()).into_owned();
    unsafe {
        libc::free(ptr as *mut c_void);
    }
    s
}

macro_rules! ffi_try {
    ( $($function:ident)::*( $( $arg:expr ),* ) ) => ({
        let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
        let result = $($function)::*($($arg),*, &mut err);
        if !err.is_null() {
            return Err(Error::new($crate::ffi_util::error_message(err)));
        }
        result
    })
}
