// Copyright 2018. Starry, Inc. All Rights Reserved.
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

use std::ffi::CString;
use std::path::Path;

use Error;

pub fn pathref_to_cstring<P>(path: P) -> Result<CString, Error>
    where P: AsRef<Path>
{
    match CString::new(path.as_ref().to_string_lossy().as_bytes()) {
        Ok(cpath) => Ok(cpath),
        Err(err) => {
            Err(Error::new(
                format!("Failed to convert path to CString when opening DB: {:?}", err).into()
            ))
        }
    }
}
