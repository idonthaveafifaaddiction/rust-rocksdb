// Copyright 2018. Starry, Inc. All Rights Reserved.
//
// FIXME based off prior work...
// Copyright 2014 Tyler Neely
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

/// A simple wrapper round a string, used for errors reported from ffi calls.
#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    message: String
}

impl Error {
    pub(crate) fn new(message: String) -> Self {
        Error { message }
    }
}

impl AsRef<str> for Error {
    fn as_ref(&self) -> &str {
        &self.message
    }
}

// FIXME needed?
// impl From<Error> for String {
//     fn from(error: Error) -> Self {
//         error.message
//     }
// }

impl std::error::Error for Error {
    fn description(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.message.fmt(formatter)
    }
}
