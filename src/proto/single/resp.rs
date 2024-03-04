use crate::error::{self, Error};
use std::io::prelude::*;

fn bad(expected: &'static str, got: &RawResponse) -> error::Protocol {
    let stringy = match *got {
        RawResponse::String(ref s) => Some(&**s),
        RawResponse::Blob(ref b) => {
            if let Ok(s) = std::str::from_utf8(b) {
                Some(s)
            } else {
                None
            }
        }
        _ => None,
    };

    match stringy {
        Some(s) => error::Protocol::BadType {
            expected,
            received: s.to_string(),
        },
        None => error::Protocol::BadType {
            expected,
            received: format!("{:?}", got),
        },
    }
}

// ----------------------------------------------

pub fn read_json<R: BufRead, T: serde::de::DeserializeOwned>(r: R) -> Result<Option<T>, Error> {
    let rr = read(r)?;
    match rr {
        RawResponse::String(ref s) if s == "OK" => {
            return Ok(None);
        }
        RawResponse::String(ref s) => {
            return serde_json::from_str(s)
                .map(Some)
                .map_err(Error::Serialization);
        }
        RawResponse::Blob(ref b) if b == b"OK" => {
            return Ok(None);
        }
        RawResponse::Blob(ref b) => {
            if b.is_empty() {
                return Ok(None);
            }
            return serde_json::from_slice(b)
                .map(Some)
                .map_err(Error::Serialization);
        }
        RawResponse::Null => return Ok(None),
        _ => {}
    };

    Err(bad("json", &rr).into())
}

// ----------------------------------------------

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Hi {
    #[serde(rename = "v")]
    pub version: usize,
    #[serde(rename = "i")]
    pub iterations: Option<usize>,
    #[serde(rename = "s")]
    pub salt: Option<String>,
}

pub fn read_hi<R: BufRead>(r: R) -> Result<Hi, Error> {
    let rr = read(r)?;
    if let RawResponse::String(ref s) = rr {
        if let Some(s) = s.strip_prefix("HI ") {
            return serde_json::from_str(s).map_err(Error::Serialization);
        }
    }

    Err(bad("server hi", &rr).into())
}

// ----------------------------------------------

pub fn read_ok<R: BufRead>(r: R) -> Result<(), Error> {
    let rr = read(r)?;
    if let RawResponse::String(ref s) = rr {
        if s == "OK" {
            return Ok(());
        }
    }

    Err(bad("server ok", &rr).into())
}

// ----------------------------------------------
//
// below is the implementation of the Redis RESP protocol
//
// ----------------------------------------------

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum RawResponse {
    String(String),
    Blob(Vec<u8>),
    Number(isize),
    Null,
}

fn read<R: BufRead>(mut r: R) -> Result<RawResponse, Error> {
    let mut cmdbuf = [0u8; 1];
    r.read_exact(&mut cmdbuf)?;
    match cmdbuf[0] {
        b'+' => {
            // Simple String
            // https://redis.io/topics/protocol#resp-simple-strings
            let mut s = String::new();
            r.read_line(&mut s)?;

            // remove newlines
            let l = s.len() - 2;
            s.truncate(l);

            Ok(RawResponse::String(s))
        }
        b'-' => {
            // Error
            // https://redis.io/topics/protocol#resp-errors
            let mut s = String::new();
            r.read_line(&mut s)?;

            // remove newlines
            let l = s.len() - 2;
            s.truncate(l);

            Err(error::Protocol::new(s).into())
        }
        b':' => {
            // Integer
            // https://redis.io/topics/protocol#resp-integers
            let mut s = String::with_capacity(32);
            r.read_line(&mut s)?;

            // remove newlines
            let l = s.len() - 2;
            s.truncate(l);

            match (*s).parse::<isize>() {
                Ok(i) => Ok(RawResponse::Number(i)),
                Err(_) => Err(error::Protocol::BadResponse {
                    typed_as: "integer",
                    error: "invalid integer value",
                    bytes: s.into_bytes(),
                }
                .into()),
            }
        }
        b'$' => {
            // Bulk String
            // https://redis.io/topics/protocol#resp-bulk-strings
            let mut bytes = Vec::with_capacity(32);
            r.read_until(b'\n', &mut bytes)?;
            let s = std::str::from_utf8(&bytes[0..bytes.len() - 2]).map_err(|_| {
                error::Protocol::BadResponse {
                    typed_as: "bulk string",
                    error: "server bulk response contains non-utf8 size prefix",
                    bytes: bytes[0..bytes.len() - 2].to_vec(),
                }
            })?;

            let size = s
                .parse::<isize>()
                .map_err(|_| error::Protocol::BadResponse {
                    typed_as: "bulk string",
                    error: "server bulk response size prefix is not an integer",
                    bytes: s.as_bytes().to_vec(),
                })?;

            if size == -1 {
                Ok(RawResponse::Null)
            } else {
                let size = size as usize;
                let mut bytes = vec![0; size];
                r.read_exact(&mut bytes[..])?;
                r.read_exact(&mut [0u8; 2])?;
                Ok(RawResponse::Blob(bytes))
            }
        }
        b'*' => {
            // Arrays
            // https://redis.io/topics/protocol#resp-arrays
            //
            // not used in faktory.
            // *and* you can't really skip them unless you parse them.
            // *and* not parsing them would leave the stream in an inconsistent state.
            // so we'll just give up
            unimplemented!();
        }
        c => Err(error::Protocol::BadResponse {
            typed_as: "unknown",
            error: "invalid response type prefix",
            bytes: vec![c],
        }
        .into()),
    }
}

// these are mostly for convenience for testing

impl<'a> From<&'a str> for RawResponse {
    fn from(s: &'a str) -> Self {
        RawResponse::String(s.to_string())
    }
}

impl From<isize> for RawResponse {
    fn from(i: isize) -> Self {
        RawResponse::Number(i)
    }
}

impl From<Vec<u8>> for RawResponse {
    fn from(b: Vec<u8>) -> Self {
        RawResponse::Blob(b)
    }
}
