use std::vec::Vec;

/// Possible requests our clients can send us
pub enum Request {
    Get { key: Vec<u8> },
    Set { key: Vec<u8>, value: Vec<u8> },
}

/// Responses to the `Request` commands above
pub enum Response {
    Value { key: Vec<u8>, value: Vec<u8> },
    Set { key: Vec<u8>, value: Vec<u8> },
    Error { msg: String },
}

impl Request {
    pub fn parse(input: &str) -> Result<Request, String> {
        let mut parts = input.splitn(3, ' ');
        match parts.next() {
            Some("GET") => {
                let key = parts.next().ok_or("GET must be followed by a key")?;
                if parts.next().is_some() {
                    return Err("GET's key must not be followed by anything".into());
                }
                Ok(Request::Get {
                    key: key.as_bytes().to_vec(),
                })
            }
            Some("SET") => {
                let key = match parts.next() {
                    Some(key) => key,
                    None => return Err("SET must be followed by a key".into()),
                };
                let value = match parts.next() {
                    Some(value) => value,
                    None => return Err("SET needs a value".into()),
                };
                Ok(Request::Set {
                    key: key.as_bytes().to_vec(),
                    value: value.as_bytes().to_vec(),
                })
            }
            Some(cmd) => Err(format!("unknown command: {}", cmd)),
            None => Err("empty input".into()),
        }
    }
}

impl Response {
    pub fn serialize(&self) -> String {
        match *self {
            Response::Value { ref key, ref value } => format!("{:?} = {:?}", key, value),
            Response::Set { ref key, ref value } => format!("set {:?} = `{:?}`", key, value),
            Response::Error { ref msg } => format!("error: {}", msg),
        }
    }
}
