use crate::error::{self, Error};
use bufstream::BufStream;
use libc::getpid;
use std::io;
use std::io::prelude::*;
use std::net::TcpStream;
use url::Url;

pub(crate) const EXPECTED_PROTOCOL_VERSION: usize = 2;

mod single;

pub use self::single::{Ack, Fail, Job};

pub(crate) fn get_env_url() -> String {
    use std::env;
    let var = env::var("FAKTORY_PROVIDER").unwrap_or_else(|_| "FAKTORY_URL".to_string());
    env::var(var).unwrap_or_else(|_| "tcp://localhost:7419".to_string())
}

pub(crate) fn host_from_url(url: &Url) -> String {
    format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(7419))
}

pub(crate) fn url_parse(url: &str) -> Result<Url, Error> {
    let url = Url::parse(url).map_err(error::Connect::ParseUrl)?;
    if url.scheme() != "tcp" {
        return Err(error::Connect::BadScheme {
            scheme: url.scheme().to_string(),
        }
        .into());
    }

    if url.host_str().is_none() || url.host_str().unwrap().is_empty() {
        return Err(error::Connect::MissingHostname.into());
    }

    Ok(url)
}

/// A stream that can be re-established after failing.
pub trait Reconnect: Sized {
    /// Re-establish the stream.
    fn reconnect(&self) -> io::Result<Self>;
}

impl Reconnect for TcpStream {
    fn reconnect(&self) -> io::Result<Self> {
        TcpStream::connect(self.peer_addr().unwrap())
    }
}

#[derive(Clone)]
pub(crate) struct ClientOptions {
    /// Hostname to advertise to server.
    /// Defaults to machine hostname.
    pub(crate) hostname: Option<String>,

    /// PID to advertise to server.
    /// Defaults to process ID.
    pub(crate) pid: Option<usize>,

    /// Worker ID to advertise to server.
    /// Defaults to a GUID.
    pub(crate) wid: Option<String>,

    /// Labels to advertise to server.
    /// Defaults to ["rust"].
    pub(crate) labels: Vec<String>,

    /// Password to authenticate with
    /// Defaults to None,
    pub(crate) password: Option<String>,

    is_producer: bool,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            hostname: None,
            pid: None,
            wid: None,
            labels: vec!["rust".to_string()],
            password: None,
            is_producer: false,
        }
    }
}

pub(crate) struct Client<S: Read + Write> {
    stream: BufStream<S>,
    opts: ClientOptions,
}

impl<S> Client<S>
where
    S: Read + Write + Reconnect,
{
    pub(crate) fn connect_again(&self) -> Result<Self, Error> {
        let s = self.stream.get_ref().reconnect()?;
        Client::new(s, self.opts.clone())
    }

    pub fn reconnect(&mut self) -> Result<(), Error> {
        let s = self.stream.get_ref().reconnect()?;
        self.stream = BufStream::new(s);
        self.init()
    }
}

impl<S: Read + Write> Client<S> {
    pub(crate) fn new(stream: S, opts: ClientOptions) -> Result<Client<S>, Error> {
        let mut c = Client {
            stream: BufStream::new(stream),
            opts,
        };
        c.init()?;
        Ok(c)
    }
}

impl<S: Read + Write> Client<S> {
    fn init(&mut self) -> Result<(), Error> {
        let hi = single::read_hi(&mut self.stream)?;

        if hi.version != EXPECTED_PROTOCOL_VERSION {
            return Err(error::Connect::VersionMismatch {
                ours: EXPECTED_PROTOCOL_VERSION,
                theirs: hi.version,
            }
            .into());
        }

        // fill in any missing options, and remember them for re-connect
        let mut hello = single::Hello::default();
        if !self.opts.is_producer {
            let hostname = self
                .opts
                .hostname
                .clone()
                .or_else(|| hostname::get().ok()?.into_string().ok())
                .unwrap_or_else(|| "local".to_string());
            self.opts.hostname = Some(hostname);
            let pid = self
                .opts
                .pid
                .unwrap_or_else(|| unsafe { getpid() } as usize);
            self.opts.pid = Some(pid);
            let wid = self.opts.wid.clone().unwrap_or_else(|| {
                use rand::{thread_rng, Rng};
                thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .map(char::from)
                    .take(32)
                    .collect()
            });
            self.opts.wid = Some(wid);

            hello.hostname = Some(self.opts.hostname.clone().unwrap());
            hello.wid = Some(self.opts.wid.clone().unwrap());
            hello.pid = Some(self.opts.pid.unwrap());
            hello.labels = self.opts.labels.clone();
        }

        if hi.salt.is_some() {
            if let Some(ref pwd) = self.opts.password {
                hello.set_password(&hi, pwd);
            } else {
                return Err(error::Connect::AuthenticationNeeded.into());
            }
        }

        single::write_command_and_await_ok(&mut self.stream, &hello)
    }
}

impl<S: Read + Write> Drop for Client<S> {
    fn drop(&mut self) {
        single::write_command(&mut self.stream, &single::End).unwrap();
    }
}

pub struct ReadToken<'a, S: Read + Write>(&'a mut Client<S>);

pub(crate) enum HeartbeatStatus {
    Ok,
    Terminate,
    Quiet,
}

impl<S: Read + Write> Client<S> {
    pub(crate) fn issue<FC: self::single::FaktoryCommand>(
        &mut self,
        c: &FC,
    ) -> Result<ReadToken<'_, S>, Error> {
        single::write_command(&mut self.stream, c)?;
        Ok(ReadToken(self))
    }

    pub(crate) fn heartbeat(&mut self) -> Result<HeartbeatStatus, Error> {
        Ok(HeartbeatStatus::Terminate)
    }

    pub(crate) fn fetch<Q>(&mut self, queues: &[Q]) -> Result<Option<Job>, Error>
    where
        Q: AsRef<str>,
    {
        self.issue(&single::Fetch::from(queues))?.read_json()
    }
}

impl<'a, S: Read + Write> ReadToken<'a, S> {
    pub(crate) fn await_ok(self) -> Result<(), Error> {
        single::read_ok(&mut self.0.stream)
    }

    pub(crate) fn read_json<T>(self) -> Result<Option<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        single::read_json(&mut self.0.stream)
    }
}
