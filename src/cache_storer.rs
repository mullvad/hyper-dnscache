use futures::{Async, Future, Poll};
use hyper::client::connect::dns::{InvalidNameError, Name};
use std::{collections::HashMap, fs, io, net::IpAddr, path::PathBuf, str::FromStr};


pub trait CacheStorer {
    type Error: std::error::Error;
    type StoreFuture: Future<Item = (), Error = Self::Error>;

    fn load(&mut self) -> Result<HashMap<Name, Vec<IpAddr>>, Self::Error>;

    fn store(&mut self, cache: HashMap<Name, Vec<IpAddr>>) -> Self::StoreFuture;
}


/// The default `CacheStorer` implementation. Stores the DNS cache serialized as JSON.
pub struct JsonStorer(PathBuf);

impl JsonStorer {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self(path.into())
    }
}

impl CacheStorer for JsonStorer {
    type Error = Error;
    type StoreFuture = Write;

    fn load(&mut self) -> Result<HashMap<Name, Vec<IpAddr>>, Self::Error> {
        // Load and deserialize cache file.
        let cache_data = fs::read(&self.0).map_err(|e| Error::ReadFileError(self.0.clone(), e))?;
        let file_cache: HashMap<String, Vec<IpAddr>> = serde_json::from_slice(&cache_data)
            .map_err(|e| Error::DeserializeCacheError(self.0.clone(), e))?;

        // Convert the map so the `String` key becomes a `Name`.
        let mut cache = HashMap::with_capacity(file_cache.len());
        for (name_str, addrs) in file_cache {
            let name = Name::from_str(&name_str)
                .map_err(|e| Error::InvalidDomainNameError(name_str, e))?;
            cache.insert(name, addrs);
        }
        log::debug!(
            "Loaded {} DNS entries from {}",
            cache.len(),
            self.0.display()
        );
        Ok(cache)
    }

    fn store(&mut self, cache: HashMap<Name, Vec<IpAddr>>) -> Self::StoreFuture {
        log::debug!(
            "Writing {} DNS entries to {}",
            cache.len(),
            self.0.display()
        );

        // Convert the map so the `Name` key becomes a `String`.
        let file_cache: HashMap<String, Vec<IpAddr>> = cache
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect();


        Write::new(&file_cache, self.0.clone())
    }
}

/// A `Future` that serializes a cache and writes it to a cache file.
pub struct Write {
    path: PathBuf,
    state: Option<WriteState>,
}

impl Write {
    pub fn new(cache: &HashMap<String, Vec<IpAddr>>, path: PathBuf) -> Self {
        let serialize_result = serde_json::to_vec_pretty(cache);
        Self {
            path,
            state: Some(WriteState::Serialize(serialize_result)),
        }
    }
}

enum WriteState {
    Serialize(serde_json::Result<Vec<u8>>),
    CreateFile(tokio_fs::file::CreateFuture<PathBuf>, Vec<u8>),
    Write(tokio_io::io::WriteAll<tokio_fs::File, Vec<u8>>),
}

impl Future for Write {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        use WriteState::*;
        while let Some(state) = self.state.take() {
            match state {
                Serialize(result) => match result {
                    Ok(data) => {
                        log::trace!("Write cache 1/3 done: serialize => create file");
                        let create_file = tokio_fs::File::create(self.path.clone());
                        self.state = Some(CreateFile(create_file, data));
                    }
                    Err(e) => return Err(Error::SerializeCacheError(e)),
                },
                CreateFile(mut create_file, data) => match create_file.poll() {
                    Ok(Async::NotReady) => {
                        self.state = Some(CreateFile(create_file, data));
                        return Ok(Async::NotReady);
                    }
                    Ok(Async::Ready(file)) => {
                        log::trace!("Write cache 2/3 done: create file => write data");
                        let write = tokio_io::io::write_all(file, data);
                        self.state = Some(Write(write));
                    }
                    Err(e) => return Err(Error::WriteFileError(self.path.clone(), e)),
                },
                Write(mut write) => match write.poll() {
                    Ok(Async::NotReady) => {
                        self.state = Some(Write(write));
                        return Ok(Async::NotReady);
                    }
                    Ok(Async::Ready(..)) => log::trace!("Write cache 3/3 done"),
                    Err(e) => return Err(Error::WriteFileError(self.path.clone(), e)),
                },
            }
        }
        Ok(Async::Ready(()))
    }
}

#[derive(Debug, err_derive::Error)]
pub enum Error {
    #[error(display = "Failed to read cache file at {:?}", _0)]
    ReadFileError(PathBuf, #[error(cause)] io::Error),
    #[error(display = "Failed to deserialize cache file at {:?}", _0)]
    DeserializeCacheError(PathBuf, #[error(cause)] serde_json::Error),
    #[error(display = "Cache contained invalid domain name: {}", _0)]
    InvalidDomainNameError(String, #[error(cause)] InvalidNameError),
    #[error(display = "Failed to write cache data to {:?}", _0)]
    WriteFileError(PathBuf, #[error(cause)] io::Error),
    #[error(display = "Failed to serialize cache")]
    SerializeCacheError(#[error(cause)] serde_json::Error),
}
