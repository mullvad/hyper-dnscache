use hyper::client::connect::dns::{InvalidNameError, Name};
use std::{collections::HashMap, fs, io, net::IpAddr, path::PathBuf, str::FromStr};

pub trait CacheStorer {
    type Error: std::error::Error;

    fn load(&mut self) -> Result<HashMap<Name, Vec<IpAddr>>, Self::Error>;

    fn store(&mut self, cache: HashMap<Name, Vec<IpAddr>>) -> Result<(), Self::Error>;
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

    fn load(&mut self) -> Result<HashMap<Name, Vec<IpAddr>>, Self::Error> {
        // Load and deserialize cache file.
        let cache_data = fs::read(&self.0).map_err(|e| Error::ReadFileError(self.0.clone(), e))?;
        let file_cache: HashMap<String, Vec<IpAddr>> = serde_json::from_slice(&cache_data)
            .map_err(|e| Error::DeserializeCacheError(self.0.clone(), e))?;

        // Insert all entries from the cache loaded from disk. May overwrite entries from the
        // first in-memory cache.
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

    fn store(&mut self, cache: HashMap<Name, Vec<IpAddr>>) -> Result<(), Self::Error> {
        log::debug!(
            "Writing {} DNS entries to {}",
            cache.len(),
            self.0.display()
        );

        let mut file_cache = HashMap::with_capacity(cache.len());
        for (name, addrs) in cache {
            file_cache.insert(name.to_string(), addrs);
        }

        let cache_data =
            serde_json::to_vec_pretty(&file_cache).map_err(|e| Error::SerializeCacheError(e))?;
        fs::write(&self.0, &cache_data).map_err(|e| Error::WriteFileError(self.0.clone(), e))?;
        Ok(())
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
