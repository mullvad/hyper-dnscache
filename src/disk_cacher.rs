use hyper::client::connect::dns::Name;
use std::{collections::HashMap, fs, io, net::IpAddr, path::PathBuf, str::FromStr};

pub trait DiskCache {
    type Error: std::error::Error;

    fn load(&mut self) -> Result<HashMap<Name, Vec<IpAddr>>, Self::Error>;

    fn store(&mut self, cache: HashMap<Name, Vec<IpAddr>>) -> Result<(), Self::Error>;
}


/// The default `DiskCache` implementation. Stores the DNS cache serialized as JSON.
pub struct JsonCacher(PathBuf);

impl JsonCacher {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self(path.into())
    }
}

impl DiskCache for JsonCacher {
    type Error = CacheError;

    fn load(&mut self) -> Result<HashMap<Name, Vec<IpAddr>>, Self::Error> {
        // Load and deserialize cache file.
        let cache_data = fs::read(&self.0).map_err(|e| CacheError::ReadFileError(e))?;
        let file_cache: HashMap<String, Vec<IpAddr>> = serde_json::from_slice(&cache_data)
            .map_err(|e| CacheError::DeserializeCacheError(e))?;

        // Insert all entries from the cache loaded from disk. May overwrite entries from the
        // first in-memory cache.
        let mut cache = HashMap::with_capacity(file_cache.len());
        for (name_str, addrs) in file_cache {
            let name =
                Name::from_str(&name_str).map_err(|e| CacheError::InvalidDomainNameError(e))?;
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

        let cache_data = serde_json::to_vec_pretty(&file_cache)
            .map_err(|e| CacheError::SerializeCacheError(e))?;
        fs::write(&self.0, &cache_data).map_err(|e| CacheError::WriteFileError(e))?;
        Ok(())
    }
}

#[derive(Debug, err_derive::Error)]
pub enum CacheError {
    #[error(display = "Failed to read cache file content")]
    ReadFileError(#[error(cause)] io::Error),
    #[error(display = "Failed to deserialize cache file content")]
    DeserializeCacheError(#[error(cause)] serde_json::Error),
    #[error(display = "Cache contained invalid domain name")]
    InvalidDomainNameError(#[error(cause)] hyper::client::connect::dns::InvalidNameError),
    #[error(display = "Failed to write serialized cache data to file")]
    WriteFileError(#[error(cause)] io::Error),
    #[error(display = "Failed to serialize cache")]
    SerializeCacheError(#[error(cause)] serde_json::Error),
}
