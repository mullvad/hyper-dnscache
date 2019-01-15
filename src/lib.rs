use futures::{
    stream::Fuse,
    sync::{mpsc, oneshot},
    Async, Future, Poll, Sink, Stream,
};
use hyper::client::connect::dns::{Name, Resolve};
use std::{
    collections::HashMap,
    io,
    net::IpAddr,
    path::PathBuf,
    time::{Duration, Instant},
    vec::IntoIter,
};

// TODO:
// * Read cache on creation
// * Writing cache to disk on each change
// * Optional filtering of results from underlying resolver

mod timeout;
use self::timeout::OptionalTimeout;


type Cache = HashMap<String, CacheEntry>;

struct CacheEntry {
    /// The IPs for this domain.
    addrs: Vec<IpAddr>,
    /// The time when these addresses were last updated.
    /// `None` for entries that were never resolved, but remain at their initial values.
    timestamp: Option<Instant>,
}

pub struct CachedResolverBuilder<R: Resolve> {
    resolver: R,
    resolver_timeout: Option<Duration>,
    cache: Option<Cache>,
    cache_expiry: Option<Duration>,
    cache_file: Option<PathBuf>,
}

impl<R: Resolve> CachedResolverBuilder<R> {
    pub fn new(resolver: R) -> Self {
        Self {
            resolver,
            resolver_timeout: None,
            cache: None,
            cache_expiry: None,
            cache_file: None,
        }
    }

    pub fn timeout(mut self, resolver_timeout: Duration) -> Self {
        self.resolver_timeout = Some(resolver_timeout);
        self
    }

    pub fn cache(mut self, cache: HashMap<String, Vec<IpAddr>>) -> Self {
        let mut timestamped_cache = HashMap::with_capacity(cache.len());
        for (name, addrs) in cache {
            timestamped_cache.insert(
                name,
                CacheEntry {
                    addrs,
                    timestamp: None,
                },
            );
        }
        self.cache = Some(timestamped_cache);
        self
    }

    pub fn cache_expiry(mut self, expiry: Duration) -> Self {
        self.cache_expiry = Some(expiry);
        self
    }

    pub fn cache_file(mut self, cache_file: impl Into<PathBuf>) -> Self {
        self.cache_file = Some(cache_file.into());
        self
    }

    pub fn build(self) -> (CachedResolver<R>, ResolverHandle) {
        let (handles_tx, handles_rx) = mpsc::channel(0);
        let cached_resolver = CachedResolver {
            resolver: self.resolver,
            resolver_timeout: self.resolver_timeout,
            cache: self.cache.unwrap_or_default(),
            cache_expiry: self.cache_expiry,
            _cache_file: self.cache_file,
            handles_rx: handles_rx.fuse(),
            ongoing_resolutions: HashMap::new(),
        };
        let handle = ResolverHandle {
            resolver: handles_tx,
        };
        (cached_resolver, handle)
    }
}

pub struct CachedResolver<R: Resolve> {
    resolver: R,
    resolver_timeout: Option<Duration>,
    cache: Cache,
    cache_expiry: Option<Duration>,
    _cache_file: Option<PathBuf>,
    handles_rx: Fuse<mpsc::Receiver<(Name, oneshot::Sender<IntoIter<IpAddr>>)>>,
    ongoing_resolutions: HashMap<
        String,
        (
            OptionalTimeout<R::Future>,
            Vec<oneshot::Sender<IntoIter<IpAddr>>>,
        ),
    >,
}

impl<R: Resolve> CachedResolver<R> {
    pub fn builder(resolver: R) -> CachedResolverBuilder<R> {
        CachedResolverBuilder::new(resolver)
    }
}

impl<R: Resolve> CachedResolver<R> {
    fn poll_handles(&mut self) -> Async<()> {
        // Process requests from all handles
        loop {
            match self.handles_rx.poll() {
                // Nothing new from the handles
                Ok(Async::NotReady) => {
                    break;
                }
                // All handles are gone, no more requests will come in.
                Ok(Async::Ready(None)) => {
                    return Async::Ready(());
                }
                // Incoming resolve request from a handle
                Ok(Async::Ready(Some((name, listener)))) => {
                    if let Some(addrs) = self.get_cache_entry(&name) {
                        log::debug!(
                            "Replying from cache for \"{}\" with {:?}",
                            name.as_str(),
                            addrs
                        );
                        let _ = listener.send(addrs);
                    } else if let Some((_fut, listeners)) =
                        self.ongoing_resolutions.get_mut(name.as_str())
                    {
                        log::trace!(
                            "Adding listener for existing resolution of \"{}\"",
                            name.as_str()
                        );
                        listeners.push(listener);
                    } else {
                        self.resolve(name, listener);
                    }
                }
                // Channels should not error
                Err(()) => {
                    log::error!("Handle channel receiver got an error");
                    unreachable!()
                }
            }
        }
        Async::NotReady
    }

    /// Returns the addresses for a name if it exists in the cache and the cache entry is not too
    /// old.
    fn get_cache_entry(&self, name: &Name) -> Option<IntoIter<IpAddr>> {
        if let Some(cache_entry) = self.cache.get(name.as_str()) {
            let cache_is_valid = match (self.cache_expiry, cache_entry.timestamp) {
                // Our cache does not have an expiry, always valid.
                (None, _) => true,
                // Cache can expire, current entry has no timestamp, outdated.
                (Some(_), None) => false,
                // Cache can expire, current entry has timestamp, compare.
                (Some(cache_expiry), Some(cache_timestamp)) => {
                    cache_timestamp.elapsed() < cache_expiry
                }
            };
            if cache_is_valid {
                Some(cache_entry.addrs.clone().into_iter())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn resolve(&mut self, name: Name, listener: oneshot::Sender<IntoIter<IpAddr>>) {
        let name_str = name.as_str().to_string();
        log::debug!("Resolving \"{}\"", name_str);
        let resolve_future =
            OptionalTimeout::new(self.resolver.resolve(name), self.resolver_timeout);
        let listeners = vec![listener];
        self.ongoing_resolutions
            .insert(name_str, (resolve_future, listeners));
    }

    fn poll_ongoing_resolutions(&mut self) -> Async<()> {
        // Process all ongoing DNS resolutions
        let mut finished_resolutions = Vec::new();
        for (name, (resolve_future, _listeners)) in &mut self.ongoing_resolutions {
            match resolve_future.poll() {
                Ok(Async::NotReady) => (),
                Ok(Async::Ready(addrs_iter)) => {
                    let cache_entry = CacheEntry {
                        addrs: addrs_iter.collect(),
                        timestamp: Some(Instant::now()),
                    };
                    self.cache.insert(name.clone(), cache_entry);
                    finished_resolutions.push(name.clone());
                }
                Err(error) => {
                    log::error!("Unable to resolve \"{}\": {}", name, error);
                    finished_resolutions.push(name.clone());
                }
            }
        }
        for name in finished_resolutions {
            let (_resolve_future, listeners) = self.ongoing_resolutions.remove(&name).unwrap();
            let addrs = self
                .cache
                .get(name.as_str())
                .map(|entry| &entry.addrs)
                .cloned()
                .unwrap_or_default();
            log::debug!("Replying for \"{}\" with {:?}", name, addrs);
            for listener in listeners {
                let _ = listener.send(addrs.clone().into_iter());
            }
        }
        if self.ongoing_resolutions.is_empty() {
            Async::Ready(())
        } else {
            Async::NotReady
        }
    }
}

impl<R: Resolve> Future for CachedResolver<R> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let handle_rx = self.poll_handles();
        let ongoing_resolutions = self.poll_ongoing_resolutions();

        if handle_rx == Async::Ready(()) && ongoing_resolutions == Async::Ready(()) {
            log::debug!("All handles closed, shutting down CachedResolver");
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}

/// A handle to a [`CachedResolver`]. This type implements the hyper `Resolve` trait and can be used
/// as the resolver on a hyper `HttpConnector`.
/// Cloning this returns a new handle backed by the same caching [`CachedResolver`].
#[derive(Clone)]
pub struct ResolverHandle {
    resolver: mpsc::Sender<(Name, oneshot::Sender<IntoIter<IpAddr>>)>,
}

impl Resolve for ResolverHandle {
    type Addrs = IntoIter<IpAddr>;
    type Future = Box<dyn Future<Item = Self::Addrs, Error = io::Error> + Send + 'static>;

    fn resolve(&self, name: Name) -> Self::Future {
        log::debug!("ResolverHandle resolving {}", name.as_str());
        let (response_tx, response_rx) = oneshot::channel();
        let f = self
            .resolver
            .clone()
            .send((name, response_tx))
            .map_err(|e| {
                log::error!("Unable to send request to CachedResolver: {:?}", e);
                io::Error::new(io::ErrorKind::Other, "CachedResolver has been canceled")
            })
            .and_then(|_self| {
                response_rx.map_err(|e| {
                    log::error!("No response from CachedResolver: {:?}", e);
                    io::Error::new(io::ErrorKind::Other, "CachedResolver has been canceled")
                })
            });
        Box::new(f)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;
    use std::{
        mem,
        net::{Ipv4Addr, Ipv6Addr},
        sync::mpsc,
        thread,
        time::Duration,
    };
    use tokio::prelude::FutureExt;

    struct MockResolver {
        cache: HashMap<String, Vec<IpAddr>>,
        requests: mpsc::Sender<String>,
    }

    impl MockResolver {
        pub fn new(cache: HashMap<String, Vec<IpAddr>>) -> (Self, mpsc::Receiver<String>) {
            let (tx, rx) = mpsc::channel();
            let resolver = Self {
                cache,
                requests: tx,
            };
            (resolver, rx)
        }
    }

    impl Resolve for MockResolver {
        type Addrs = IntoIter<IpAddr>;
        type Future = future::FutureResult<Self::Addrs, io::Error>;
        fn resolve(&self, name: Name) -> Self::Future {
            log::debug!("Mock resolving {}", name.as_str());
            let _ = self.requests.send(name.as_str().to_owned());
            if let Some(addrs) = self.cache.get(name.as_str()) {
                future::ok(addrs.clone().into_iter())
            } else {
                future::err(io::Error::new(io::ErrorKind::Other, "fail"))
            }
        }
    }

    // A test resolver that never replies.
    struct SlowMockResolver;

    impl Resolve for SlowMockResolver {
        type Addrs = IntoIter<IpAddr>;
        type Future = future::Empty<Self::Addrs, io::Error>;
        fn resolve(&self, name: Name) -> Self::Future {
            log::debug!("Mock resolving {} (will never reply)", name.as_str());
            future::empty()
        }
    }

    #[test]
    fn no_cache_failing_resolver() {
        let (resolver, _) = MockResolver::new(HashMap::new());
        let (cached_resolver, handle) = CachedResolver::builder(resolver).build();

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.spawn(cached_resolver);
        let result = runtime
            .block_on(handle.resolve(name("example.com")))
            .unwrap();
        let expected: &[IpAddr] = &[];
        assert_eq!(result.as_slice(), expected);
    }

    #[test]
    fn with_cache_failing_resolver() {
        let mut cache = HashMap::new();
        cache.insert("example.com".to_string(), vec![Ipv4Addr::LOCALHOST.into()]);
        cache.insert(
            "test1.example.com".to_string(),
            vec![Ipv6Addr::LOCALHOST.into()],
        );
        cache.insert(
            "website.com".to_string(),
            vec![Ipv6Addr::UNSPECIFIED.into(), Ipv4Addr::UNSPECIFIED.into()],
        );
        let (resolver, _) = MockResolver::new(HashMap::new());

        let (cached_resolver, handle) = CachedResolver::builder(resolver).cache(cache).build();

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.spawn(cached_resolver);

        let result = runtime
            .block_on(handle.resolve(name("example.com")))
            .unwrap();
        let expected: &[IpAddr] = &[Ipv4Addr::LOCALHOST.into()];
        assert_eq!(result.as_slice(), expected);

        let result = runtime
            .block_on(handle.resolve(name("test1.example.com")))
            .unwrap();
        let expected: &[IpAddr] = &[Ipv6Addr::LOCALHOST.into()];
        assert_eq!(result.as_slice(), expected);

        let result = runtime
            .block_on(handle.resolve(name("website.com")))
            .unwrap();
        let expected: &[IpAddr] = &[Ipv6Addr::UNSPECIFIED.into(), Ipv4Addr::UNSPECIFIED.into()];
        assert_eq!(result.as_slice(), expected);

        let result = runtime
            .block_on(handle.resolve(name("not-in-cache.com")))
            .unwrap();
        let expected: &[IpAddr] = &[];
        assert_eq!(result.as_slice(), expected);
    }

    #[test]
    fn no_cache_working_resolver() {
        let mut domains = HashMap::new();
        domains.insert(
            "example.com".to_string(),
            vec![Ipv4Addr::new(10, 9, 8, 7).into()],
        );
        let (resolver, _) = MockResolver::new(domains);

        let (cached_resolver, handle) = CachedResolver::builder(resolver).build();

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.spawn(cached_resolver);

        let result = runtime
            .block_on(handle.resolve(name("example.com")))
            .unwrap();
        let expected: &[IpAddr] = &[Ipv4Addr::new(10, 9, 8, 7).into()];
        assert_eq!(result.as_slice(), expected);
    }

    #[test]
    fn prefer_cache_over_resolver() {
        let mut resolver_domains = HashMap::new();
        resolver_domains.insert(
            "cached.net".to_string(),
            vec![Ipv4Addr::new(10, 9, 8, 7).into()],
        );
        let (resolver, _) = MockResolver::new(resolver_domains);
        let mut cache = HashMap::new();
        cache.insert("cached.net".to_string(), vec![Ipv6Addr::LOCALHOST.into()]);

        let (cached_resolver, handle) = CachedResolver::builder(resolver).cache(cache).build();

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.spawn(cached_resolver);

        let result = runtime
            .block_on(handle.resolve(name("cached.net")))
            .unwrap();
        let expected: &[IpAddr] = &[Ipv6Addr::LOCALHOST.into()];
        assert_eq!(result.as_slice(), expected);
    }

    #[test]
    fn timeout_slow_resolver() {
        let (cached_resolver, handle) = CachedResolver::builder(SlowMockResolver)
            .timeout(Duration::from_millis(1000))
            .build();

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.spawn(cached_resolver);

        let time_limited_future = handle
            .resolve(name("some.domain.org"))
            .timeout(Duration::from_millis(100));

        runtime.block_on(time_limited_future).unwrap_err();
    }

    #[test]
    fn slow_resolver_uses_cache_or_empty_result() {
        let mut cache = HashMap::new();
        cache.insert(
            "a.cached.domain.it".to_string(),
            vec![Ipv4Addr::new(7, 6, 5, 4).into()],
        );

        let (cached_resolver, handle) = CachedResolver::builder(SlowMockResolver)
            .timeout(Duration::from_millis(100))
            .cache(cache)
            .build();

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.spawn(cached_resolver);

        let time_limited_future = handle
            .resolve(name("a.cached.domain.it"))
            .timeout(Duration::from_millis(1000));

        let result = runtime.block_on(time_limited_future).unwrap();
        let expected: &[IpAddr] = &[Ipv4Addr::new(7, 6, 5, 4).into()];
        assert_eq!(result.as_slice(), expected);

        let time_limited_future = handle
            .resolve(name("some.not.cached.domain.org"))
            .timeout(Duration::from_millis(1000));

        let result = runtime.block_on(time_limited_future).unwrap();
        let expected: &[IpAddr] = &[];
        assert_eq!(result.as_slice(), expected);
    }

    #[test]
    fn cache_expiry_causes_resolve() {
        let mut cache = HashMap::new();
        cache.insert(
            "a.cached.domain.it".to_string(),
            vec![Ipv4Addr::new(7, 6, 5, 4).into()],
        );
        let mut resolver_domains = HashMap::new();
        resolver_domains.insert(
            "a.cached.domain.it".to_string(),
            vec![Ipv4Addr::new(100, 1, 2, 3).into()],
        );
        let (resolver, mock_rx) = MockResolver::new(resolver_domains);

        let (cached_resolver, handle) = CachedResolver::builder(resolver)
            .cache(cache)
            .cache_expiry(Duration::from_millis(100))
            .build();

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.spawn(cached_resolver);

        // Cache should always be treated as expired from the start
        let result = runtime
            .block_on(handle.resolve(name("a.cached.domain.it")))
            .unwrap();
        let expected: &[IpAddr] = &[Ipv4Addr::new(100, 1, 2, 3).into()];
        assert_eq!(result.as_slice(), expected);
        assert_eq!(mock_rx.try_recv(), Ok("a.cached.domain.it".to_string()));
        assert!(mock_rx.try_recv().is_err());

        // Now the cache should be valid, and not cause a resolve
        let result = runtime
            .block_on(handle.resolve(name("a.cached.domain.it")))
            .unwrap();
        assert_eq!(result.as_slice(), expected);
        assert!(mock_rx.try_recv().is_err());

        thread::sleep(Duration::from_secs(1));

        // Now the cache should have expired again, triggering a resolve
        let result = runtime
            .block_on(handle.resolve(name("a.cached.domain.it")))
            .unwrap();
        assert_eq!(result.as_slice(), expected);
        assert_eq!(mock_rx.try_recv(), Ok("a.cached.domain.it".to_string()));
        assert!(mock_rx.try_recv().is_err());
    }

    /// Hack for converting a string into a hyper compatible `Name`
    fn name(name: &str) -> Name {
        let name = name.to_string();
        unsafe { mem::transmute(name) }
    }
}
