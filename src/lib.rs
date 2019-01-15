use futures::{
    stream::Fuse,
    sync::{mpsc, oneshot},
    Async, Future, Poll, Sink, Stream,
};
use hyper::client::connect::dns::{Name, Resolve};
use std::{collections::HashMap, io, net::IpAddr, path::PathBuf, vec};

// TODO:
// * Read cache on creation
// * Optional timeout support
// * Writing cache to disk on each change
// * Optional filtering of results from underlying resolver
// * Cache max age

pub type CachedAddrs = vec::IntoIter<IpAddr>;

type Cache = HashMap<String, Vec<IpAddr>>;

pub struct CachedResolverBuilder<R: Resolve> {
    resolver: R,
    cache: Option<Cache>,
    cache_file: Option<PathBuf>,
}

impl<R: Resolve> CachedResolverBuilder<R> {
    pub fn new(resolver: R) -> Self {
        Self {
            resolver,
            cache: None,
            cache_file: None,
        }
    }

    pub fn cache(mut self, cache: Cache) -> Self {
        self.cache = Some(cache);
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
            cache: self.cache.unwrap_or_default(),
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
    cache: Cache,
    _cache_file: Option<PathBuf>,
    handles_rx: Fuse<mpsc::Receiver<(Name, oneshot::Sender<CachedAddrs>)>>,
    ongoing_resolutions: HashMap<String, (R::Future, Vec<oneshot::Sender<CachedAddrs>>)>,
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
                    if let Some(addrs) = self.cache.get(name.as_str()) {
                        log::debug!(
                            "Replying from cache for \"{}\" with {:?}",
                            name.as_str(),
                            addrs
                        );
                        let _ = listener.send(addrs.clone().into_iter());
                    } else if let Some((_fut, listeners)) =
                        self.ongoing_resolutions.get_mut(name.as_str())
                    {
                        log::trace!(
                            "Adding listener for existing resolution of \"{}\"",
                            name.as_str()
                        );
                        listeners.push(listener);
                    } else {
                        let name_str = name.as_str().to_string();
                        log::debug!("Resolving \"{}\"", name_str);
                        let resolve_future = self.resolver.resolve(name);
                        let listeners = vec![listener];
                        self.ongoing_resolutions
                            .insert(name_str, (resolve_future, listeners));
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

    fn poll_ongoing_resolutions(&mut self) -> Async<()> {
        // Process all ongoing DNS resolutions
        let mut finished_resolutions = Vec::new();
        for (name, (resolve_future, _listeners)) in &mut self.ongoing_resolutions {
            match resolve_future.poll() {
                Ok(Async::NotReady) => (),
                Ok(Async::Ready(addrs_iter)) => {
                    self.cache.insert(name.clone(), addrs_iter.collect());
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
            let addrs = self.cache.get(name.as_str()).cloned().unwrap_or_default();
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

/// A handle to a `CachedResolver`. This type implements the hyper `Resolve` trait and can be used
/// as the resolver on a hyper `HttpConnector`.
/// Cloning this returns a new handle backed by the same caching `CachedResolver`.
#[derive(Clone)]
pub struct ResolverHandle {
    resolver: mpsc::Sender<(Name, oneshot::Sender<CachedAddrs>)>,
}

impl Resolve for ResolverHandle {
    type Addrs = CachedAddrs;
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
    };

    struct MockResolver(HashMap<String, Vec<IpAddr>>);

    impl Resolve for MockResolver {
        type Addrs = CachedAddrs;
        type Future = future::FutureResult<Self::Addrs, io::Error>;
        fn resolve(&self, name: Name) -> Self::Future {
            log::debug!("Mock resolving {}", name.as_str());
            if let Some(addrs) = self.0.get(name.as_str()) {
                future::ok(addrs.clone().into_iter())
            } else {
                future::err(io::Error::new(io::ErrorKind::Other, "fail"))
            }
        }
    }

    #[test]
    fn no_cache_failing_resolver() {
        let resolver = MockResolver(HashMap::new());
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
        let resolver = MockResolver(HashMap::new());

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
        let resolver = MockResolver(domains);

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
        let resolver = MockResolver(resolver_domains);
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

    fn name(name: &str) -> Name {
        let name = name.to_string();
        unsafe { mem::transmute(name) }
    }
}
