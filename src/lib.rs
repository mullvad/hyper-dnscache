use futures::{
    stream::Fuse,
    sync::{mpsc, oneshot},
    Async, Future, Poll, Sink, Stream,
};
use hyper::client::connect::dns::{Name, Resolve};
use log::debug;
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
// * Limit the size of the in-memory cache. Use an LRU cache or similar.


pub mod cache_storer;
pub use cache_storer::CacheStorer;

mod timeout;
use self::timeout::OptionalTimeout;

struct CacheEntry {
    /// The IPs for this domain.
    addrs: Vec<IpAddr>,
    /// The time when these addresses were last updated.
    /// `None` for entries that were never resolved, but remain at their initial values.
    timestamp: Option<Instant>,
}

/// Builder for [`CachedResolver`].
pub struct CachedResolverBuilder<R: Resolve, C: CacheStorer = cache_storer::JsonStorer> {
    resolver: R,
    resolver_timeout: Option<Duration>,
    cache: Option<HashMap<Name, CacheEntry>>,
    cache_expiry: Option<Duration>,
    cache_storer: Option<C>,
}

impl<R: Resolve> CachedResolverBuilder<R, cache_storer::JsonStorer> {
    /// Sets a file path to load cache from/store cache to. If this is set then the file will be
    /// read and parsed in [`build`]. Any entries from the file that overlaps with entries given
    /// to [`cache`] will replace those for the in-memory cache of the resulting
    /// [`CachedResolver`].
    ///
    /// At any point where the in-memory cache is updated thanks to a resolution with the underlying
    /// resolver, the in-memory cache will be serialized and written to the file at the path given
    /// here.
    ///
    /// This method is just a shorthand for calling [`cache_storer`] with
    /// `JsonStorer::new(cache_file)`
    ///
    /// [`build`]: #method.build
    /// [`cache`]: #method.cache
    /// [`cache_storer`]: #method.cache_storer
    pub fn cache_file(mut self, cache_file: impl Into<PathBuf>) -> Self {
        self.cache_storer = Some(cache_storer::JsonStorer::new(cache_file));
        self
    }
}

impl<R: Resolve, C: CacheStorer> CachedResolverBuilder<R, C> {
    /// Returns a new builder that will build a [`CachedResolver`] using `resolver` as the
    /// underlying DNS resolver.
    pub fn new(resolver: R, cache_storer: Option<C>) -> Self {
        Self {
            resolver,
            resolver_timeout: None,
            cache: None,
            cache_expiry: None,
            cache_storer,
        }
    }

    /// Sets a timeout that will be used to time out requests to the underlying resolver.
    /// By default there is no timeout. So resolutions using the underlying resolver will continue
    /// until they complete or return an error.
    pub fn timeout(mut self, resolver_timeout: Duration) -> Self {
        self.resolver_timeout = Some(resolver_timeout);
        self
    }

    /// Sets an initial fallback cache. This cache will bootstrap the cached resolver.
    pub fn cache(mut self, cache: HashMap<Name, Vec<IpAddr>>) -> Self {
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

    /// Changes the [`CacheStorer`] implementation instance for this builder. Allows customizing
    /// how the cache is serialized and persisted.
    pub fn cache_storer<C2: CacheStorer>(self, cache_storer: C2) -> CachedResolverBuilder<R, C2> {
        CachedResolverBuilder {
            resolver: self.resolver,
            resolver_timeout: self.resolver_timeout,
            cache: self.cache,
            cache_expiry: self.cache_expiry,
            cache_storer: Some(cache_storer),
        }
    }

    /// Sets how old the cache for a domain has to be before a lookup of that domain triggers a
    /// resolution with the underlying resolver, `R`, in order to update the cache.
    /// Not setting this means looking up a name already in the cache will always just return the
    /// cached value directly.
    ///
    /// Note that if setting this at all, all entries given to [`cache`] are considered
    /// immediately expired and will trigger a resolution on first access.
    ///
    /// Even expired cache entries are returned from this resolver if doing a resolution using
    /// the underlying resolver fails. So "expired" in this context does not mean discarded.
    /// It just means that trying to access it attempts to update it, in contrast to just
    /// returning the cached value immediately.
    ///
    /// [`cache`]: #method.cache
    pub fn cache_expiry(mut self, expiry: Duration) -> Self {
        self.cache_expiry = Some(expiry);
        self
    }

    /// Constructs the [`CachedResolver`] and the corresponding [`ResolverHandle`].
    ///
    /// Returns an error if [`cache_storer`] or [`cache_file`] is set and the active cacher, `C`,
    /// fails to load the cache.
    ///
    /// [`cache_file`]: #method.cache_file
    /// [`cache_storer`]: #method.cache_storer
    pub fn build(mut self) -> Result<(CachedResolver<R, C>, ResolverHandle), C::Error> {
        // Start out with the provided in-memory cache, or an empty one.
        let mut cache = self.cache.unwrap_or_default();
        if let Some(cache_storer) = &mut self.cache_storer {
            let loaded_cache = cache_storer.load()?;
            for (name, addrs) in loaded_cache {
                cache.insert(
                    name,
                    CacheEntry {
                        addrs,
                        timestamp: None,
                    },
                );
            }
        }
        debug!(
            "Building CachedResolver with {} entries in the cache.",
            cache.len()
        );

        let (handles_tx, handles_rx) = mpsc::channel(0);
        let cached_resolver = CachedResolver {
            resolver: self.resolver,
            resolver_timeout: self.resolver_timeout,
            cache,
            cache_expiry: self.cache_expiry,
            cache_storer: self.cache_storer,
            handles_rx: handles_rx.fuse(),
            ongoing_resolutions: HashMap::new(),
        };
        let handle = ResolverHandle {
            resolver: handles_tx,
        };
        Ok((cached_resolver, handle))
    }
}


pub struct CachedResolver<R: Resolve, C: CacheStorer = cache_storer::JsonStorer> {
    resolver: R,
    resolver_timeout: Option<Duration>,
    cache: HashMap<Name, CacheEntry>,
    cache_expiry: Option<Duration>,
    cache_storer: Option<C>,
    handles_rx: Fuse<mpsc::Receiver<(Name, oneshot::Sender<Result<IntoIter<IpAddr>, io::Error>>)>>,
    ongoing_resolutions: HashMap<
        Name,
        (
            OptionalTimeout<R::Future>,
            Vec<oneshot::Sender<Result<IntoIter<IpAddr>, io::Error>>>,
        ),
    >,
}

impl<R: Resolve> CachedResolver<R, cache_storer::JsonStorer> {
    pub fn builder(resolver: R) -> CachedResolverBuilder<R, cache_storer::JsonStorer> {
        CachedResolverBuilder::new(resolver, None)
    }
}

impl<R: Resolve, C: CacheStorer> CachedResolver<R, C> {
    fn poll_handles(&mut self) -> Async<()> {
        // Process requests from all handles
        loop {
            match self.handles_rx.poll() {
                // Nothing new from the handles
                Ok(Async::NotReady) => {
                    return Async::NotReady;
                }
                // All handles are gone, no more requests will come in.
                Ok(Async::Ready(None)) => {
                    return Async::Ready(());
                }
                // Incoming resolve request from a handle
                Ok(Async::Ready(Some((name, listener)))) => {
                    if let Some(addrs) = self.get_cache_entry(&name) {
                        log::debug!("Replying from cache for \"{}\" with {:?}", name, addrs);
                        let _ = listener.send(Ok(addrs));
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
    }

    /// Returns the addresses for a name if it exists in the cache and the cache entry is not too
    /// old.
    fn get_cache_entry(&self, name: &Name) -> Option<IntoIter<IpAddr>> {
        let cache_entry = self.cache.get(name)?;
        let cache_is_valid = match (self.cache_expiry, cache_entry.timestamp) {
            // Our cache does not have an expiry, always valid.
            (None, _) => true,
            // Cache can expire, current entry has no timestamp, outdated.
            (Some(_), None) => false,
            // Cache can expire, current entry has timestamp, compare.
            (Some(cache_expiry), Some(cache_timestamp)) => cache_timestamp.elapsed() < cache_expiry,
        };
        if cache_is_valid {
            Some(cache_entry.addrs.clone().into_iter())
        } else {
            None
        }
    }

    fn resolve(
        &mut self,
        name: Name,
        listener: oneshot::Sender<Result<IntoIter<IpAddr>, io::Error>>,
    ) {
        if let Some((_fut, listeners)) = self.ongoing_resolutions.get_mut(&name) {
            log::trace!("Adding listener for existing resolutions of \"{}\"", name);
            listeners.push(listener);
        } else {
            log::debug!("Resolving \"{}\"", name);
            let resolve_future =
                OptionalTimeout::new(self.resolver.resolve(name.clone()), self.resolver_timeout);
            let listeners = vec![listener];
            self.ongoing_resolutions
                .insert(name, (resolve_future, listeners));
        }
    }

    fn poll_ongoing_resolutions(&mut self) -> Async<()> {
        // Process all ongoing DNS resolutions
        let mut finished_resolutions = HashMap::new();
        let now = Instant::now();
        for (name, (resolve_future, _listeners)) in &mut self.ongoing_resolutions {
            match resolve_future.poll() {
                Ok(Async::NotReady) => (),
                Ok(Async::Ready(addrs_iter)) => {
                    let addrs: Vec<IpAddr> = addrs_iter.collect();
                    finished_resolutions.insert(name.clone(), Ok(addrs.clone().into_iter()));
                    let cache_entry = CacheEntry {
                        addrs,
                        timestamp: Some(now),
                    };
                    self.cache.insert(name.clone(), cache_entry);
                }
                Err(timer_error) => {
                    log::error!("Unable to resolve \"{}\": {}", name, timer_error);
                    let resolve_error = ResolveError::from(timer_error);
                    finished_resolutions.insert(name.clone(), Err(resolve_error));
                }
            }
        }
        if !finished_resolutions.is_empty() {
            self.persist_cache();
        }
        for (name, result) in finished_resolutions {
            let (_resolve_future, listeners) = self.ongoing_resolutions.remove(&name).unwrap();
            log::debug!("Replying for \"{}\" with {:?}", name, result);
            for listener in listeners {
                let resolve_result = match &result {
                    Ok(addrs) => Ok(addrs.clone()),
                    Err(resolve_error) => Err(resolve_error.to_io_error()),
                };
                let _ = listener.send(resolve_result);
            }
        }
        if self.ongoing_resolutions.is_empty() {
            Async::Ready(())
        } else {
            Async::NotReady
        }
    }

    fn persist_cache(&mut self) {
        if let Some(cache_storer) = &mut self.cache_storer {
            let mut cache = HashMap::with_capacity(self.cache.len());
            for (name, cache_entry) in &self.cache {
                cache.insert(name.clone(), cache_entry.addrs.clone());
            }
            if let Err(e) = cache_storer.store(cache) {
                log_error("Failed to persist cache", &e);
            }
        }
    }
}

fn log_error(msg: &str, error: &impl std::error::Error) {
    let mut buffer = format!("Error: {}", msg);
    let mut source: Option<&dyn std::error::Error> = Some(error);
    while let Some(error) = source {
        buffer.push_str("\nCaused by: ");
        buffer.push_str(&error.to_string());
        source = error.source();
    }
    log::error!("{}", buffer);
}

impl<R: Resolve, C: CacheStorer> Future for CachedResolver<R, C> {
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
///
/// These handles will only work as long as their backing [`CachedResolver`] is running.
#[derive(Clone)]
pub struct ResolverHandle {
    resolver: mpsc::Sender<(Name, oneshot::Sender<Result<IntoIter<IpAddr>, io::Error>>)>,
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
            .and_then(|_sender| {
                response_rx
                    .map_err(|e| {
                        log::error!("No response from CachedResolver: {:?}", e);
                        io::Error::new(io::ErrorKind::Other, "CachedResolver has been canceled")
                    })
                    .flatten()
            });
        Box::new(f)
    }
}

#[derive(Debug)]
enum ResolveError {
    Inner(String),
    Timeout,
    Timer(String),
}

impl ResolveError {
    pub fn to_io_error(&self) -> io::Error {
        use ResolveError::*;
        match self {
            Inner(msg) => io::Error::new(io::ErrorKind::Other, msg.as_str()),
            Timeout => io::Error::new(io::ErrorKind::TimedOut, "The lookup timed out"),
            Timer(msg) => io::Error::new(io::ErrorKind::Other, format!("Error in timer: {}", msg)),
        }
    }
}

impl From<timeout::Error<io::Error>> for ResolveError {
    fn from(error: timeout::Error<io::Error>) -> Self {
        if error.is_inner() {
            ResolveError::Inner(error.into_inner().unwrap().to_string())
        } else if error.is_elapsed() {
            ResolveError::Timeout
        } else if error.is_timer() {
            ResolveError::Timer(error.into_timer().unwrap().to_string())
        } else {
            unreachable!("Timer error not one of the expected types");
        }
    }
}
