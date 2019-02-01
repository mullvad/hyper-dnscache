use futures::{future, Future};
use hyper::client::connect::dns::{Name, Resolve};
use hyper_dnscache::*;
use std::{
    collections::HashMap,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::FromStr,
    sync::mpsc,
    thread,
    time::Duration,
    vec::IntoIter,
};
use tokio::prelude::FutureExt;


struct MockResolver {
    cache: HashMap<Name, Vec<IpAddr>>,
    requests: mpsc::Sender<Name>,
}

impl MockResolver {
    pub fn new(cache: HashMap<Name, Vec<IpAddr>>) -> (Self, mpsc::Receiver<Name>) {
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
        log::debug!("Mock resolving {}", name);
        let _ = self.requests.send(name.clone());
        if let Some(addrs) = self.cache.get(&name) {
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

struct MockStorer {
    cache: HashMap<Name, Vec<IpAddr>>,
    stores: mpsc::Sender<HashMap<Name, Vec<IpAddr>>>,
}

impl MockStorer {
    pub fn new(
        cache: HashMap<Name, Vec<IpAddr>>,
    ) -> (Self, mpsc::Receiver<HashMap<Name, Vec<IpAddr>>>) {
        let (tx, rx) = mpsc::channel();
        let cacher = Self { cache, stores: tx };
        (cacher, rx)
    }
}

impl CacheStorer for MockStorer {
    type Error = MockStoreError;
    type StoreFuture = Box<dyn Future<Item = (), Error = Self::Error> + Send>;

    fn load(&mut self) -> Result<HashMap<Name, Vec<IpAddr>>, Self::Error> {
        Ok(dbg!(self.cache.clone()))
    }

    fn store(&mut self, cache: HashMap<Name, Vec<IpAddr>>) -> Self::StoreFuture {
        let _ = self.stores.send(cache.clone());
        self.cache = dbg!(cache);
        Box::new(future::ok(()))
    }
}

#[derive(Debug, err_derive::Error)]
#[error(display = "Mock store failed")]
struct MockStoreError;


#[test]
fn no_cache_failing_resolver() {
    let (resolver, _) = MockResolver::new(HashMap::new());
    let (cached_resolver, handle) = CachedResolver::builder(resolver).build().unwrap();

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.spawn(cached_resolver);
    let result = runtime.block_on(handle.resolve(name("example.com")));
    assert!(result.is_err());
}

#[test]
fn with_cache_failing_resolver() {
    let cache = test_cache(&[
        ("example.com", &[Ipv4Addr::LOCALHOST.into()]),
        ("test1.example.com", &[Ipv6Addr::LOCALHOST.into()]),
        (
            "website.com",
            &[Ipv6Addr::UNSPECIFIED.into(), Ipv4Addr::UNSPECIFIED.into()],
        ),
    ]);

    let (resolver, _) = MockResolver::new(HashMap::new());

    let (cached_resolver, handle) = CachedResolver::builder(resolver)
        .cache(cache)
        .build()
        .unwrap();

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

    let result = runtime.block_on(handle.resolve(name("not-in-cache.com")));
    assert!(result.is_err());
}

#[test]
fn no_cache_working_resolver() {
    let domains = test_cache(&[("example.com", &[Ipv4Addr::new(10, 9, 8, 7).into()])]);

    let (resolver, _) = MockResolver::new(domains);

    let (cached_resolver, handle) = CachedResolver::builder(resolver).build().unwrap();

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
    let resolver_domains = test_cache(&[("cached.net", &[Ipv4Addr::new(10, 9, 8, 7).into()])]);
    let cache = test_cache(&[("cached.net", &[Ipv6Addr::LOCALHOST.into()])]);

    let (resolver, _) = MockResolver::new(resolver_domains);
    let (cached_resolver, handle) = CachedResolver::builder(resolver)
        .cache(cache)
        .build()
        .unwrap();

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
        .build()
        .unwrap();

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.spawn(cached_resolver);

    let time_limited_future = handle
        .resolve(name("some.domain.org"))
        .timeout(Duration::from_millis(100));

    runtime.block_on(time_limited_future).unwrap_err();
}

#[test]
fn slow_resolver_uses_cache_or_empty_result() {
    let cache = test_cache(&[("a.cached.domain.it", &[Ipv4Addr::new(7, 6, 5, 4).into()])]);

    let (cached_resolver, handle) = CachedResolver::builder(SlowMockResolver)
        .timeout(Duration::from_millis(100))
        .cache(cache)
        .build()
        .unwrap();

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

    let result = runtime.block_on(time_limited_future);
    assert!(result.is_err());
}

#[test]
fn cache_expiry_causes_resolve() {
    let cache = test_cache(&[("a.cached.domain.it", &[Ipv4Addr::new(7, 6, 5, 4).into()])]);
    let resolver_domains =
        test_cache(&[("a.cached.domain.it", &[Ipv4Addr::new(100, 1, 2, 3).into()])]);

    let (resolver, mock_rx) = MockResolver::new(resolver_domains);

    let (cached_resolver, handle) = CachedResolver::builder(resolver)
        .cache(cache)
        .cache_expiry(Duration::from_millis(100))
        .build()
        .unwrap();

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.spawn(cached_resolver);

    // Cache should always be treated as expired from the start
    let result = runtime
        .block_on(handle.resolve(name("a.cached.domain.it")))
        .unwrap();
    let expected: &[IpAddr] = &[Ipv4Addr::new(100, 1, 2, 3).into()];
    assert_eq!(result.as_slice(), expected);
    assert_eq!(mock_rx.try_recv(), Ok(name("a.cached.domain.it")));
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
    assert_eq!(mock_rx.try_recv(), Ok(name("a.cached.domain.it")));
    assert!(mock_rx.try_recv().is_err());
}

#[test]
fn loads_disk_cache() {
    let (resolver, _mock_rx) = MockResolver::new(HashMap::new());
    let file_cache = test_cache(&[("a.cached.domain.it", &[Ipv4Addr::new(7, 6, 5, 4).into()])]);
    let (cache_storer, _) = MockStorer::new(file_cache);

    let (cached_resolver, handle) = CachedResolver::builder(resolver)
        .cache_storer(cache_storer)
        .build()
        .unwrap();

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.spawn(cached_resolver);

    let result = runtime
        .block_on(handle.resolve(name("a.cached.domain.it")))
        .unwrap();
    let expected: &[IpAddr] = &[Ipv4Addr::new(7, 6, 5, 4).into()];
    assert_eq!(result.as_slice(), expected);
}

#[test]
fn stores_disk_cache() {
    let resolver_domains = test_cache(&[("example.com", &[Ipv4Addr::new(2, 3, 4, 5).into()])]);
    let (resolver, _mock_rx) = MockResolver::new(resolver_domains);
    let (cache_storer, stores_rx) = MockStorer::new(HashMap::new());

    let (cached_resolver, handle) = CachedResolver::builder(resolver)
        .cache_storer(cache_storer)
        .build()
        .unwrap();

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.spawn(cached_resolver);

    let result = runtime
        .block_on(handle.resolve(name("example.com")))
        .unwrap();
    let expected: &[IpAddr] = &[Ipv4Addr::new(2, 3, 4, 5).into()];
    assert_eq!(result.as_slice(), expected);
    let store_cache = stores_rx.try_recv().expect("Store was never called");
    assert_eq!(store_cache.len(), 1);
    assert_eq!(
        store_cache.get(&name("example.com")).map(Vec::as_slice),
        Some(expected)
    );
}


fn name(name: &str) -> Name {
    Name::from_str(name).unwrap()
}

fn test_cache(entries: &[(&str, &[IpAddr])]) -> HashMap<Name, Vec<IpAddr>> {
    let mut cache = HashMap::new();
    for entry in entries {
        cache.insert(Name::from_str(entry.0).unwrap(), entry.1.to_vec());
    }
    cache
}
