use futures::future;
use hyper::client::connect::dns::{GaiResolver, Name, Resolve};
use hyper_dnscache::*;
use std::{
    collections::HashMap,
    env, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::FromStr,
    sync::mpsc,
    thread,
    time::Duration,
    vec::IntoIter,
};
use tokio::prelude::FutureExt;


fn main() {
    env_logger::init();
    let mut args = env::args().skip(1);
    let name_str = args.next().expect("Give domain name as first argument");
    let name = Name::from_str(&name_str).expect("Given domain has an invalid format");

    let cache_file = args.next();

    let resolver = GaiResolver::new(1);
    let mut cached_resolver_builder = CachedResolver::builder(resolver);
    if let Some(cache_file) = cache_file {
        cached_resolver_builder = cached_resolver_builder.cache_file(cache_file);
    }
    let (cached_resolver, handle) = unwrap_log(cached_resolver_builder.build());

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.spawn(cached_resolver);

    let result = runtime.block_on(handle.resolve(name));
    match result {
        Ok(addrs) => {
            for addr in addrs {
                println!("{}", addr);
            }
        }
        Err(e) => {
            eprintln!("Unable to resolve domain: {}", e);
            std::process::exit(1);
        }
    }
}

fn unwrap_log<T, E: std::error::Error>(result: Result<T, E>) -> T {
    match result {
        Ok(t) => t,
        Err(e) => {
            log_error(&e);
            std::process::exit(1);
        }
    }
}

fn log_error(error: &impl std::error::Error) {
    let mut buffer = format!("Error: {}", error);
    let mut source: Option<&dyn std::error::Error> = error.source();
    while let Some(error) = source {
        buffer.push_str("\nCaused by: ");
        buffer.push_str(&error.to_string());
        source = error.source();
    }
    log::error!("{}", buffer);
}
