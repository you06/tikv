// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rand::Rng;
use url::Url;

mod local;
mod s3;

pub use local::LocalStorage;
pub use s3::S3Storage;

/// Create a new storage from the given url.
pub fn create_storage(url: &str) -> io::Result<Arc<dyn Storage>> {
    let url = Url::parse(url).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("failed to create storage {} {}", e, url),
        )
    })?;

    match url.scheme() {
        LocalStorage::SCHEME => {
            let p = Path::new(url.path());
            LocalStorage::new(p).map(|s| Arc::new(s) as _)
        }
        S3Storage::SCHEME => S3Storage::new(url).map(|s| Arc::new(s) as _),
        _ => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("unknown storage {}", url),
        )),
    }
}

/// An abstraction of an external storage.
pub trait Storage: Sync + Send + 'static {
    /// Write all contents of the read to the given path.
    // TODO: should it return a writer?
    fn write(&self, name: &str, reader: &mut dyn Read) -> io::Result<()>;
    /// Read all contents of the given path.
    fn read(&self, name: &str) -> io::Result<Box<dyn Read>>;
}

impl Storage for Arc<dyn Storage> {
    fn write(&self, name: &str, reader: &mut dyn Read) -> io::Result<()> {
        (**self).write(name, reader)
    }
    fn read(&self, name: &str) -> io::Result<Box<dyn Read>> {
        (**self).read(name)
    }
}
