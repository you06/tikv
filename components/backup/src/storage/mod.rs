// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rand::Rng;
use url::Url;

const LOCAL_STORAGE_TMP_DIR: &str = "localtmp";
const LOCAL_STORAGE_TMP_FILE_SUFFIX: &str = "tmp";

/// Create a new storage from the given url.
pub fn create_storage(url: &str) -> io::Result<Arc<dyn Storage>> {
    LocalStorage::from_url(url).map(|s| Arc::new(s) as _)
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

fn maybe_create_dir(path: &Path) -> io::Result<()> {
    if let Err(e) = fs::create_dir_all(path) {
        if e.kind() != io::ErrorKind::AlreadyExists {
            return Err(e);
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct LocalStorage {
    base: PathBuf,
    tmp: PathBuf,
}

impl LocalStorage {
    const SCHEME: &'static str = "local";

    pub fn new(base: &Path) -> io::Result<LocalStorage> {
        info!("create local storage"; "base" => base.display());
        let tmp = base.join(LOCAL_STORAGE_TMP_DIR);
        maybe_create_dir(&tmp)?;
        Ok(LocalStorage {
            base: base.to_owned(),
            tmp,
        })
    }

    fn from_url(url: &str) -> io::Result<LocalStorage> {
        let url = Url::parse(url)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{} local storage", e)))?;
        if url.scheme() != LocalStorage::SCHEME {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("[{}] schema not match local storage", url.scheme()),
            ));
        }
        LocalStorage::new(Path::new(url.path()))
    }

    // TODO(backup): gc tmp files.
    fn tmp_path(&self, path: &Path) -> PathBuf {
        let uid: u64 = rand::thread_rng().gen();
        let tmp_suffix = format!("{}{:016x}", LOCAL_STORAGE_TMP_FILE_SUFFIX, uid);
        self.tmp.join(path).with_extension(tmp_suffix)
    }
}

// TODO(backup): fsync dirs.
impl Storage for LocalStorage {
    fn write(&self, name: &str, reader: &mut dyn Read) -> io::Result<()> {
        // Storage does not support dir,
        // "a/a.sst", "/" and "" will return an error.
        if Path::new(name)
            .parent()
            .map_or(true, |p| p.parent().is_some())
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("[{}] parent is not allowed in storage", name),
            ));
        }
        // Sanitize check, do not save file if it is already exist.
        if fs::metadata(self.base.join(name)).is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("[{}] is already exists in {}", name, self.base.display()),
            ));
        }
        let tmp_path = self.tmp_path(Path::new(name));
        let mut tmp_f = File::create(&tmp_path)?;
        io::copy(reader, &mut tmp_f)?;
        tmp_f.metadata()?.permissions().set_readonly(true);
        tmp_f.sync_all()?;
        fs::rename(tmp_path, self.base.join(name))
    }

    fn read(&self, name: &str) -> io::Result<Box<dyn Read>> {
        let file = File::open(self.base.join(name))?;
        Ok(Box::new(file))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    #[test]
    fn test_local_storage() {
        let temp_dir = Builder::new().tempdir().unwrap();
        let path = temp_dir.path();
        let ls = LocalStorage::from_url(&format!("{}://{}", LocalStorage::SCHEME, path.display()))
            .unwrap();

        // Test tmp_path
        let tp = ls.tmp_path(Path::new("t.sst"));
        assert_eq!(tp.parent().unwrap(), path.join(LOCAL_STORAGE_TMP_DIR));
        assert!(tp.file_name().unwrap().to_str().unwrap().starts_with('t'));
        assert!(tp
            .as_path()
            .extension()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with(LOCAL_STORAGE_TMP_FILE_SUFFIX));

        // Test save_file
        let mut magic_contents = "5678".as_bytes();
        ls.write("a.log", &mut magic_contents.clone()).unwrap();
        assert_eq!(fs::read(path.join("a.log")).unwrap(), magic_contents);

        // Names contain parent is not allowed.
        ls.write("a/a.log", &mut magic_contents).unwrap_err();
        // Empty name is not allowed.
        ls.write("", &mut magic_contents).unwrap_err();
        // root is not allowed.
        ls.write("/", &mut magic_contents).unwrap_err();
    }
}
