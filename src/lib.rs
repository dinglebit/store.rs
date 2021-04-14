//! A generic storage interface for accessing and storing blobs,
//! objects, whatever.

use std::fs::{remove_file, File};
use std::io::{copy, Cursor, ErrorKind};

use bincode;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use sled;

/// The errors this package returns.
#[derive(Debug)]
pub enum Error {
    /// This is more of a generic error that something bad happened.
    SystemError(String),

    /// The requested path was not valid (e.g. not UTF-8).
    InvalidPath,

    /// The process did not have permissions to perform the operation.
    PermissionDenied,

    /// The request object was not found.
    NotFound,
}

/// An object contains general information about the object.
pub trait Object {
    /// The name (e.g. key) of the object.
    fn name(&self) -> &str;

    /// The number of bytes in the object.
    fn size(&self) -> usize;

    /// When the object was originally created.
    fn created(&self) -> DateTime<Utc>;

    /// When the object was last accessed.
    fn accessed(&self) -> Option<DateTime<Utc>>;
}

/// A generic Storage system.
pub trait Store<O, I>
where
    O: Object,
    I: std::iter::Iterator<Item = Result<O, Error>>,
{
    /// Save the content os r into the storage system with the given
    /// name.
    fn put_from<R: std::io::Read>(&mut self, name: &str, r: &mut R) -> Result<usize, Error>;

    /// Similar to put_from but uses the give data instead of a
    /// reader.
    fn put(&mut self, name: &str, data: &[u8]) -> Result<(), Error> {
        match self.put_from(name, &mut Cursor::new(data)) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Read the contents of the object with the given name into the
    /// given writer.
    fn get_to<W: std::io::Write>(&mut self, name: &str, w: &mut W) -> Result<usize, Error>;

    /// Similar to get_to but reads the contents into a byte vector.
    fn get(&mut self, name: &str) -> Result<Vec<u8>, Error> {
        let mut buf: Vec<u8> = Vec::new();
        match self.get_to(name, &mut buf) {
            Ok(_) => Ok(buf),
            Err(e) => Err(e),
        }
    }

    /// Find all the files that have the given prefix and iterate over
    /// them.
    fn prefix(&mut self, prefix: &str) -> I;

    /// Delete the file with the given name.
    fn delete(&mut self, name: &str) -> Result<(), Error>;

    /// Similar to delete but it does not return an error if the error
    /// is `NotFound`.
    fn remove(&mut self, name: &str) -> Result<(), Error> {
        match self.delete(name) {
            Ok(_) => Ok(()),
            Err(e) => match e {
                Error::NotFound => Ok(()),
                _ => Err(e),
            },
        }
    }
}

/// A simple Object implementation.
#[derive(Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
pub struct SimpleObject {
    n: String,
    s: usize,
    c: DateTime<Utc>,
    a: Option<DateTime<Utc>>,
}

impl SimpleObject {
    // Create a new SimpleObject with the given values.
    pub fn new(n: String, s: usize, c: DateTime<Utc>, a: Option<DateTime<Utc>>) -> Self {
        Self { n, s, c, a }
    }
}

impl Object for SimpleObject {
    fn name(&self) -> &str {
        self.n.as_str()
    }

    fn size(&self) -> usize {
        self.s
    }

    fn created(&self) -> DateTime<Utc> {
        self.c
    }

    fn accessed(&self) -> Option<DateTime<Utc>> {
        self.a
    }
}

/// An implementation of a Store using a folder on a file system.
#[derive(Debug)]
pub struct FileSystem {
    db: sled::Db,
    path: std::path::PathBuf,
    hasher: Sha512,
}

impl FileSystem {
    /// Create an instance of a FileSystem that uses the given path to
    /// store objects and metadata.
    pub fn new(path: &str) -> Result<Self, Error> {
        let path = std::path::PathBuf::from(path);
        let db = path.join("files.db");
        let db = match db.to_str() {
            Some(path) => path,
            None => return Err(Error::InvalidPath),
        };
        let db = match sled::open(db) {
            Ok(db) => db,
            Err(e) => return Err(Error::SystemError(e.to_string())),
        };

        Ok(Self {
            db: db,
            path: path,
            hasher: Sha512::new(),
        })
    }

    /// Helper function to get the name as a sha512 hash.
    fn path_hash(&mut self, name: &str) -> String {
        self.hasher.update(name);
        self.path
            .join(format!("{:x}", self.hasher.finalize_reset()))
            .to_str()
            .unwrap()
            .to_string()
    }
}

impl Store<SimpleObject, FileSystemIter> for FileSystem {
    fn put_from<R: std::io::Read>(&mut self, name: &str, r: &mut R) -> Result<usize, Error> {
        // Determine the path location and open up a file handle.
        let path = self.path_hash(name);
        let mut f = match File::create(path.clone()) {
            Ok(f) => f,
            Err(e) => match e.kind() {
                ErrorKind::NotFound => return Err(Error::NotFound),
                ErrorKind::PermissionDenied => return Err(Error::PermissionDenied),
                _ => return Err(Error::SystemError(e.to_string())),
            },
        };

        // Copy to the file.
        let w = match copy(r, &mut f) {
            Ok(w) => w as usize,
            Err(e) => {
                return match e.kind() {
                    ErrorKind::NotFound => Err(Error::NotFound),
                    ErrorKind::PermissionDenied => Err(Error::PermissionDenied),
                    _ => Err(Error::SystemError(e.to_string())),
                }
            }
        };

        // Create the object and then store it in the database. We use the original name here.
        let o = SimpleObject::new(name.to_string(), w, Utc::now(), None);
        let data = match bincode::serialize(&o) {
            Ok(d) => d,
            Err(e) => return Err(Error::SystemError(e.to_string())),
        };
        match self.db.insert(name, data) {
            Ok(_) => Ok(w),
            Err(e) => Err(Error::SystemError(e.to_string())),
        }
    }

    fn get_to<W: std::io::Write>(&mut self, name: &str, w: &mut W) -> Result<usize, Error> {
        // NOTE: no need to modify the database here. We could
        // eventually add an access time.

        // Open the file at the given path.
        let name = self.path_hash(name);
        let mut f = match File::open(name) {
            Ok(f) => f,
            Err(e) => match e.kind() {
                ErrorKind::NotFound => return Err(Error::NotFound),
                ErrorKind::PermissionDenied => return Err(Error::PermissionDenied),
                _ => return Err(Error::SystemError(e.to_string())),
            },
        };

        // Copy from the file to the given write.
        match copy(&mut f, w) {
            Ok(w) => Ok(w as usize),
            Err(e) => match e.kind() {
                ErrorKind::NotFound => Err(Error::NotFound),
                ErrorKind::PermissionDenied => Err(Error::PermissionDenied),
                _ => Err(Error::SystemError(e.to_string())),
            },
        }
    }

    fn delete(&mut self, name: &str) -> Result<(), Error> {
        // Determine the file path
        let path = self.path_hash(name);

        // Remove it from the database. We use the given name here,
        // not the entire path.
        match self.db.remove(name.clone()) {
            Ok(_) => (),
            Err(e) => return Err(Error::SystemError(e.to_string())),
        };

        // Remove it from the file system.
        match remove_file(path) {
            Ok(_) => Ok(()),
            Err(e) => match e.kind() {
                ErrorKind::NotFound => Err(Error::NotFound),
                ErrorKind::PermissionDenied => Err(Error::PermissionDenied),
                _ => Err(Error::SystemError(e.to_string())),
            },
        }
    }

    fn prefix(&mut self, prefix: &str) -> FileSystemIter {
        FileSystemIter {
            iter: self.db.scan_prefix(prefix),
        }
    }
}

pub struct FileSystemIter {
    iter: sled::Iter,
}

impl Iterator for FileSystemIter {
    type Item = Result<SimpleObject, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        let data = match self.iter.next() {
            Some(r) => match r {
                Ok(data) => data,
                Err(e) => return Some(Err(Error::SystemError(e.to_string()))),
            },
            None => return None,
        };
        let o: SimpleObject = match bincode::deserialize(&(data.1)) {
            Ok(o) => o,
            Err(e) => return Some(Err(Error::SystemError(e.to_string()))),
        };
        return Some(Ok(o));
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use tempdir::TempDir;

    #[test]
    fn simple_object() {
        let now = Utc::now();
        let so = SimpleObject::new("test".to_string(), 1043, now, None);
        assert_eq!(so.name(), "test".to_string());
        assert_eq!(so.size(), 1043);
        assert_eq!(so.created(), now);
        assert_eq!(so.accessed(), None);
    }

    #[test]
    fn it_works() {
        let td = TempDir::new("test-file-system").expect("unable to make tempdir");
        let mut fs = FileSystem::new(td.path().to_str().expect("unable to convert tempdir"))
            .expect("failed opening filesystem");

        fs.put("test-1", b"test-1").expect("failed to put test-1");
        fs.put("test-2", b"test-2").expect("failed to put test-2");
        fs.put("test-3", b"test-3").expect("failed to put test-3");
        fs.put("test-4", b"test-4").expect("failed to put test-4");
        fs.put("a", b"a").expect("failed to put a");
        fs.put("b", b"c").expect("failed to put b");
        fs.put("c", b"c").expect("failed to put c");

        assert_eq!(fs.get("test-1").expect("failed to get test-1"), b"test-1");
        assert_eq!(fs.get("test-2").expect("failed to get test-2"), b"test-2");
        assert_eq!(fs.get("test-3").expect("failed to get test-3"), b"test-3");
        assert_eq!(fs.get("test-4").expect("failed to get test-4"), b"test-4");
        assert!(fs.get("not found").is_err());

        fs.remove("test-1").expect("failed to remove test-1");
        fs.remove("test-1").expect("failed to remove test-1"); // ignore not found
        assert!(fs.get("test-1").is_err());

        let mut list: Vec<String> = Vec::new();
        for res in fs.prefix("test") {
            let f = res.expect("failed to get in loop");
            list.push(f.name().to_string());
        }
        list.sort();
        assert_eq!(list, vec!["test-2", "test-3", "test-4"]);
    }
}
