//! A storage system for objects (structs) stored by key.

use std::marker::PhantomData;
use std::str::from_utf8;

use serde::{de::DeserializeOwned, Serialize};
use serde_json::{from_str, to_string};

type Result<T> = std::result::Result<T, Error>;

/// The errors this package returns.
#[derive(Debug)]
pub enum Error {
    /// This is more of a generic error that something bad happened.
    SystemError(String),

    /// Serailization/Deserialization failed for some reason.
    Serialization(String),

    /// The process did not have permissions to perform the operation.
    PermissionDenied,
}

/// A Collection contains serializable objects which are accessible by
/// a key.
pub trait Collection<'a, T>
where
    T: Serialize + DeserializeOwned + 'a,
{
    /// Put the given value into the collection under the given
    /// key. If the key already exists, it's replaced with the given
    /// value and the old value is returned.
    fn put(&mut self, key: &str, value: T) -> Result<Option<T>>;

    // Get the value associated with the given key.
    fn get(&mut self, key: &str) -> Result<Option<T>>;

    /// Find all the files that have the given prefix and iterate over
    /// them.
    fn prefix(&mut self, prefix: &str) -> Box<dyn Iterator<Item = Result<T>> + 'a>;
}

/// Sled is an impementation of a Collection using Sled to store the objects.
pub struct Sled {
    db: sled::Db,
}

impl Sled {
    /// Open the given sled database.
    pub fn open(file: &str) -> Result<Self> {
        let db = match sled::open(file) {
            Ok(db) => db,
            Err(e) => return Err(Error::SystemError(e.to_string())),
        };

        Ok(Self { db: db })
    }
}

impl<'a, T> Collection<'a, T> for Sled
where
    T: Serialize + DeserializeOwned + 'a,
{
    fn put(&mut self, key: &str, value: T) -> Result<Option<T>> {
        // Convert to JSON.
        let json = match to_string(&value) {
            Ok(str) => str,
            Err(err) => return Err(Error::Serialization(err.to_string())),
        };

        // Insert into database.
        let value = match self.db.insert(key.as_bytes(), json.as_bytes()) {
            Ok(value) => match value {
                Some(value) => value,
                None => return Ok(None),
            },
            Err(err) => return Err(Error::SystemError(err.to_string())),
        };

        // If we replaced an existing value, convert it to it's type
        // so we can return it.
        let json = match from_utf8(&value) {
            Ok(json) => json,
            Err(err) => return Err(Error::Serialization(err.to_string())),
        };
        let obj: T = match from_str(json) {
            Ok(json) => json,
            Err(err) => return Err(Error::Serialization(err.to_string())),
        };

        Ok(Some(obj))
    }

    fn get(&mut self, key: &str) -> Result<Option<T>> {
        // Get the value from the database.
        let ivec = match self.db.get(key.as_bytes()) {
            Err(err) => return Err(Error::SystemError(err.to_string())),
            Ok(ivec) => match ivec {
                Some(ivec) => ivec,
                None => return Ok(None),
            },
        };

        // Convert it to the given type and return it.
        let json = match from_utf8(&ivec) {
            Ok(json) => json,
            Err(err) => return Err(Error::Serialization(err.to_string())),
        };
        let json: T = match from_str(json) {
            Ok(json) => json,
            Err(err) => return Err(Error::Serialization(err.to_string())),
        };

        Ok(Some(json))
    }

    fn prefix(&mut self, prefix: &str) -> Box<dyn Iterator<Item = Result<T>> + 'a> {
        Box::new(SledIter {
            p: PhantomData,
            iter: self.db.scan_prefix(prefix.as_bytes()),
        })
    }
}

/// SledIter implements the std Iterator for the prefix method.
pub struct SledIter<T> {
    p: PhantomData<T>,
    iter: sled::Iter,
}

impl<T> Iterator for SledIter<T>
where
    T: DeserializeOwned,
{
    type Item = Result<T>;
    fn next(&mut self) -> Option<Self::Item> {
        // Get the next value.
        let data = match self.iter.next() {
            Some(r) => match r {
                Ok(data) => data,
                Err(e) => return Some(Err(Error::SystemError(e.to_string()))),
            },
            None => return None,
        };

        // Convert it to the given type and return it.
        let json = match from_utf8(&(data.1)) {
            Ok(json) => json,
            Err(err) => return Some(Err(Error::Serialization(err.to_string()))),
        };
        let json: T = match from_str(json) {
            Ok(json) => json,
            Err(err) => return Some(Err(Error::Serialization(err.to_string()))),
        };
        Some(Ok(json))
    }
}

#[cfg(test)]
mod tests {
    use crate::collection::*;
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct Person {
        name: String,
    }

    #[test]
    fn collection_get_put() {
        let file = tempdir().expect("unable to make temp file");
        let mut s: Box<dyn Collection<'static, Person>> =
            Box::new(Sled::open(file.path().to_str().unwrap()).expect("failed to open with sled"));

        let p = Person {
            name: "foo".to_string(),
        };
        s.put(p.name.as_str(), p.clone())
            .expect("failed to put foo");

        let r = s.get(p.name.as_str()).expect("failed to get foo").unwrap();
        assert_eq!(p, r);

        assert_eq!(
            s.get("i don't exist").expect("failed to get non-existant"),
            None
        );
    }

    #[test]
    fn collection_prefix() {
        let file = tempdir().expect("unable to make temp file");
        let mut s: Box<dyn Collection<'static, Person>> =
            Box::new(Sled::open(file.path().to_str().unwrap()).expect("failed to open with sled"));

        let pp = vec![
            Person {
                name: "foo".to_string(),
            },
            Person {
                name: "bar/foo0".to_string(),
            },
            Person {
                name: "bar/foo1".to_string(),
            },
            Person {
                name: "bar/foo2".to_string(),
            },
            Person {
                name: "baz".to_string(),
            },
        ];

        for p in pp {
            s.put(p.name.as_str(), p.clone()).expect("failed to put");
        }

        let rr = s
            .prefix("bar/")
            .map(|r| r.expect("collecting"))
            .collect::<Vec<Person>>();

        let exp = vec![
            Person {
                name: "bar/foo0".to_string(),
            },
            Person {
                name: "bar/foo1".to_string(),
            },
            Person {
                name: "bar/foo2".to_string(),
            },
        ];

        assert_eq!(rr, exp);
    }
}
