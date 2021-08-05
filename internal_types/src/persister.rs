use bytes::Bytes;
use data_types::server_id::ServerId;
use futures::{stream::BoxStream, Stream};
use object_store::{
    path::{ObjectStorePath, Path},
    ListResult, ObjectStore, ObjectStoreApi, Result,
};
use std::{io, sync::Arc};

/// Handles persistence of data for a particular database. Writes within its directory/prefix.
#[derive(Debug)]
pub struct Persister {
    store: Arc<ObjectStore>,
    server_id: ServerId,
    database_name: String, // data_types DatabaseName?
}

impl Persister {
    pub fn new(store: Arc<ObjectStore>, server_id: ServerId, database_name: &str) -> Self {
        Self {
            store,
            server_id,
            database_name: database_name.into(),
        }
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Path where transactions are stored.
    ///
    /// The format is:
    ///
    /// ```text
    /// <server_id>/<db_name>/transactions/
    /// ```
    pub fn catalog_path(&self) -> Path {
        let mut path = self.store.new_path();
        path.push_dir(self.server_id.to_string());
        path.push_dir(&self.database_name);
        path.push_dir("transactions");
        path
    }

    /// Location where parquet data goes to.
    ///
    /// Schema currently is:
    ///
    /// ```text
    /// <server_id>/<db_name>/data/
    /// ```
    pub fn data_path(&self) -> Path {
        let mut path = self.store.new_path();
        path.push_dir(self.server_id.to_string());
        path.push_dir(&self.database_name);
        path.push_dir("data");
        path
    }

    pub async fn put<S>(&self, _location: &Path, _bytes: S, _length: Option<usize>) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        unimplemented!();
    }

    pub async fn list<'a>(
        &'a self,
        _prefix: Option<&'a Path>,
    ) -> Result<BoxStream<'a, Result<Vec<Path>>>> {
        unimplemented!();
    }

    pub async fn list_with_delimiter(&self, _prefix: &Path) -> Result<ListResult<Path>> {
        unimplemented!();
    }

    pub async fn get(&self, _location: &Path) -> Result<BoxStream<'static, Result<Bytes>>> {
        unimplemented!();
    }

    pub async fn delete(&self, _location: &Path) -> Result<()> {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    /// Creates new test server ID
    fn make_server_id() -> ServerId {
        ServerId::new(NonZeroU32::new(1).unwrap())
    }

    /// Creates a new in-memory object store. These tests rely on the `Path`s being of type
    /// `DirsAndFileName` and thus using object_store::path::DELIMITER as the separator
    fn make_object_store() -> Arc<ObjectStore> {
        Arc::new(ObjectStore::new_in_memory())
    }

    #[test]
    fn catalog_path_is_relative_to_db_root() {
        let server_id = make_server_id();
        let database_name = "clouds";
        let persister = Persister::new(make_object_store(), server_id, database_name);
        assert_eq!(persister.catalog_path().display(), "1/clouds/transactions/");
    }

    #[test]
    fn data_path_is_relative_to_db_root() {
        let server_id = make_server_id();
        let database_name = "clouds";
        let persister = Persister::new(make_object_store(), server_id, database_name);
        assert_eq!(persister.data_path().display(), "1/clouds/data/");
    }
}
