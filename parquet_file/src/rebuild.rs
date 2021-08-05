//! Contains code to rebuild a catalog from files.
use std::{fmt::Debug, sync::Arc};

use futures::TryStreamExt;
use internal_types::persister::Persister;
use object_store::path::{parsed::DirsAndFileName, Path};
use observability_deps::tracing::error;
use snafu::{ResultExt, Snafu};

use crate::{
    catalog::{CatalogParquetInfo, CatalogState, PreservedCatalog},
    metadata::IoxParquetMetaData,
};
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot create new empty catalog: {}", source))]
    NewEmptyFailure { source: crate::catalog::Error },

    #[snafu(display("Cannot read store: {}", source))]
    ReadFailure { source: object_store::Error },

    #[snafu(display("Cannot read IOx metadata from parquet file ({:?}): {}", path, source))]
    MetadataReadFailure {
        source: crate::metadata::Error,
        path: Path,
    },

    #[snafu(display("Cannot add file to transaction: {}", source))]
    FileRecordFailure { source: crate::catalog::Error },

    #[snafu(display("Cannot commit transaction: {}", source))]
    CommitFailure { source: crate::catalog::Error },

    #[snafu(display("Cannot create checkpoint: {}", source))]
    CheckpointFailure { source: crate::catalog::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Creates a new catalog from parquet files.
///
/// Users are required to [wipe](PreservedCatalog::wipe) the existing catalog before running this
/// procedure (**after creating a backup!**).
///
/// # Limitations
/// Compared to an intact catalog, wiping a catalog and rebuilding it from Parquet files has the following drawbacks:
///
/// - **Garbage Susceptibility:** The rebuild process will stumble over garbage parquet files (i.e. files being present
///   in the object store but that were not part of the catalog). For files that where not written by IOx it will likely
///   report [`Error::MetadataReadFailure`].
/// - **No Removals:** The rebuild system cannot recover the fact that files where removed from the catalog during some
///   transaction. This might not always be an issue due to "deduplicate while read"-logic in the query engine, but also
///   might have unwanted side effects (e.g. performance issues).
/// - **Single Transaction:** All files are added in a single transaction. Hence time-traveling is NOT possible using
///   the resulting catalog.
/// - **Fork Detection:** The rebuild procedure does NOT detects forks (i.e. files written for the same server ID by
///   multiple IOx instances).
///
/// # Error Handling
/// This routine will fail if:
///
/// - **Metadata Read Failure:** There is a parquet file with metadata that cannot be read. Set
///   `ignore_metadata_read_failure` to `true` to ignore these cases.
pub async fn rebuild_catalog<S>(
    persister: Arc<Persister>,
    search_location: &Path,
    catalog_empty_input: S::EmptyInput,
    ignore_metadata_read_failure: bool,
) -> Result<(PreservedCatalog, S)>
where
    S: CatalogState + Debug + Send + Sync,
{
    // collect all revisions from parquet files
    let files = collect_files(&persister, search_location, ignore_metadata_read_failure).await?;

    // create new empty catalog
    let (catalog, mut state) =
        PreservedCatalog::new_empty::<S>(Arc::clone(&persister), catalog_empty_input)
            .await
            .context(NewEmptyFailure)?;

    // create single transaction with all files
    if !files.is_empty() {
        let mut transaction = catalog.open_transaction().await;
        for info in files {
            state
                .add(Arc::clone(&persister), info.clone())
                .context(FileRecordFailure)?;
            transaction.add_parquet(&info).context(FileRecordFailure)?;
        }
        transaction.commit().await.context(CheckpointFailure)?;
    }

    Ok((catalog, state))
}

/// Collect all files under the given locations.
///
/// Returns a vector of (file, size, metadata) tuples.
///
/// The file listing is recursive.
async fn collect_files(
    persister: &Persister,
    search_location: &Path,
    ignore_metadata_read_failure: bool,
) -> Result<Vec<CatalogParquetInfo>> {
    let mut stream = persister
        .list(Some(search_location))
        .await
        .context(ReadFailure)?;

    let mut files = vec![];

    while let Some(paths) = stream.try_next().await.context(ReadFailure)? {
        for path in paths.into_iter().filter(is_parquet) {
            match read_parquet(persister, &path).await {
                Ok((file_size_bytes, metadata)) => {
                    let path = path.into();
                    files.push(CatalogParquetInfo {
                        path,
                        file_size_bytes,
                        metadata,
                    });
                }
                Err(e @ Error::MetadataReadFailure { .. }) if ignore_metadata_read_failure => {
                    error!("error while reading metdata from parquet, ignoring: {}", e);
                    continue;
                }
                Err(e) => return Err(e),
            };
        }
    }

    Ok(files)
}

/// Checks if the given path is (likely) a parquet file.
fn is_parquet(path: &Path) -> bool {
    let path: DirsAndFileName = path.clone().into();
    if let Some(filename) = path.file_name {
        filename.encoded().ends_with(".parquet")
    } else {
        false
    }
}

/// Read Parquet and IOx metadata from given path.
async fn read_parquet(
    persister: &Persister,
    path: &Path,
) -> Result<(usize, Arc<IoxParquetMetaData>)> {
    let data = persister
        .get(path)
        .await
        .context(ReadFailure)?
        .map_ok(|bytes| bytes.to_vec())
        .try_concat()
        .await
        .context(ReadFailure)?;

    let file_size_bytes = data.len();

    let parquet_metadata = IoxParquetMetaData::from_file_bytes(data)
        .context(MetadataReadFailure { path: path.clone() })?;

    // validate IOxMetadata
    parquet_metadata
        .read_iox_metadata()
        .context(MetadataReadFailure { path: path.clone() })?;

    Ok((file_size_bytes, Arc::new(parquet_metadata)))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use data_types::chunk_metadata::ChunkAddr;
    use datafusion::physical_plan::SendableRecordBatchStream;
    use datafusion_util::MemoryStream;
    use parquet::arrow::ArrowWriter;
    use tokio_stream::StreamExt;

    use super::*;

    use crate::metadata::IoxMetadata;
    use crate::test_utils::create_partition_and_database_checkpoint;
    use crate::{catalog::test_helpers::TestCatalogState, storage::MemWriter};
    use crate::{
        catalog::PreservedCatalog,
        storage::Storage,
        test_utils::{make_persister, make_record_batch},
    };

    #[tokio::test]
    async fn test_rebuild_successfull() {
        let persister = make_persister();

        // build catalog with some data
        let (catalog, mut state) =
            PreservedCatalog::new_empty::<TestCatalogState>(Arc::clone(&persister), ())
                .await
                .unwrap();
        {
            let mut transaction = catalog.open_transaction().await;

            let info = create_parquet_file(&persister, 0).await;
            state.parquet_files.insert(info.path.clone(), info.clone());
            transaction.add_parquet(&info).unwrap();

            let info = create_parquet_file(&persister, 1).await;
            state.parquet_files.insert(info.path.clone(), info.clone());
            transaction.add_parquet(&info).unwrap();

            transaction.commit().await.unwrap();
        }
        {
            // empty transaction
            let transaction = catalog.open_transaction().await;
            transaction.commit().await.unwrap();
        }
        {
            let mut transaction = catalog.open_transaction().await;

            let info = create_parquet_file(&persister, 2).await;
            state.parquet_files.insert(info.path.clone(), info.clone());
            transaction.add_parquet(&info).unwrap();

            transaction.commit().await.unwrap();
        }

        // store catalog state
        let paths_expected = {
            let mut tmp: Vec<_> = state.parquet_files.keys().cloned().collect();
            tmp.sort();
            tmp
        };

        // wipe catalog
        drop(catalog);
        PreservedCatalog::wipe(&persister).await.unwrap();

        // rebuild
        let path = persister.new_path();
        let (catalog, state) = rebuild_catalog::<TestCatalogState>(persister, &path, (), false)
            .await
            .unwrap();

        // check match
        let paths_actual = {
            let mut tmp: Vec<_> = state.parquet_files.keys().cloned().collect();
            tmp.sort();
            tmp
        };
        assert_eq!(paths_actual, paths_expected);
        assert_eq!(catalog.revision_counter(), 1);
    }

    #[tokio::test]
    async fn test_rebuild_empty() {
        let persister = make_persister();

        // build empty catalog
        let (catalog, _state) =
            PreservedCatalog::new_empty::<TestCatalogState>(Arc::clone(&persister), ())
                .await
                .unwrap();

        // wipe catalog
        drop(catalog);
        PreservedCatalog::wipe(&persister).await.unwrap();

        // rebuild
        let path = persister.new_path();
        let (catalog, state) = rebuild_catalog::<TestCatalogState>(persister, &path, (), false)
            .await
            .unwrap();

        // check match
        assert!(state.parquet_files.is_empty());
        assert_eq!(catalog.revision_counter(), 0);
    }

    #[tokio::test]
    async fn test_rebuild_no_metadata() {
        let persister = make_persister();

        // build catalog with same data
        let catalog = PreservedCatalog::new_empty::<TestCatalogState>(Arc::clone(&persister), ())
            .await
            .unwrap();

        // file w/o metadata
        create_parquet_file_without_metadata(&persister, 0).await;

        // wipe catalog
        drop(catalog);
        PreservedCatalog::wipe(&persister).await.unwrap();

        // rebuild (do not ignore errors)
        let path = persister.new_path();
        let res =
            rebuild_catalog::<TestCatalogState>(Arc::clone(&persister), &path, (), false).await;
        assert!(dbg!(res.unwrap_err().to_string())
            .starts_with("Cannot read IOx metadata from parquet file"));

        // rebuild (ignore errors)
        let (catalog, state) = rebuild_catalog::<TestCatalogState>(persister, &path, (), true)
            .await
            .unwrap();
        assert!(state.parquet_files.is_empty());
        assert_eq!(catalog.revision_counter(), 0);
    }

    #[tokio::test]
    async fn test_rebuild_creates_no_checkpoint() {
        // the rebuild method will create a catalog with the following transactions:
        // 1. an empty one (done by `PreservedCatalog::new_empty`)
        // 2. an "add all the files"
        //
        // There is no real need to create a checkpoint in this case. So here we delete all transaction files and then
        // check that rebuilt catalog will be gone afterwards. Note the difference to the `test_rebuild_empty` case
        // where we can indeed proof the existence of a catalog (even though it is empty aka has no files).
        let persister = make_persister();

        // build catalog with some data (2 transactions + initial empty one)
        let (catalog, _state) =
            PreservedCatalog::new_empty::<TestCatalogState>(Arc::clone(&persister), ())
                .await
                .unwrap();
        assert_eq!(catalog.revision_counter(), 0);

        // wipe catalog
        drop(catalog);
        PreservedCatalog::wipe(&persister).await.unwrap();

        // rebuild
        let path = persister.new_path();
        let catalog = rebuild_catalog::<TestCatalogState>(Arc::clone(&persister), &path, (), false)
            .await
            .unwrap();
        drop(catalog);

        // delete transaction files
        let paths = persister
            .list(None)
            .await
            .unwrap()
            .try_concat()
            .await
            .unwrap();
        let mut deleted = false;
        for path in paths {
            let parsed: DirsAndFileName = path.clone().into();
            if parsed
                .file_name
                .map_or(false, |part| part.encoded().ends_with(".txn"))
            {
                persister.delete(&path).await.unwrap();
                deleted = true;
            }
        }
        assert!(deleted);

        // catalog gone
        assert!(!PreservedCatalog::exists(&persister).await.unwrap());
    }

    pub async fn create_parquet_file(
        persister: &Arc<Persister>,
        chunk_id: u32,
    ) -> CatalogParquetInfo {
        let table_name = Arc::from("table1");
        let partition_key = Arc::from("part1");
        let (record_batches, _schema, _column_summaries, _num_rows) = make_record_batch("foo");

        let storage = Storage::new(Arc::clone(persister));
        let (partition_checkpoint, database_checkpoint) = create_partition_and_database_checkpoint(
            Arc::clone(&table_name),
            Arc::clone(&partition_key),
        );
        let metadata = IoxMetadata {
            creation_timestamp: Utc::now(),
            table_name: Arc::clone(&table_name),
            partition_key: Arc::clone(&partition_key),
            chunk_id,
            partition_checkpoint,
            database_checkpoint,
            time_of_first_write: Utc::now(),
            time_of_last_write: Utc::now(),
        };
        let stream: SendableRecordBatchStream = Box::pin(MemoryStream::new(record_batches));
        let (path, file_size_bytes, metadata) = storage
            .write_to_object_store(
                ChunkAddr {
                    db_name: persister.database_name().into(),
                    table_name,
                    partition_key,
                    chunk_id,
                },
                stream,
                metadata,
            )
            .await
            .unwrap();

        CatalogParquetInfo {
            path: path.into(),
            file_size_bytes,
            metadata: Arc::new(metadata),
        }
    }

    pub async fn create_parquet_file_without_metadata(
        persister: &Arc<Persister>,
        chunk_id: u32,
    ) -> (DirsAndFileName, IoxParquetMetaData) {
        let (record_batches, schema, _column_summaries, _num_rows) = make_record_batch("foo");
        let mut stream: SendableRecordBatchStream = Box::pin(MemoryStream::new(record_batches));

        let mem_writer = MemWriter::default();
        {
            let mut writer =
                ArrowWriter::try_new(mem_writer.clone(), Arc::clone(schema.inner()), None).unwrap();
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                writer.write(&batch).unwrap();
            }
            writer.close().unwrap();
        } // drop the reference to the MemWriter that the SerializedFileWriter has

        let data = mem_writer.into_inner().unwrap();
        let md = IoxParquetMetaData::from_file_bytes(data.clone()).unwrap();
        let storage = Storage::new(Arc::clone(persister));
        let path = storage.location(&ChunkAddr {
            db_name: Arc::from(persister.database_name()),
            table_name: Arc::from("table1"),
            partition_key: Arc::from("part1"),
            chunk_id,
        });
        storage.to_object_store(data, &path).await.unwrap();

        let path: DirsAndFileName = path.into();
        (path, md)
    }
}
