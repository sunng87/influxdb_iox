//! This crate contains code that defines the logic for a running InfluxDB IOx
//! server. It also has the logic for how servers talk to each other, which
//! includes replication, subscriptions, querying, and traits that abstract
//! these methods away for testing purposes.
//!
//! This diagram shows the lifecycle of a write coming into a set of IOx servers
//! configured in different roles. This doesn't include ensuring that the
//! replicated writes are durable, or snapshotting partitions in the write
//! buffer. Have a read through the comments in the source before trying to make
//! sense of this diagram.
//!
//! Each level of servers exists to serve a specific function, ideally isolating
//! the kinds of failures that would cause one portion to go down.
//!
//! The router level simply converts the line protocol to the flatbuffer format
//! and computes the partition key. It keeps no state.
//!
//! The HostGroup/AZ level is for receiving the replicated writes and keeping
//! multiple copies of those writes in memory before they are persisted to
//! object storage. Individual databases or groups of databases can be routed to
//! the same set of host groups, which will limit the blast radius for databases
//! that overload the system with writes or for situations where subscribers lag
//! too far behind.
//!
//! The Subscriber level is for actually pulling in the data and making it
//! available for query through indexing in the write buffer or writing that
//! data out to Parquet in object storage. Subscribers can also be used for
//! real-time alerting and monitoring.
//!
//! ```text
//!                                    ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!            ┌────────┐  ┌────────┐   Step 1:                 │
//!            │Router 1│  │Router 2│  │  Parse LP
//!            │        │  │        │     Create SequencedEntry │
//!            └───┬─┬──┘  └────────┘  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!                │ │
//!                │ │                     ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!       ┌────────┘ └───┬──────────────┐   Step 2:                 │
//!       │              │              │  │  Replicate to
//!       │              │              │     all host groups       │
//!       ▼              ▼              ▼  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//! ┌───────────┐  ┌───────────┐  ┌───────────┐
//! │HostGroup 1│  │HostGroup 2│  │HostGroup 3│
//! │(AZ 1)     │  │(AZ 2)     │  │(AZ 3)     │
//! └───────────┘  └───────────┘  └───────────┘
//!       │
//!       │
//!       │     ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!       │      Step 3:                 │
//!       └──┐  │  Push subscription
//!          │                           │
//!          │  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!          │
//!          ▼
//!   ┌────────────┐  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!   │Query Server│   Step 4:                 │
//!   │(subscriber)│  │  Store in WriteBuffer
//!   │            │                           │
//!   └────────────┘  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//! ```

#![deny(broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use data_types::database_rules::ShardConfig;
use data_types::{
    database_rules::{DatabaseRules, NodeGroup, RoutingRules, ShardId, Sink},
    job::Job,
    server_id::ServerId,
    {DatabaseName, DatabaseNameError},
};
use database::Database;
use db::load::create_preserved_catalog;
use entry::{lines_to_sharded_entries, pb_to_entry, Entry, ShardedEntry};
use generated_types::database_rules::encode_database_rules;
use generated_types::influxdata::pbdata::v1 as pb;
use hashbrown::HashMap;
use influxdb_line_protocol::ParsedLine;
use internal_types::freezable::Freezable;
use lifecycle::LockableChunk;
use metrics::{KeyValue, MetricObserverBuilder};
use object_store::{ObjectStore, ObjectStoreApi};
use observability_deps::tracing::{error, info, warn};
use parking_lot::RwLock;
use query::{exec::Executor, DatabaseStore};
use rand::seq::SliceRandom;
use resolver::Resolver;
use snafu::{OptionExt, ResultExt, Snafu};
use tracker::{TaskTracker, TrackedFutureExt};

use crate::database::DatabaseConfig;
pub use application::ApplicationState;
pub use connection::{ConnectionManager, ConnectionManagerImpl, RemoteServer};
use data_types::error::ErrorLogger;
pub use db::Db;
pub use job::JobRegistry;
use object_store::path::parsed::DirsAndFileName;
use object_store::path::{ObjectStorePath, Path};
pub use resolver::{GrpcConnectionString, RemoteTemplate};

mod application;
mod connection;
pub mod database;
pub mod db;
mod job;
mod resolver;

/// Utility modules used by benchmarks and tests
pub mod utils;

type DatabaseError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("id not set"))]
    IdNotSet,

    #[snafu(display(
        "Server ID is set ({}) but server is not yet initialized (e.g. DBs and remotes are not loaded). Server is not yet ready to read/write data.", server_id
    ))]
    ServerNotInitialized { server_id: ServerId },

    #[snafu(display("id already set"))]
    IdAlreadySet,

    #[snafu(display("database not initialized"))]
    DatabaseNotInitialized { db_name: String },

    #[snafu(display("database not found"))]
    DatabaseNotFound { db_name: String },

    #[snafu(display("database already exists: {}", db_name))]
    DatabaseAlreadyExists { db_name: String },

    #[snafu(display("Server error: {}", source))]
    ServerError { source: std::io::Error },

    #[snafu(display("invalid database: {}", source))]
    InvalidDatabaseName { source: DatabaseNameError },

    #[snafu(display("error wiping preserved catalog: {}", source))]
    WipePreservedCatalog { source: database::Error },

    #[snafu(display("database error: {}", source))]
    UnknownDatabaseError { source: DatabaseError },

    #[snafu(display("chunk not found: {}", source))]
    ChunkNotFound { source: db::catalog::Error },

    #[snafu(display("getting mutable buffer chunk: {}", source))]
    MutableBufferChunk { source: DatabaseError },

    #[snafu(display("no local buffer for database: {}", db))]
    NoLocalBuffer { db: String },

    #[snafu(display("unable to get connection to remote server: {}", server))]
    UnableToGetConnection {
        server: String,
        source: DatabaseError,
    },

    #[snafu(display("error replicating to remote: {}", source))]
    ErrorReplicating { source: DatabaseError },

    #[snafu(display("error serializing database rules to protobuf: {}", source))]
    ErrorSerializingRulesProtobuf {
        source: generated_types::database_rules::EncodeError,
    },

    #[snafu(display("store error: {}", source))]
    StoreError { source: object_store::Error },

    #[snafu(display("error converting line protocol to flatbuffers: {}", source))]
    LineConversion { source: entry::Error },

    #[snafu(display("error converting protobuf to flatbuffers: {}", source))]
    PBConversion { source: entry::Error },

    #[snafu(display("shard not found: {}", shard_id))]
    ShardNotFound { shard_id: ShardId },

    #[snafu(display("hard buffer limit reached"))]
    HardLimitReached {},

    #[snafu(display(
        "Cannot write to database {}, it's configured to only read from the write buffer",
        db_name
    ))]
    WritingOnlyAllowedThroughWriteBuffer { db_name: String },

    #[snafu(display("Cannot write to write buffer, bytes {}: {}", bytes, source))]
    WriteBuffer { source: db::Error, bytes: u64 },

    #[snafu(display("no remote configured for node group: {:?}", node_group))]
    NoRemoteConfigured { node_group: NodeGroup },

    #[snafu(display("all remotes failed connecting: {:?}", errors))]
    NoRemoteReachable {
        errors: HashMap<GrpcConnectionString, connection::ConnectionManagerError>,
    },

    #[snafu(display("remote error: {}", source))]
    RemoteError {
        source: connection::ConnectionManagerError,
    },

    #[snafu(display("cannot create preserved catalog: {}", source))]
    CannotCreatePreservedCatalog { source: DatabaseError },

    #[snafu(display("database failed to initialize: {}", source))]
    DatabaseInit { source: Arc<database::InitError> },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A collection of metrics used to instrument the Server.
#[derive(Debug)]
pub struct ServerMetrics {
    /// This metric tracks all requests to the Server
    pub http_requests: metrics::RedMetric,

    /// The number of LP lines ingested
    pub ingest_lines_total: metrics::Counter,

    /// The number of LP fields ingested
    pub ingest_fields_total: metrics::Counter,

    /// The number of LP bytes ingested
    pub ingest_points_bytes_total: metrics::Counter,

    /// The number of Entry bytes ingested
    pub ingest_entries_bytes_total: metrics::Counter,
}

impl ServerMetrics {
    pub fn new(registry: Arc<metrics::MetricRegistry>) -> Self {
        // Server manages multiple domains.
        let http_domain = registry.register_domain("http");
        let ingest_domain = registry.register_domain("ingest");
        let jemalloc_domain = registry.register_domain("jemalloc");

        // This isn't really a property of the server, perhaps it should be somewhere else?
        jemalloc_domain.register_observer(None, &[], |observer: MetricObserverBuilder<'_>| {
            observer.register_gauge_u64(
                "memstats",
                Some("bytes"),
                "jemalloc memstats",
                |observer| {
                    use tikv_jemalloc_ctl::{epoch, stats};
                    epoch::advance().unwrap();

                    let active = stats::active::read().unwrap();
                    observer.observe(active as u64, &[KeyValue::new("stat", "active")]);

                    let allocated = stats::allocated::read().unwrap();
                    observer.observe(allocated as u64, &[KeyValue::new("stat", "alloc")]);

                    let metadata = stats::metadata::read().unwrap();
                    observer.observe(metadata as u64, &[KeyValue::new("stat", "metadata")]);

                    let mapped = stats::mapped::read().unwrap();
                    observer.observe(mapped as u64, &[KeyValue::new("stat", "mapped")]);

                    let resident = stats::resident::read().unwrap();
                    observer.observe(resident as u64, &[KeyValue::new("stat", "resident")]);

                    let retained = stats::retained::read().unwrap();
                    observer.observe(retained as u64, &[KeyValue::new("stat", "retained")]);
                },
            )
        });

        Self {
            http_requests: http_domain.register_red_metric(None),
            ingest_lines_total: ingest_domain.register_counter_metric(
                "points",
                None,
                "total LP points ingested",
            ),
            ingest_fields_total: ingest_domain.register_counter_metric(
                "fields",
                None,
                "total LP field values ingested",
            ),
            ingest_points_bytes_total: ingest_domain.register_counter_metric(
                "points",
                Some("bytes"),
                "total LP points bytes ingested",
            ),
            ingest_entries_bytes_total: ingest_domain.register_counter_metric(
                "entries",
                Some("bytes"),
                "total Entry bytes ingested",
            ),
        }
    }
}

/// Configuration options for `Server`
#[derive(Debug)]
pub struct ServerConfig {
    pub remote_template: Option<RemoteTemplate>,

    pub wipe_catalog_on_error: bool,

    pub skip_replay_and_seek_instead: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            remote_template: None,
            wipe_catalog_on_error: false,
            skip_replay_and_seek_instead: false,
        }
    }
}

/// `Server` is the container struct for how servers store data internally, as
/// well as how they communicate with other servers. Each server will have one
/// of these structs, which keeps track of all replication and query rules.
#[derive(Debug)]
pub struct Server<M: ConnectionManager> {
    connection_manager: Arc<M>,

    metrics: Arc<ServerMetrics>,

    /// Resolver for mapping ServerId to gRPC connection strings
    resolver: RwLock<Resolver>,

    /// State shared with the background worker
    shared: Arc<ServerShared>,
}

#[derive(Debug)]
struct ServerShared {
    application: Arc<ApplicationState>,

    state: RwLock<Freezable<ServerState>>,
}

#[derive(Debug, Snafu)]
pub enum InitError {
    #[snafu(display("error listing databases in object storage: {}", source))]
    ListRules { source: object_store::Error },
}

/// The stage of the server in the startup process
///
/// The progression is linear Startup -> InitReady -> Initializing -> Initialized
/// with the sole exception that on failure Initializing -> InitReady
///
/// Errors encountered on server init will be retried, however, errors encountered
/// during database init will require operator intervention
///
/// These errors are exposed via `Server::error_generic` and `Server::error_database` respectively
///
/// They do not impact the state machine's progression, but instead are exposed to the
/// gRPC management API to allow an operator to assess the state of the system
#[derive(Debug)]
enum ServerState {
    /// Server has started but doesn't have a server id yet
    Startup(ServerStateStartup),

    /// Server can be initialized
    InitReady(ServerStateInitReady),

    /// Server encountered error initializing
    InitError(ServerStateInitReady, Arc<InitError>),

    /// Server has finish initializing
    Initialized(ServerStateInitialized),
}

impl ServerState {
    fn initialized(&self) -> Result<&ServerStateInitialized> {
        match self {
            ServerState::Startup(_) => Err(Error::IdNotSet),
            ServerState::InitReady(state) | ServerState::InitError(state, _) => {
                Err(Error::ServerNotInitialized {
                    server_id: state.server_id,
                })
            }
            ServerState::Initialized(state) => Ok(state),
        }
    }

    fn server_id(&self) -> Option<ServerId> {
        match self {
            ServerState::Startup(_) => None,
            ServerState::InitReady(state) => Some(state.server_id),
            ServerState::InitError(state, _) => Some(state.server_id),
            ServerState::Initialized(state) => Some(state.server_id),
        }
    }
}

#[derive(Debug, Clone)]
struct ServerStateStartup {
    wipe_catalog_on_error: bool,
    skip_replay_and_seek_instead: bool,
}

#[derive(Debug, Clone)]
struct ServerStateInitReady {
    wipe_catalog_on_error: bool,
    skip_replay_and_seek_instead: bool,
    server_id: ServerId,
}

#[derive(Debug)]
struct ServerStateInitialized {
    server_id: ServerId,

    /// A lock that is held whilst creating a new database, this is necessary because
    /// not all object stores can provide CreateIfNotExists semantics and therefore
    /// we must prevent two catalogs being created at the same path simultaneously
    create_database_lock: Arc<tokio::sync::Mutex<()>>,

    /// A map of possibly initialized `Database` owned by this `Server`
    databases: HashMap<DatabaseName<'static>, Arc<Database>>,
}

#[derive(Debug)]
pub enum UpdateError<E> {
    Update(Error),
    Closure(E),
}

impl<E> From<Error> for UpdateError<E> {
    fn from(e: Error) -> Self {
        Self::Update(e)
    }
}

impl<M> Server<M>
where
    M: ConnectionManager + Send + Sync,
{
    pub fn new(
        connection_manager: M,
        application: Arc<ApplicationState>,
        config: ServerConfig,
    ) -> Self {
        let metrics = Arc::new(ServerMetrics::new(Arc::clone(
            application.metric_registry(),
        )));

        Self {
            metrics,
            connection_manager: Arc::new(connection_manager),
            resolver: RwLock::new(Resolver::new(config.remote_template)),
            shared: Arc::new(ServerShared {
                application,
                state: RwLock::new(Freezable::new(ServerState::Startup(ServerStateStartup {
                    wipe_catalog_on_error: config.wipe_catalog_on_error,
                    skip_replay_and_seek_instead: config.skip_replay_and_seek_instead,
                }))),
            }),
        }
    }

    /// sets the id of the server, which is used for replication and the base
    /// path in object storage.
    ///
    /// A valid server ID Must be non-zero.
    pub fn set_id(&self, server_id: ServerId) -> Result<()> {
        let mut state = self.shared.state.write();
        let startup = match &**state {
            ServerState::Startup(startup) => startup.clone(),
            _ => return Err(Error::IdAlreadySet),
        };

        *state.get_mut().expect("transaction in progress") =
            ServerState::InitReady(ServerStateInitReady {
                wipe_catalog_on_error: startup.wipe_catalog_on_error,
                skip_replay_and_seek_instead: startup.skip_replay_and_seek_instead,
                server_id,
            });

        Ok(())
    }

    /// Return the metrics associated with this server
    ///
    /// TODO: Server should record its own metrics
    pub fn metrics(&self) -> &Arc<ServerMetrics> {
        &self.metrics
    }

    /// Returns the server id for this server if set
    pub fn server_id(&self) -> Option<ServerId> {
        self.shared.state.read().server_id()
    }

    /// Returns true if the server is initialized
    ///
    /// NB: not all databases may be initialized
    pub fn initialized(&self) -> bool {
        self.shared.state.read().initialized().is_ok()
    }

    /// Error occurred during generic server init (e.g. listing store content).
    pub fn server_init_error(&self) -> Option<Arc<InitError>> {
        match &**self.shared.state.read() {
            ServerState::InitError(_, e) => Some(Arc::clone(e)),
            _ => None,
        }
    }

    /// Returns a list of `Database` for this `Server` in no particular order
    pub fn databases(&self) -> Result<Vec<Arc<Database>>> {
        let state = self.shared.state.read();
        let initialized = state.initialized()?;
        Ok(initialized.databases.values().cloned().collect())
    }

    /// Get the `Database` by name
    pub fn database(&self, db_name: &DatabaseName<'_>) -> Result<Arc<Database>> {
        let state = self.shared.state.read();
        let initialized = state.initialized()?;
        let db = initialized
            .databases
            .get(db_name)
            .ok_or(Error::DatabaseNotFound {
                db_name: db_name.to_string(),
            })?;

        Ok(Arc::clone(db))
    }

    /// Returns an initialized `Db` by name
    pub fn db(&self, name: &DatabaseName<'_>) -> Result<Arc<Db>> {
        self.database(name)?
            .initialized_db()
            .ok_or(Error::DatabaseNotInitialized {
                db_name: name.to_string(),
            })
    }

    /// Tells the server the set of rules for a database.
    pub async fn create_database(&self, rules: DatabaseRules) -> Result<()> {
        let db_name = rules.name.clone();
        let object_store = self.shared.application.object_store().as_ref();

        let (server_id, create_database_lock) = {
            let state = self.shared.state.read();
            let initialized = state.initialized()?;

            if initialized.databases.contains_key(&rules.name) {
                return Err(Error::DatabaseAlreadyExists {
                    db_name: db_name.to_string(),
                });
            }
            (
                initialized.server_id,
                Arc::clone(&initialized.create_database_lock),
            )
        };

        // Prevent concurrent database creation
        let guard = create_database_lock.lock().await;

        let store_prefix = database_store_prefix(object_store, server_id, &db_name);
        persist_database_rules(object_store, &store_prefix, rules).await?;

        create_preserved_catalog(
            db_name.as_str(),
            Arc::clone(self.shared.application.object_store()),
            server_id,
            Arc::clone(self.shared.application.metric_registry()),
        )
        .await
        .map_err(|e| Box::new(e) as _)
        .context(CannotCreatePreservedCatalog)?;

        let database = {
            let mut state = self.shared.state.write();

            // Have exclusive lock on state - can drop database creation lock
            std::mem::drop(guard);

            let state = state.get_mut().expect("no transaction in progress");
            let database = match state {
                ServerState::Initialized(initialized) => {
                    match initialized.databases.entry(db_name.clone()) {
                        hashbrown::hash_map::Entry::Vacant(vacant) => {
                            vacant.insert(Arc::new(Database::new(
                                Arc::clone(&self.shared.application),
                                DatabaseConfig {
                                    name: db_name,
                                    server_id,
                                    store_prefix,
                                    wipe_catalog_on_error: false,
                                    skip_replay: false,
                                },
                            )))
                        }
                        hashbrown::hash_map::Entry::Occupied(_) => unreachable!(),
                    }
                }
                _ => unreachable!(),
            };
            Arc::clone(database)
        };

        database.wait_for_init().await.context(DatabaseInit)?;

        Ok(())
    }

    /// Loads the database configurations based on the databases in the
    /// object store. Any databases in the config already won't be
    /// replaced.
    ///
    /// This requires the serverID to be set.
    ///
    /// It will be a no-op if the configs are already loaded and the server is ready.
    ///
    /// TODO: Make server own background loop and make this private
    pub async fn maybe_initialize_server(&self) {
        if self.initialized() {
            return;
        }

        let (init_ready, handle) = {
            let mut state = self.shared.state.write();

            let init_ready = match &**state {
                ServerState::Startup(_) => {
                    info!("server not initialized - ID not set");
                    return;
                }
                ServerState::InitReady(state) => {
                    info!(server_id=%state.server_id, "server init ready");
                    state.clone()
                }
                ServerState::InitError(state, e) => {
                    info!(server_id=%state.server_id, %e, "retrying server init");
                    state.clone()
                }
                ServerState::Initialized(_) => return,
            };

            let handle = match state.try_freeze() {
                Some(handle) => handle,
                None => return,
            };

            (init_ready, handle)
        };

        let maybe_databases = list_databases(
            self.shared.application.object_store().as_ref(),
            init_ready.server_id,
        )
        .await;

        let next_state = match maybe_databases {
            Ok(databases) => {
                let databases = databases
                    .into_iter()
                    .map(|(db_name, store_prefix)| {
                        (
                            db_name.clone(),
                            Arc::new(Database::new(
                                Arc::clone(&self.shared.application),
                                DatabaseConfig {
                                    name: db_name,
                                    server_id: init_ready.server_id,
                                    store_prefix,
                                    wipe_catalog_on_error: init_ready.wipe_catalog_on_error,
                                    skip_replay: init_ready.skip_replay_and_seek_instead,
                                },
                            )),
                        )
                    })
                    .collect();

                info!(server_id=%init_ready.server_id, "server initialized");

                ServerState::Initialized(ServerStateInitialized {
                    server_id: init_ready.server_id,
                    create_database_lock: Default::default(),
                    databases,
                })
            }
            Err(e) => {
                error!(server_id=%init_ready.server_id, %e, "error initializing server");
                ServerState::InitError(init_ready, Arc::new(e))
            }
        };

        *self.shared.state.write().unfreeze(handle) = next_state;
    }

    pub async fn write_pb(&self, database_batch: pb::DatabaseBatch) -> Result<()> {
        let db_name = DatabaseName::new(database_batch.database_name.as_str())
            .context(InvalidDatabaseName)?;

        let entry = pb_to_entry(&database_batch).context(PBConversion)?;

        // TODO: Apply routing/sharding logic (#2139)
        self.write_entry_local(&db_name, entry).await?;

        Ok(())
    }

    /// `write_lines` takes in raw line protocol and converts it to a collection
    /// of ShardedEntry which are then sent to other IOx servers based on
    /// the ShardConfig or sent to the local database for buffering in the
    /// WriteBuffer and/or the MutableBuffer if configured.
    ///
    /// The provided `default_time` is nanoseconds since the epoch and will be assigned
    /// to any lines that don't have a timestamp.
    ///
    /// TODO: Move this routing logic into a subsystem owned by `Database`
    pub async fn write_lines(
        &self,
        db_name: &DatabaseName<'_>,
        lines: &[ParsedLine<'_>],
        default_time: i64,
    ) -> Result<()> {
        let db = self.db(db_name)?;

        // need to split this in two blocks because we cannot hold a lock across an async call.
        let routing_config_target = {
            let rules = db.rules();
            if let Some(RoutingRules::RoutingConfig(routing_config)) = &rules.routing_rules {
                let sharded_entries = lines_to_sharded_entries(
                    lines,
                    default_time,
                    None as Option<&ShardConfig>,
                    &*rules,
                )
                .context(LineConversion)?;
                Some((routing_config.sink.clone(), sharded_entries))
            } else {
                None
            }
        };

        if let Some((sink, sharded_entries)) = routing_config_target {
            for i in sharded_entries {
                self.write_entry_sink(db_name, &sink, i.entry).await?;
            }
            return Ok(());
        }

        // Split lines into shards while holding a read lock on the sharding config.
        // Once the lock is released we have a vector of entries, each associated with a
        // shard id, and an Arc to the mapping between shard ids and node
        // groups. This map is atomically replaced every time the sharding
        // config is updated, hence it's safe to use after we release the shard config
        // lock.
        let (sharded_entries, shards) = {
            let rules = db.rules();

            let shard_config = rules.routing_rules.as_ref().map(|cfg| match cfg {
                RoutingRules::RoutingConfig(_) => unreachable!("routing config handled above"),
                RoutingRules::ShardConfig(shard_config) => shard_config,
            });

            let sharded_entries =
                lines_to_sharded_entries(lines, default_time, shard_config, &*rules)
                    .context(LineConversion)?;

            let shards = shard_config
                .as_ref()
                .map(|cfg| Arc::clone(&cfg.shards))
                .unwrap_or_default();

            (sharded_entries, shards)
        };

        // Write to all shards in parallel; as soon as one fails return error
        // immediately to the client and abort all other outstanding requests.
        // This can take some time, but we're no longer holding the lock to the shard
        // config.
        futures_util::future::try_join_all(
            sharded_entries
                .into_iter()
                .map(|e| self.write_sharded_entry(db_name, Arc::clone(&shards), e)),
        )
        .await?;

        Ok(())
    }

    async fn write_sharded_entry(
        &self,
        db_name: &DatabaseName<'_>,
        shards: Arc<std::collections::HashMap<u32, Sink>>,
        sharded_entry: ShardedEntry,
    ) -> Result<()> {
        match sharded_entry.shard_id {
            Some(shard_id) => {
                let sink = shards.get(&shard_id).context(ShardNotFound { shard_id })?;
                self.write_entry_sink(db_name, sink, sharded_entry.entry)
                    .await?
            }
            None => self.write_entry_local(db_name, sharded_entry.entry).await?,
        }
        Ok(())
    }

    async fn write_entry_sink(
        &self,
        db_name: &DatabaseName<'_>,
        sink: &Sink,
        entry: Entry,
    ) -> Result<()> {
        match sink {
            Sink::Iox(node_group) => {
                self.write_entry_downstream(db_name, node_group, entry)
                    .await
            }
            Sink::Kafka(_) => {
                // The write buffer write path is currently implemented in "db", so confusingly we
                // need to invoke write_entry_local.
                // TODO(mkm): tracked in #2134
                self.write_entry_local(db_name, entry).await
            }
            Sink::DevNull => {
                // write is silently ignored, as requested by the configuration.
                Ok(())
            }
        }
    }

    async fn write_entry_downstream(
        &self,
        db_name: &str,
        node_group: &[ServerId],
        entry: Entry,
    ) -> Result<()> {
        // Return an error if this server is not yet ready
        self.shared.state.read().initialized()?;

        let addrs: Vec<_> = {
            let resolver = self.resolver.read();
            node_group
                .iter()
                .filter_map(|&node| resolver.resolve_remote(node))
                .collect()
        };

        if addrs.is_empty() {
            return NoRemoteConfigured { node_group }.fail();
        }

        let mut errors = HashMap::new();
        // this needs to be in its own statement because rand::thread_rng is not Send and the loop below is async.
        // braces around the expression would work but clippy don't know that and complains the braces are useless.
        let random_addrs_iter = addrs.choose_multiple(&mut rand::thread_rng(), addrs.len());
        for addr in random_addrs_iter {
            match self.connection_manager.remote_server(addr).await {
                Err(err) => {
                    info!("error obtaining remote for {}: {}", addr, err);
                    errors.insert(addr.to_owned(), err);
                }
                Ok(remote) => {
                    return remote
                        .write_entry(db_name, entry)
                        .await
                        .context(RemoteError)
                }
            };
        }
        NoRemoteReachable { errors }.fail()
    }

    /// Write an entry to the local `Db`
    ///
    /// TODO: Push this out of `Server` into `Database`
    pub async fn write_entry_local(&self, db_name: &DatabaseName<'_>, entry: Entry) -> Result<()> {
        let db = self.db(db_name)?;
        let bytes = entry.data().len() as u64;
        db.store_entry(entry).await.map_err(|e| {
            self.metrics.ingest_entries_bytes_total.add_with_labels(
                bytes,
                &[
                    metrics::KeyValue::new("status", "error"),
                    metrics::KeyValue::new("db_name", db_name.to_string()),
                ],
            );
            match e {
                db::Error::HardLimitReached {} => Error::HardLimitReached {},
                db::Error::WritingOnlyAllowedThroughWriteBuffer {} => {
                    Error::WritingOnlyAllowedThroughWriteBuffer {
                        db_name: db_name.into(),
                    }
                }
                db::Error::WriteBufferWritingError { .. } => {
                    Error::WriteBuffer { source: e, bytes }
                }
                _ => Error::UnknownDatabaseError {
                    source: Box::new(e),
                },
            }
        })?;

        self.metrics.ingest_entries_bytes_total.add_with_labels(
            bytes,
            &[
                metrics::KeyValue::new("status", "ok"),
                metrics::KeyValue::new("db_name", db_name.to_string()),
            ],
        );

        Ok(())
    }

    /// Update database rules and save on success.
    pub async fn update_db_rules<F, E>(
        &self,
        db_name: &DatabaseName<'_>,
        update: F,
    ) -> std::result::Result<Arc<DatabaseRules>, UpdateError<E>>
    where
        F: FnOnce(DatabaseRules) -> Result<DatabaseRules, E> + Send,
    {
        // TODO: Move into Database (#2053)
        let database = self.database(db_name)?;
        let db = database
            .initialized_db()
            .ok_or(Error::DatabaseNotInitialized {
                db_name: db_name.to_string(),
            })?;

        let rules = db.update_rules(update).map_err(UpdateError::Closure)?;

        // TODO: Handle failure
        persist_database_rules(
            self.shared.application.object_store().as_ref(),
            &database.config().store_prefix,
            rules.as_ref().clone(),
        )
        .await?;
        Ok(rules)
    }

    pub fn remotes_sorted(&self) -> Vec<(ServerId, String)> {
        self.resolver.read().remotes_sorted()
    }

    pub fn update_remote(&self, id: ServerId, addr: GrpcConnectionString) {
        self.resolver.write().update_remote(id, addr)
    }

    pub fn delete_remote(&self, id: ServerId) -> Option<GrpcConnectionString> {
        self.resolver.write().delete_remote(id)
    }

    /// Closes a chunk and starts moving its data to the read buffer, as a
    /// background job, dropping when complete.
    pub fn close_chunk(
        &self,
        db_name: &DatabaseName<'_>,
        table_name: impl Into<String>,
        partition_key: impl Into<String>,
        chunk_id: u32,
    ) -> Result<TaskTracker<Job>> {
        let db = self.db(db_name)?;
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        let chunk = db
            .lockable_chunk(&table_name, &partition_key, chunk_id)
            .context(ChunkNotFound)?;

        let guard = chunk.write();

        LockableChunk::move_to_read_buffer(guard).map_err(|e| Error::UnknownDatabaseError {
            source: Box::new(e),
        })
    }

    /// Recover database that has failed to load its catalog by wiping it
    ///
    /// The DB must exist in the server and have failed to load the catalog for this to work
    /// This is done to prevent race conditions between DB jobs and this command
    pub fn wipe_preserved_catalog(
        &self,
        db_name: &DatabaseName<'static>,
    ) -> Result<TaskTracker<Job>> {
        let database = self.database(db_name)?;
        let registry = self.shared.application.job_registry();

        let (tracker, registration) = registry.register(Job::WipePreservedCatalog {
            db_name: Arc::from(db_name.as_str()),
        });

        let fut = database
            .wipe_preserved_catalog()
            .context(WipePreservedCatalog)?;

        let _ = tokio::spawn(fut.track(registration));

        Ok(tracker)
    }

    /// Background worker function for the server
    ///
    /// TODO: Server should own its background thread
    pub async fn background_worker(&self, shutdown: tokio_util::sync::CancellationToken) {
        info!("started background worker");

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

        // TODO: Move out of Server background loop
        let job_registry = self.shared.application.job_registry();

        while !shutdown.is_cancelled() {
            self.maybe_initialize_server().await;
            job_registry.reclaim();

            // TODO: Trigger termination if database worker finishes

            tokio::select! {
                _ = interval.tick() => {},
                _ = shutdown.cancelled() => break
            }
        }

        info!("shutting down background workers");
        if let Ok(databases) = self.databases() {
            for database in databases {
                database.shutdown();
                let _ = database.join().await.log_if_error("database worker");
            }
        }

        info!("draining tracker registry");

        // Wait for any outstanding jobs to finish - frontend shutdown should be
        // sequenced before shutting down the background workers and so there
        // shouldn't be any
        while job_registry.reclaim() != 0 {
            interval.tick().await;
        }

        info!("drained tracker registry");
    }
}

/// TODO: Revisit this trait's API
#[async_trait]
impl<M> DatabaseStore for Server<M>
where
    M: ConnectionManager + std::fmt::Debug + Send + Sync,
{
    type Database = Db;
    type Error = Error;

    fn db_names_sorted(&self) -> Vec<String> {
        self.shared
            .state
            .read()
            .initialized()
            .map(|initialized| {
                let mut keys: Vec<_> = initialized
                    .databases
                    .keys()
                    .map(ToString::to_string)
                    .collect();

                keys.sort_unstable();
                keys
            })
            .unwrap_or_default()
    }

    fn db(&self, name: &str) -> Option<Arc<Self::Database>> {
        DatabaseName::new(name)
            .ok()
            .and_then(|name| self.db(&name).ok())
    }

    // TODO: refactor usages of this to use the Server rather than this trait and to
    //       explicitly create a database.
    async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error> {
        let db_name = DatabaseName::new(name.to_string()).context(InvalidDatabaseName)?;

        let db = match self.db(&db_name) {
            Ok(db) => db,
            Err(Error::DatabaseNotFound { .. }) => {
                self.create_database(DatabaseRules::new(db_name.clone()))
                    .await?;
                self.db(&db_name).expect("db not inserted")
            }
            Err(e) => return Err(e),
        };

        Ok(db)
    }

    /// Return a handle to the query executor
    fn executor(&self) -> Arc<Executor> {
        Arc::clone(self.shared.application.executor())
    }
}

pub(crate) const DB_RULES_FILE_NAME: &str = "rules.pb";

/// Returns a list of database names and their prefix in object storage
async fn list_databases(
    object_store: &ObjectStore,
    server_id: ServerId,
) -> Result<Vec<(DatabaseName<'static>, Path)>, InitError> {
    let mut path = object_store.new_path();
    path.push_dir(server_id.to_string());

    let list_result = object_store
        .list_with_delimiter(&path)
        .await
        .context(ListRules)?;

    Ok(list_result
        .common_prefixes
        .into_iter()
        .filter_map(|path| {
            let path_parsed: DirsAndFileName = path.clone().into();
            let last = path_parsed.directories.last().expect("path can't be empty");
            let db_name = DatabaseName::new(last.encoded().to_string())
                .log_if_error("invalid database directory")
                .ok()?;

            Some((db_name, path))
        })
        .collect())
}

/// Returns the path to the prefix of a `Database` in object storage
fn database_store_prefix(
    object_store: &ObjectStore,
    server_id: ServerId,
    db_name: &DatabaseName<'_>,
) -> Path {
    // TODO: Use fresh path for database (i.e. add generation) (#1881)
    let mut path = object_store.new_path();
    path.push_dir(server_id.to_string());
    path.push_dir(db_name);
    path
}

/// Persist the the `DatabaseRules` given the `Database` store prefix
async fn persist_database_rules(
    object_store: &ObjectStore,
    store_prefix: &Path,
    rules: DatabaseRules,
) -> Result<()> {
    let mut data = BytesMut::new();
    encode_database_rules(rules, &mut data).context(ErrorSerializingRulesProtobuf)?;

    let mut location = store_prefix.clone();
    location.set_file_name(DB_RULES_FILE_NAME);

    let len = data.len();

    let stream_data = std::io::Result::Ok(data.freeze());
    object_store
        .put(
            &location,
            futures::stream::once(async move { stream_data }),
            Some(len),
        )
        .await
        .context(StoreError)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        convert::{Infallible, TryFrom},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };

    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use arrow_util::assert_batches_eq;
    use connection::test_helpers::{TestConnectionManager, TestRemoteServer};
    use data_types::chunk_metadata::ChunkAddr;
    use data_types::database_rules::{
        HashRing, LifecycleRules, PartitionTemplate, ShardConfig, TemplatePart, NO_SHARD_CONFIG,
    };
    use generated_types::database_rules::decode_database_rules;
    use influxdb_line_protocol::parse_lines;
    use metrics::TestMetricRegistry;
    use object_store::{path::ObjectStorePath, ObjectStore};
    use parquet_file::catalog::{test_helpers::TestCatalogState, PreservedCatalog};
    use query::{exec::ExecutorType, frontend::sql::SqlQueryPlanner, QueryDatabase};

    use super::*;

    const ARBITRARY_DEFAULT_TIME: i64 = 456;

    fn make_application() -> Arc<ApplicationState> {
        Arc::new(ApplicationState::new(
            Arc::new(ObjectStore::new_in_memory()),
            None,
        ))
    }

    fn make_server(application: Arc<ApplicationState>) -> Arc<Server<TestConnectionManager>> {
        Arc::new(Server::new(
            TestConnectionManager::new(),
            application,
            Default::default(),
        ))
    }

    #[tokio::test]
    async fn server_api_calls_return_error_with_no_id_set() {
        let server = make_server(make_application());

        let lines = parsed_lines("cpu foo=1 10");
        let resp = server
            .write_lines(
                &DatabaseName::new("foo").unwrap(),
                &lines,
                ARBITRARY_DEFAULT_TIME,
            )
            .await
            .unwrap_err();
        assert!(matches!(resp, Error::IdNotSet));
    }

    #[tokio::test]
    async fn create_database_persists_rules() {
        let application = make_application();
        let server = make_server(Arc::clone(&application));
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let name = DatabaseName::new("bananas").unwrap();

        let rules = DatabaseRules {
            name: name.clone(),
            partition_template: PartitionTemplate {
                parts: vec![TemplatePart::TimeFormat("YYYY-MM".to_string())],
            },
            lifecycle_rules: LifecycleRules {
                catalog_transactions_until_checkpoint: std::num::NonZeroU64::new(13).unwrap(),
                ..Default::default()
            },
            routing_rules: None,
            worker_cleanup_avg_sleep: Duration::from_secs(2),
            write_buffer_connection: None,
        };

        // Create a database
        server
            .create_database(rules.clone())
            .await
            .expect("failed to create database");

        let mut rules_path = application.object_store().new_path();
        rules_path.push_all_dirs(&["1", name.as_str()]);
        rules_path.set_file_name("rules.pb");

        let read_data = application
            .object_store()
            .get(&rules_path)
            .await
            .unwrap()
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap()
            .freeze();

        let read_rules = decode_database_rules(read_data).unwrap();

        assert_eq!(rules, read_rules);

        let db2 = DatabaseName::new("db_awesome").unwrap();
        server
            .create_database(DatabaseRules::new(db2.clone()))
            .await
            .expect("failed to create 2nd db");

        application
            .object_store()
            .list_with_delimiter(&application.object_store().new_path())
            .await
            .unwrap();

        let server2 = make_server(application);
        server2.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server2.maybe_initialize_server().await;

        let database1 = server2.database(&name).unwrap();
        let database2 = server2.database(&db2).unwrap();

        database1.wait_for_init().await.unwrap();
        database2.wait_for_init().await.unwrap();

        assert!(server2.db(&db2).is_ok());
        assert!(server2.db(&name).is_ok());
    }

    #[tokio::test]
    async fn duplicate_database_name_rejected() {
        // Covers #643

        let server = make_server(make_application());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let name = DatabaseName::new("bananas").unwrap();

        // Create a database
        server
            .create_database(DatabaseRules::new(name.clone()))
            .await
            .expect("failed to create database");

        // Then try and create another with the same name
        let got = server
            .create_database(DatabaseRules::new(name.clone()))
            .await
            .unwrap_err();

        if !matches!(got, Error::DatabaseAlreadyExists { .. }) {
            panic!("expected already exists error");
        }
    }

    async fn create_simple_database<M>(
        server: &Server<M>,
        name: impl Into<String> + Send,
    ) -> Result<()>
    where
        M: ConnectionManager + Send + Sync,
    {
        let name = DatabaseName::new(name.into()).unwrap();

        let rules = DatabaseRules {
            name,
            partition_template: PartitionTemplate {
                parts: vec![TemplatePart::TimeFormat("YYYY-MM".to_string())],
            },
            lifecycle_rules: Default::default(),
            routing_rules: None,
            worker_cleanup_avg_sleep: Duration::from_secs(2),
            write_buffer_connection: None,
        };

        // Create a database
        server.create_database(rules.clone()).await
    }

    #[tokio::test]
    async fn load_databases() {
        let application = make_application();
        let store = application.object_store();

        let server = make_server(Arc::clone(&application));
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;
        create_simple_database(&server, "bananas")
            .await
            .expect("failed to create database");

        let mut rules_path = store.new_path();
        rules_path.push_all_dirs(&["1", "bananas"]);
        rules_path.set_file_name("rules.pb");

        std::mem::drop(server);

        let server = make_server(Arc::clone(&application));
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        create_simple_database(&server, "apples")
            .await
            .expect("failed to create database");

        assert_eq!(server.db_names_sorted(), vec!["apples", "bananas"]);

        std::mem::drop(server);

        store
            .delete(&rules_path)
            .await
            .expect("cannot delete rules file");

        let server = make_server(application);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        assert_eq!(server.db_names_sorted(), vec!["apples", "bananas"]);

        let bananas = DatabaseName::new("bananas").unwrap();
        let apples = DatabaseName::new("apples").unwrap();

        let apples_database = server.database(&apples).unwrap();
        let bananas_database = server.database(&bananas).unwrap();

        apples_database.wait_for_init().await.unwrap();
        let err = bananas_database.wait_for_init().await.unwrap_err();

        assert!(apples_database.init_error().is_none());
        assert!(matches!(
            err.as_ref(),
            database::InitError::RulesFetch { .. }
        ));
        assert!(Arc::ptr_eq(&err, &bananas_database.init_error().unwrap()));
    }

    #[tokio::test]
    async fn db_names_sorted() {
        let server = make_server(make_application());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let names = vec!["bar", "baz"];

        for name in &names {
            let name = DatabaseName::new(name.to_string()).unwrap();
            server
                .create_database(DatabaseRules::new(name))
                .await
                .expect("failed to create database");
        }

        let db_names_sorted = server.db_names_sorted();
        assert_eq!(names, db_names_sorted);
    }

    #[tokio::test]
    async fn writes_local() {
        let server = make_server(make_application());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let db_name = DatabaseName::new("foo".to_string()).unwrap();
        server
            .create_database(DatabaseRules::new(db_name.clone()))
            .await
            .unwrap();

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server
            .write_lines(&db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        let db = server.db(&db_name).unwrap();
        let batches = run_query(db, "select * from cpu").await;

        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn write_entry_local() {
        let application = make_application();
        let metric_registry = TestMetricRegistry::new(Arc::clone(application.metric_registry()));
        let server = make_server(application);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let name = DatabaseName::new("foo".to_string()).unwrap();
        server
            .create_database(DatabaseRules::new(name.clone()))
            .await
            .unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        let db = server.db(&db_name).unwrap();
        let rules = db.rules();

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        let sharded_entries = lines_to_sharded_entries(
            &lines,
            ARBITRARY_DEFAULT_TIME,
            NO_SHARD_CONFIG,
            rules.as_ref(),
        )
        .expect("sharded entries");

        let entry = &sharded_entries[0].entry;
        server
            .write_entry_local(&name, entry.clone())
            .await
            .expect("write entry");

        let batches = run_query(db, "select * from cpu").await;
        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);

        metric_registry
            .has_metric_family("ingest_entries_bytes_total")
            .with_labels(&[("status", "ok"), ("db_name", "foo")])
            .counter()
            .eq(240.0)
            .unwrap();
    }

    // This tests sets up a database with a sharding config which defines exactly one shard
    // backed by 3 remote nodes. One of the nodes is modeled to be "down", while the other two
    // can record write entry events.
    // This tests goes through a few trivial error cases before checking that the both working
    // mock remote servers actually receive write entry events.
    //
    // This test is theoretically flaky, low probability though (in the order of 1e-30)
    #[tokio::test]
    async fn write_entry_downstream() {
        const TEST_SHARD_ID: ShardId = 1;
        const GOOD_REMOTE_ADDR_1: &str = "http://localhost:111";
        const GOOD_REMOTE_ADDR_2: &str = "http://localhost:222";
        const BAD_REMOTE_ADDR: &str = "http://localhost:666";

        let good_remote_id_1 = ServerId::try_from(1).unwrap();
        let good_remote_id_2 = ServerId::try_from(2).unwrap();
        let bad_remote_id = ServerId::try_from(666).unwrap();

        let mut manager = TestConnectionManager::new();
        let written_1 = Arc::new(AtomicBool::new(false));
        manager.remotes.insert(
            GOOD_REMOTE_ADDR_1.to_owned(),
            Arc::new(TestRemoteServer {
                written: Arc::clone(&written_1),
            }),
        );
        let written_2 = Arc::new(AtomicBool::new(false));
        manager.remotes.insert(
            GOOD_REMOTE_ADDR_2.to_owned(),
            Arc::new(TestRemoteServer {
                written: Arc::clone(&written_2),
            }),
        );

        let server = Server::new(manager, make_application(), Default::default());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let db_name = DatabaseName::new("foo").unwrap();
        server
            .create_database(DatabaseRules::new(db_name.clone()))
            .await
            .unwrap();

        let remote_ids = vec![bad_remote_id, good_remote_id_1, good_remote_id_2];
        let db = server.db(&db_name).unwrap();
        db.update_rules(|mut rules| {
            let shard_config = ShardConfig {
                hash_ring: Some(HashRing {
                    shards: vec![TEST_SHARD_ID].into(),
                    ..Default::default()
                }),
                shards: Arc::new(
                    vec![(TEST_SHARD_ID, Sink::Iox(remote_ids.clone()))]
                        .into_iter()
                        .collect(),
                ),
                ..Default::default()
            };
            rules.routing_rules = Some(RoutingRules::ShardConfig(shard_config));
            Ok::<_, Infallible>(rules)
        })
        .unwrap();

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();

        let err = server
            .write_lines(&db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::NoRemoteConfigured { node_group } if node_group == remote_ids)
        );

        // one remote is configured but it's down and we'll get connection error
        server.update_remote(bad_remote_id, BAD_REMOTE_ADDR.into());
        let err = server
            .write_lines(&db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::NoRemoteReachable { errors } if matches!(
                errors[BAD_REMOTE_ADDR],
                connection::ConnectionManagerError::RemoteServerConnectError {..}
            )
        ));
        assert!(!written_1.load(Ordering::Relaxed));
        assert!(!written_2.load(Ordering::Relaxed));

        // We configure the address for the other remote, this time connection will succeed
        // despite the bad remote failing to connect.
        server.update_remote(good_remote_id_1, GOOD_REMOTE_ADDR_1.into());
        server.update_remote(good_remote_id_2, GOOD_REMOTE_ADDR_2.into());

        // Remotes are tried in random order, so we need to repeat the test a few times to have a reasonable
        // probability both the remotes will get hit.
        for _ in 0..100 {
            server
                .write_lines(&db_name, &lines, ARBITRARY_DEFAULT_TIME)
                .await
                .expect("cannot write lines");
        }
        assert!(written_1.load(Ordering::Relaxed));
        assert!(written_2.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn close_chunk() {
        test_helpers::maybe_start_logging();
        let server = make_server(make_application());

        let cancel_token = CancellationToken::new();
        let background_handle = spawn_worker(Arc::clone(&server), cancel_token.clone());

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let db_name = DatabaseName::new("foo").unwrap();
        server
            .create_database(DatabaseRules::new(db_name.clone()))
            .await
            .unwrap();

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server
            .write_lines(&db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap();

        // start the close (note this is not an async)
        let chunk_addr = ChunkAddr {
            db_name: Arc::from(db_name.as_str()),
            table_name: Arc::from("cpu"),
            partition_key: Arc::from(""),
            chunk_id: 0,
        };
        let tracker = server
            .close_chunk(
                &db_name,
                chunk_addr.table_name.as_ref(),
                chunk_addr.partition_key.as_ref(),
                chunk_addr.chunk_id,
            )
            .unwrap();

        let metadata = tracker.metadata();
        let expected_metadata = Job::CompactChunk {
            chunk: chunk_addr.clone(),
        };
        assert_eq!(metadata, &expected_metadata);

        // wait for the job to complete
        tracker.join().await;

        // Data should be in the read buffer and not in mutable buffer
        let db_name = DatabaseName::new("foo").unwrap();
        let db = server.db(&db_name).unwrap();

        let mut chunk_summaries = db.chunk_summaries().unwrap();
        chunk_summaries.sort_unstable();

        let actual = chunk_summaries
            .into_iter()
            .map(|s| format!("{:?} {}", s.storage, s.id))
            .collect::<Vec<_>>();

        let expected = vec!["ReadBuffer 0"];

        assert_eq!(
            expected, actual,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, actual
        );

        // ensure that we don't leave the server instance hanging around
        cancel_token.cancel();
        let _ = background_handle.await;
    }

    #[tokio::test]
    async fn background_task_cleans_jobs() {
        let application = make_application();
        let server = make_server(Arc::clone(&application));

        let cancel_token = CancellationToken::new();
        let background_handle = spawn_worker(Arc::clone(&server), cancel_token.clone());

        let wait_nanos = 1000;
        let job = application.job_registry().spawn_dummy_job(vec![wait_nanos]);

        // Note: this will hang forever if the background task has not been started
        job.join().await;

        assert!(job.is_complete());

        // ensure that we don't leave the server instance hanging around
        cancel_token.cancel();
        let _ = background_handle.await;
    }

    fn parsed_lines(lp: &str) -> Vec<ParsedLine<'_>> {
        parse_lines(lp).map(|l| l.unwrap()).collect()
    }

    fn spawn_worker<M>(server: Arc<Server<M>>, token: CancellationToken) -> JoinHandle<()>
    where
        M: ConnectionManager + Send + Sync + 'static,
    {
        tokio::task::spawn(async move { server.background_worker(token).await })
    }

    #[tokio::test]
    async fn hard_buffer_limit() {
        let server = make_server(make_application());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let name = DatabaseName::new("foo".to_string()).unwrap();
        server
            .create_database(DatabaseRules::new(name.clone()))
            .await
            .unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        let db = server.db(&db_name).unwrap();
        let rules = db
            .update_rules(|mut rules| {
                rules.lifecycle_rules.buffer_size_hard =
                    Some(std::num::NonZeroUsize::new(10).unwrap());
                Ok::<_, Infallible>(rules)
            })
            .unwrap();

        // inserting first line does not trigger hard buffer limit
        let line_1 = "cpu bar=1 10";
        let lines_1: Vec<_> = parse_lines(line_1).map(|l| l.unwrap()).collect();
        let sharded_entries_1 =
            lines_to_sharded_entries(&lines_1, ARBITRARY_DEFAULT_TIME, NO_SHARD_CONFIG, &*rules)
                .expect("first sharded entries");

        let entry_1 = &sharded_entries_1[0].entry;
        server
            .write_entry_local(&name, entry_1.clone())
            .await
            .expect("write first entry");

        // inserting second line will
        let line_2 = "cpu bar=2 20";
        let lines_2: Vec<_> = parse_lines(line_2).map(|l| l.unwrap()).collect();
        let sharded_entries_2 = lines_to_sharded_entries(
            &lines_2,
            ARBITRARY_DEFAULT_TIME,
            NO_SHARD_CONFIG,
            rules.as_ref(),
        )
        .expect("second sharded entries");
        let entry_2 = &sharded_entries_2[0].entry;
        let res = server.write_entry_local(&name, entry_2.clone()).await;
        assert!(matches!(res, Err(super::Error::HardLimitReached {})));
    }

    #[tokio::test]
    async fn cannot_create_db_until_server_is_initialized() {
        let server = make_server(make_application());

        // calling before serverID set leads to `IdNotSet`
        let err = create_simple_database(&server, "bananas")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::IdNotSet));

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        // do NOT call `server.maybe_load_database_configs` so DBs are not loaded and server is not ready

        // calling with serverId but before loading is done leads to
        let err = create_simple_database(&server, "bananas")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ServerNotInitialized { .. }));
    }

    #[tokio::test]
    async fn background_worker_eventually_inits_server() {
        let server = make_server(make_application());

        let cancel_token = CancellationToken::new();
        let background_handle = spawn_worker(Arc::clone(&server), cancel_token.clone());

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();

        let t_0 = Instant::now();
        loop {
            if server.initialized() {
                break;
            }
            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // ensure that we don't leave the server instance hanging around
        cancel_token.cancel();
        let _ = background_handle.await;
    }

    #[tokio::test]
    async fn init_error_generic() {
        // use an object store that will hopefully fail to read
        let store = Arc::new(ObjectStore::new_failing_store().unwrap());
        let application = Arc::new(ApplicationState::new(store, None));
        let server = make_server(application);

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;
        assert!(dbg!(server.server_init_error().unwrap().to_string())
            .starts_with("error listing databases in object storage:"));
    }

    #[tokio::test]
    async fn init_error_database() {
        let application = make_application();
        let store = Arc::clone(application.object_store());
        let server_id = ServerId::try_from(1).unwrap();

        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.maybe_initialize_server().await;

        let foo_db_name = DatabaseName::new("foo").unwrap();
        let bar_db_name = DatabaseName::new("bar").unwrap();

        create_simple_database(&server, "foo")
            .await
            .expect("failed to create database");

        // tamper store
        let mut path = database_store_prefix(store.as_ref(), server_id, &bar_db_name);
        path.set_file_name(DB_RULES_FILE_NAME);

        let data = Bytes::from("x");
        let len = data.len();
        store
            .put(
                &path,
                futures::stream::once(async move { Ok(data) }),
                Some(len),
            )
            .await
            .unwrap();

        store.get(&path).await.unwrap();

        // start server
        let server = make_server(application);
        server.set_id(server_id).unwrap();
        server.maybe_initialize_server().await;

        // generic error MUST NOT be set
        assert!(server.server_init_error().is_none());

        // server is initialized
        assert!(server.initialized());

        // DB names contain all DBs
        assert_eq!(
            server.db_names_sorted(),
            vec!["bar".to_string(), "foo".to_string()]
        );

        let foo_database = server.database(&foo_db_name).unwrap();
        let bar_database = server.database(&bar_db_name).unwrap();

        foo_database.wait_for_init().await.unwrap();
        assert!(foo_database.init_error().is_none());

        let err = bar_database.wait_for_init().await.unwrap_err();
        assert!(matches!(
            err.as_ref(),
            database::InitError::RulesDecode { .. }
        ));
        assert!(Arc::ptr_eq(&err, &bar_database.init_error().unwrap()));

        // can only write to successfully created DBs
        let lines = parsed_lines("cpu foo=1 10");
        server
            .write_lines(&foo_db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap();

        let err = server
            .write_lines(&bar_db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::DatabaseNotInitialized { .. }));

        // creating failed DBs does not work
        let err = create_simple_database(&server, "bar").await.unwrap_err();
        assert!(matches!(err, Error::DatabaseAlreadyExists { .. }));
    }

    #[tokio::test]
    async fn wipe_preserved_catalog() {
        // have the following DBs:
        // 1. existing => cannot be wiped
        // 2. non-existing => can be wiped, will not exist afterwards
        // 3. existing one, but rules file is broken => can be wiped, will not exist afterwards
        // 4. existing one, but catalog is broken => can be wiped, will exist afterwards
        // 5. recently (during server lifecycle) created one => cannot be wiped
        let db_name_existing = DatabaseName::new("db_existing".to_string()).unwrap();
        let db_name_non_existing = DatabaseName::new("db_non_existing".to_string()).unwrap();
        let db_name_rules_broken = DatabaseName::new("db_broken_rules".to_string()).unwrap();
        let db_name_catalog_broken = DatabaseName::new("db_broken_catalog".to_string()).unwrap();
        let db_name_created = DatabaseName::new("db_created".to_string()).unwrap();

        // setup
        let application = make_application();
        let store = Arc::clone(application.object_store());
        let server_id = ServerId::try_from(1).unwrap();

        // Create temporary server to create existing databases
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.maybe_initialize_server().await;

        create_simple_database(&server, db_name_existing.clone())
            .await
            .expect("failed to create database");

        create_simple_database(&server, db_name_rules_broken.clone())
            .await
            .expect("failed to create database");

        create_simple_database(&server, db_name_catalog_broken.clone())
            .await
            .expect("failed to create database");

        // tamper store to break one database
        let mut path = database_store_prefix(store.as_ref(), server_id, &db_name_rules_broken);
        path.set_file_name(DB_RULES_FILE_NAME);

        let data = Bytes::from("x");
        let len = data.len();

        store
            .put(
                &path,
                futures::stream::once(async move { Ok(data) }),
                Some(len),
            )
            .await
            .unwrap();

        let (preserved_catalog, _catalog) = PreservedCatalog::load::<TestCatalogState>(
            Arc::clone(&store),
            server_id,
            db_name_catalog_broken.to_string(),
            (),
        )
        .await
        .unwrap()
        .unwrap();

        parquet_file::catalog::test_helpers::break_catalog_with_weird_version(&preserved_catalog)
            .await;
        drop(preserved_catalog);

        store.get(&path).await.unwrap();

        // boot actual test server
        let server = make_server(Arc::clone(&application));

        // cannot wipe if server ID is not set
        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_non_existing)
                .unwrap_err()
                .to_string(),
            "id not set"
        );

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        // Wait for databases to finish startup
        let databases = server.databases().unwrap();
        assert_eq!(databases.len(), 3);

        for database in databases {
            let name = &database.config().name;
            if name == &db_name_existing {
                database.wait_for_init().await.unwrap();
            } else if name == &db_name_catalog_broken {
                let err = database.wait_for_init().await.unwrap_err();
                assert!(matches!(
                    err.as_ref(),
                    database::InitError::CatalogLoad { .. }
                ))
            } else if name == &db_name_rules_broken {
                let err = database.wait_for_init().await.unwrap_err();
                assert!(matches!(
                    err.as_ref(),
                    database::InitError::RulesDecode { .. }
                ))
            } else {
                unreachable!()
            }
        }

        // 1. cannot wipe if DB exists
        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_existing)
                .unwrap_err()
                .to_string(),
            "error wiping preserved catalog: database (db_existing) in invalid state (Initialized) for transition (WipePreservedCatalog)"
        );
        assert!(PreservedCatalog::exists(
            application.object_store().as_ref(),
            server_id,
            db_name_existing.as_str()
        )
        .await
        .unwrap());

        // 2. cannot wipe non-existent DB
        assert!(matches!(
            server.database(&db_name_non_existing).unwrap_err(),
            Error::DatabaseNotFound { .. }
        ));
        PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(application.object_store()),
            server_id,
            db_name_non_existing.to_string(),
            (),
        )
        .await
        .unwrap();
        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_non_existing)
                .unwrap_err()
                .to_string(),
            "database not found"
        );

        // 3. cannot wipe DB with broken rules file
        assert!(server
            .database(&db_name_rules_broken)
            .unwrap()
            .init_error()
            .is_some());

        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_rules_broken)
                .unwrap_err()
                .to_string(),
            "error wiping preserved catalog: database (db_broken_rules) in invalid state (Known) for transition (WipePreservedCatalog)"
        );

        // 4. wipe DB with broken catalog, this will bring the DB back to life
        let database = server.database(&db_name_catalog_broken).unwrap();
        assert!(database.init_error().is_some());

        let tracker = server
            .wipe_preserved_catalog(&db_name_catalog_broken)
            .unwrap();

        let metadata = tracker.metadata();
        let expected_metadata = Job::WipePreservedCatalog {
            db_name: Arc::from(db_name_catalog_broken.as_str()),
        };
        assert_eq!(metadata, &expected_metadata);
        tracker.join().await;

        database.wait_for_init().await.unwrap();

        assert!(PreservedCatalog::exists(
            application.object_store().as_ref(),
            server_id,
            &db_name_catalog_broken.to_string()
        )
        .await
        .unwrap());
        assert!(database.init_error().is_none());

        assert!(server.db(&db_name_catalog_broken).is_ok());
        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server
            .write_lines(&db_name_catalog_broken, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .expect("DB writable");

        // 5. cannot wipe if DB was just created
        server
            .create_database(DatabaseRules::new(db_name_created.clone()))
            .await
            .unwrap();

        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_created)
                .unwrap_err()
                .to_string(),
            "error wiping preserved catalog: database (db_created) in invalid state (Initialized) for transition (WipePreservedCatalog)"
        );
        assert!(PreservedCatalog::exists(
            application.object_store().as_ref(),
            server_id,
            &db_name_created.to_string()
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn cannot_create_db_when_catalog_is_present() {
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = "my_db";

        // setup server
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.maybe_initialize_server().await;

        // create catalog
        PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(application.object_store()),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        // creating database will now result in an error
        let err = create_simple_database(&server, db_name).await.unwrap_err();
        assert!(matches!(err, Error::CannotCreatePreservedCatalog { .. }));
    }

    // run a sql query against the database, returning the results as record batches
    async fn run_query(db: Arc<Db>, query: &str) -> Vec<RecordBatch> {
        let planner = SqlQueryPlanner::default();
        let executor = db.executor();

        let physical_plan = planner.query(db, query, &executor).unwrap();

        executor
            .collect(physical_plan, ExecutorType::Query)
            .await
            .unwrap()
    }
}
