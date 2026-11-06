.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. _catalog:

Catalog
==========================
Paimon C++ provides a :ref:`Catalog abstraction <cpp-api-catalog>` to manage the table of contents and metadata. The Catalog
abstraction provides a series of ways to help you better integrate with computing engines. We always
recommend that you use Catalog to access the Paimon table.

Paimon C++ supports two metastores, selected with the catalog option
``metastore``: the filesystem metastore (default) and the REST metastore.

Filesystem Catalog
~~~~~~~~~~~~~~~~~~
The filesystem metastore (``metastore=filesystem``, the default) stores both
metadata and table files in filesystems. The ``root_path`` argument of
``Catalog::Create`` is the warehouse directory holding the databases and tables.

REST Catalog
~~~~~~~~~~~~
With the REST metastore (``metastore=rest``), catalog metadata is managed by a
remote catalog server exposed through a REST API; table data itself is still read
and written through the table paths returned by the server. See `Java Paimon REST
Catalog <https://paimon.apache.org/docs/master/concepts/rest/>`_ for the concept
and the server-side protocol.

REST catalog support is an optional build component: configure the build with
``-DPAIMON_ENABLE_REST=ON`` (see :ref:`cpp_build_optional_components`).

When ``metastore=rest``, the ``root_path`` argument of ``Catalog::Create`` is not
a filesystem path but the warehouse (instance) name under which the tables are
registered on the REST server. The catalog is configured through the
``CatalogOptions`` keys:

* ``metastore``: must be ``rest`` to select the REST catalog.
* ``uri``: server url of the REST catalog server.
* ``token.provider``: authentication provider of the REST catalog. ``bear``
  (the protocol's historical spelling of "bearer") and ``dlf`` are supported.
* ``token``: token of the ``bear`` token provider.
* ``dlf.region``: region used by DLF request signing. It is inferred from the
  endpoint URI when omitted.
* ``dlf.access-key-id`` and ``dlf.access-key-secret``: static DLF access key.
* ``dlf.security-token``: optional STS security token used with a static access
  key.
* ``dlf.token-path``: path to a JSON file containing refreshable DLF credentials.
* ``dlf.token-loader``: refreshable credential loader. ``local_file`` reads
  ``dlf.token-path`` and ``ecs`` obtains an STS token from an ECS RAM role.
* ``dlf.token-ecs-metadata-url``: ECS RAM role metadata endpoint. It defaults to
  ``http://100.100.100.200/latest/meta-data/Ram/security-credentials/``.
* ``dlf.token-ecs-role-name``: optional ECS RAM role name. The loader discovers
  the role from the metadata endpoint when it is omitted.
* ``dlf.signing-algorithm``: ``default`` selects DLF4-HMAC-SHA256 for DLF VPC
  endpoints and ``openapi`` selects ROA HMAC-SHA1 for DlfNext OpenAPI endpoints.
  ``openapi-v4`` selects ACS4-HMAC-SHA256 for DlfNext OpenAPI endpoints. When a
  request matches a registered DlfNext operation, it also names that operation's
  API in the ``x-acs-action`` header; other requests are signed without it.
  Without this option, an endpoint containing ``dlfnext`` selects ``openapi`` and
  other endpoints select ``default``; ``openapi-v4`` is only used when configured.
* ``table-default.<key>``: table option defaults applied when a created table
  left ``<key>`` unset.
* ``header.<name>``: sent as the ``<name>`` http header on every request to the
  server. The server may configure headers of its own through the ``/v1/config``
  endpoint, which are merged with these as any other option is.

.. code-block:: cpp

   std::map<std::string, std::string> options = {
       {"metastore", "rest"},
       {"uri", "http://127.0.0.1:8080"},
       {"token.provider", "bear"},
       {"token", "<token>"},
   };
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::Catalog> catalog,
                          paimon::Catalog::Create(/*root_path=*/"my_instance", options));

For DLF, configure one credential source. Static AK/SK credentials, an optional
STS token, a refreshable local token file, and ECS RAM role credentials are
supported. A local or ECS token has the Java-compatible JSON fields
``AccessKeyId``, ``AccessKeySecret``, ``SecurityToken`` and ``Expiration``. The
last field uses UTC ``yyyy-MM-dd'T'HH:mm:ss'Z'`` format. Refreshable credentials
are reloaded when less than one hour of validity remains.

.. code-block:: cpp

   std::map<std::string, std::string> options = {
       {"metastore", "rest"},
       {"uri", "https://dlfnext.cn-hangzhou.aliyuncs.com"},
       {"token.provider", "dlf"},
       {"dlf.access-key-id", "<access-key-id>"},
       {"dlf.access-key-secret", "<access-key-secret>"},
       // Optional for temporary credentials:
       {"dlf.security-token", "<security-token>"},
       // Optional for endpoints that require ACS4-HMAC-SHA256 signing:
       {"dlf.signing-algorithm", "openapi-v4"},
   };

On creation the catalog queries the server's ``/v1/config`` endpoint and merges
its response with the options above: the server's overrides win over the client
options, which in turn win over the server's defaults.

Databases and tables are then created, listed, loaded, renamed and dropped
through the regular ``Catalog`` API, and table snapshots can be listed through
``Catalog::ListSnapshots``.

Committing through the catalog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A REST catalog reports ``Catalog::SupportsVersionManagement() == true`` and
publishes snapshots through the server. A loaded table retains its catalog
services and UUID. Writers and committers created from that table use the same
identity and per-table credentials:

.. code-block:: cpp

   std::shared_ptr<paimon::Catalog> shared_catalog(std::move(catalog));
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::Table> table,
                          shared_catalog->GetTable(paimon::Identifier("db", "tbl")));
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::WriteContext> write_context,
                          paimon::WriteContextBuilder("commit-user").Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FileStoreWrite> writer,
                          table->NewWrite(std::move(write_context)));
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::CommitContext> commit_context,
                          paimon::CommitContextBuilder("commit-user").Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FileStoreCommit> commit,
                          table->NewCommit(std::move(commit_context)));
   PAIMON_RETURN_NOT_OK(commit->Commit(commit_messages));

``Catalog::GetTable()`` returns a common ``Table`` interface for managed, format
and system tables. Managed tables select append-only or primary-key behavior
from the loaded schema; system tables reject writes and commits.

``NewCommit()``, ``NewWrite()``, ``NewRead()`` and ``NewScan()`` take an operation
Context and an optional dynamic options map. They use the table's bound type,
path, schema, file system and catalog services. ``GetSchema()`` returns the
bound schema without metadata I/O; reload through the catalog to refresh it.
Branch or type options that contradict a loaded managed table are rejected.
See the :ref:`Table API <cpp-api-catalog>`.

The static factories instead take ``TableLoadOptions``, an operation Context and
an optional dynamic options map. Loading parameters are independent of the
Context; finishing a Context performs no table metadata I/O:

.. code-block:: cpp

   paimon::TableLoadOptions load_options;
   load_options.catalog = shared_catalog;
   load_options.identifier.emplace("db", "tbl");
   // Optional expected UUID, validated against the catalog during loading.
   load_options.table_id = table->CatalogUuid();
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::CommitContext> context,
                          paimon::CommitContextBuilder("commit-user").Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FileStoreCommit> commit,
                          paimon::FileStoreCommit::Create(load_options, std::move(context)));

For file-system loading, set ``TableLoadOptions::path``. Optional fields specify
an explicit file system, URI scheme routing or a serialized Paimon table schema.
The serialized table schema is separate from the Arrow projection in
``ReadContextBuilder::SetReadSchema()``. Set the branch through the identifier
or the dynamic ``branch`` option; conflicting values are rejected.

Global system tables under ``sys`` retain their catalog for deferred row
loading. Keep the catalog in a ``std::shared_ptr`` before loading these tables.

For real-time writes, also set ``WithStreamingMode(true)`` and
``WithRealtimeContext(...)`` with ``realtime.enabled=true``. After
``CommitWithProgress`` succeeds, pass its returned snapshot id to
``writer->RefreshCommittedSnapshot`` to release committed in-memory data.
The refresh reads the current snapshot from the catalog. If another commit has
already advanced the latest snapshot, the requested snapshot must be published
under the table path. Recreating the writer restores offsets from the catalog's
current snapshot.

Table loading uses ``Catalog::GetTableFileSystem`` for per-table credentials.
An explicit ``TableLoadOptions::file_system`` or
``fs_scheme_to_identifier_map`` controls file-system resolution instead.
Loaded format tables create operators directly with their ``New*()`` methods.

A file system passed in ``TableLoadOptions::file_system`` is used exactly as it is: neither the
built-in file systems nor the table's own credentials are involved, so a custom
one has to authenticate its own accesses. Draw credentials that expire from a
``CredentialProvider`` you build with ``CredentialProviderFactory``: hold the
provider and call ``CredentialProvider::GetCredentials`` at each access, which
returns credentials that are still valid and reloads them before they expire.

.. code-block:: cpp

   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::CredentialProvider> provider,
                          paimon::CredentialProviderFactory::Get("my-token-service",
                                                                 "oss://bucket/tbl", options));
   // MyFileSystem merges provider->GetCredentials() over its own options per access.
   auto my_fs = std::make_shared<MyFileSystem>(provider);
   load_options.catalog = shared_catalog;
   load_options.identifier.emplace("db", "tbl");
   load_options.file_system = my_fs;

The snapshot being committed carries a uuid generated on the client, and the
commit names the snapshot it is based on by that snapshot's uuid, so the server
can tell a commit that raced another. A commit which lost such a race is
rebased on the new latest snapshot and retried, bounded by ``commit.max-retries``
and ``commit.timeout`` as a file-system commit is.

A REST catalog can also give a writer a commit lease, so concurrent writers take
turns instead of racing. The table property ``rest.commit.lock-enabled = true``
turns on lease enforcement on the server, and a writer that sets
``rest.commit.acquire-lock = true``, usually as a commit context option, acquires
a lease for each publication attempt. Both default to ``false``; enabling the
table property alone does not make writers acquire, and asking to acquire without
it fails when the commit is created. A writer with an active lease keeps the
others out, including writers that do not acquire one. The lease is bound to the
table or branch, to the table UUID retained by the loaded table,
and to the exact commit user, so give each logical writer its own commit user and
keep it across recovery.

.. code-block:: cpp

   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::Table> table,
                          shared_catalog->GetTable(paimon::Identifier("db", "tbl")));
   std::optional<std::string> table_uuid = table->CatalogUuid();
   if (!table_uuid) {
       return paimon::Status::Invalid("a commit lease needs the table UUID");
   }
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::CommitContext> context,
                          paimon::CommitContextBuilder("writer-1").Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FileStoreCommit> commit,
                          table->NewCommit(std::move(context),
                              {{paimon::Options::REST_COMMIT_ACQUIRE_LOCK, "true"}}));

Each attempt acquires its own lease before reading the latest snapshot, waits
while another writer holds it until ``commit.timeout`` without using up
``commit.max-retries``, renews it in the background and checks it is still
valid right before publishing. An attempt that fails to acquire the lease, or
loses it before publishing, publishes nothing and is retried within
``commit.max-retries`` and ``commit.timeout``, as after a conflict; an attempt
that lost its lease also removes the manifests it wrote. When the retries run
out, the error names the lease failure rather than a conflict.
``RollbackToAsLatest`` holds a lease the same way but is not retried: failing to
acquire or losing the lease fails it.

``commit.timeout`` is not a strict deadline for waiting on the lease. Each
acquisition may wait the full timeout, however much of it earlier attempts of
the same commit used. The timeout is also checked only after each acquisition
request returns, without interrupting a request in flight, so the wait can run
past it by the duration of one request, which only the HTTP client's own
timeouts bound.

There is no unlock request. The server ends a lease when a snapshot is published
under it. Ending an attempt that published nothing only stops renewing the
lease, without waiting for a renewal in flight, and the lease stays held until
it expires.

Leases need a table loaded through a catalog: a commit that only builds the commit table request
for the caller to send cannot hold one, so asking to acquire is rejected there.
The snapshot UUID checks above still apply.

The HTTP client neither retries nor follows redirects for a commit request.
Transport errors and non-success responses, including 429, 503 and redirects,
leave the outcome uncertain: the server may already have accepted the snapshot.
Its metadata files are retained. Use ``FilterAndCommit`` for batch recovery;
after a real-time error, recreate the writer and context and recover from durable
offsets as described by ``CommitWithProgress``.

The main branch's current schema and latest snapshot need no files under the table
path. A branch needs its schema published under its own directory, while its
latest snapshot can be held by the catalog alone, as described below.
A catalog response of ``{"snapshot": null}`` means the table has no snapshot.
Only a catalog reporting that snapshot loading is unsupported falls back to
file-system lookup; other catalog errors propagate to the caller.

Load the table before preparing changes so its retained ``CatalogUuid()`` lets
the server reject commits to a dropped and recreated table. Static commit
loading accepts the expected UUID through ``TableLoadOptions::table_id``.
``Table::Uuid()`` can fall back to the table name and must not supply this id.
An unset table id is sent as null and subject to server validation. Each commit
attempt to the main branch reloads the catalog's current schema id for the new
snapshot; on a branch it reloads the id published under the branch.

.. note::

   Manifests, data and historical metadata still require file-system access:

   * ``FilterAndCommit``, ``CommitWithProgress`` and streaming writer recovery
     may walk older snapshots to find the commit user's last snapshot. Those
     snapshots must be published under the table path. A missing retained
     snapshot is an error; the ``EARLIEST`` hint distinguishes expired history.
   * ``RowIdCheckConflict`` reuses the latest snapshot from the catalog and reads
     earlier snapshots from the table path. With data evolution enabled, the
     check also requires the schemas recorded by the row-id files it inspects.
     Publishing only the current schema is insufficient when files use older ids.
   * ``RollbackToAsLatest`` reads its target snapshot from the table path.
     ``Expire`` manages only snapshots published there and updates the
     ``EARLIEST`` hint; it does not expire catalog-held metadata.
     Before deleting files, expiration verifies that the catalog's current
     snapshot matches the latest published snapshot and reads the retained
     history. It skips expiration while the current snapshot is unpublished
     or differs from its file-system copy. Catalog and retained-metadata read
     errors stop expiration before deletion. A catalog that does not support
     loading its current snapshot cannot perform local expiration.
     Retained snapshots with index manifests also stop expiration with
     ``NotImplemented`` before any files are deleted.

Expiration preserves files referenced by retained snapshots, including files
restored by ``RollbackToAsLatest``. Serialize rollback and expiration through
the upstream coordinator: a rollback must finish before expiration starts,
so its restored file references are visible to the expiration operation.

.. warning::

   ``Expire`` returns ``NotImplemented`` before reading any snapshot or deleting
   any file when it runs on a branch other than main, or finds one under
   ``branch/branch-<name>`` of the table path. Data files are shared by every
   branch of the table, while expiration reads only the retained snapshots of the
   branch it runs on, so it could delete a file another branch still refers to.
   A branch the catalog holds without that directory, or one created while
   expiration runs, is not found: do not expire a table with such a branch, and
   serialize branch creation and expiration through the upstream coordinator.

Reading through the catalog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Load the table through a shared catalog and create read and scan operators from it:

.. code-block:: cpp

   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::Table> table,
                          shared_catalog->GetTable(paimon::Identifier("db", "tbl")));
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::ScanContext> scan_context,
                          paimon::ScanContextBuilder().Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::TableScan> scan,
                          table->NewScan(std::move(scan_context)));
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::ReadContext> read_context,
                          paimon::ReadContextBuilder().Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::TableRead> read,
                          table->NewRead(std::move(read_context)));

The table retains its file system, catalog configuration and identifier for
reads of dependency tables. REST dependency reads preserve an existing
``X-Paimon-Read-Via`` header or derive it from the outer table identifier.

Committing to a branch
~~~~~~~~~~~~~~~~~~~~~~
This library creates no branch: one has to exist, with its schema published under
``branch/branch-<name>``, before a write or a commit can be aimed at it.

Naming the branch
^^^^^^^^^^^^^^^^^
A branch is addressed by ``TableLoadOptions::identifier`` or the loaded table identifier, which
``paimon::Identifier("db", "tbl", "dev")`` builds as ``tbl$branch_dev``, so that
the catalog answers for the branch: the snapshot it loads is the branch's, and
the commit request is addressed to that branch by that identifier rather than by
anything in its body. The dynamic ``branch`` option may name the same branch as well; naming two different branches is rejected rather
than resolved, and ``tbl$branch_main``, ``branch=main`` and an empty name all
mean the main branch. Naming a branch only in the option while
the identifier names the bare table is rejected, as the catalog would then answer
for the table while the files were written for the branch.

Names a catalog cannot tell apart from another object are rejected during loading: a name differing from ``main`` by case alone, such as
``tbl$branch_MAIN`` or ``branch=MAIN``, which a catalog reads as the main branch
while its snapshots go to a directory of their own; and a name holding a ``$``,
such as ``branch=dev$options``, which ``tbl$branch_dev$options`` reads back as
the ``options`` system table of branch ``dev``. ``paimon::Identifier("db",
"tbl", "MAIN")`` never reaches that check: the constructor folds every spelling
of ``main`` into the bare table name, as the Java client does, and so names the
main branch.

What a branch keeps and what it shares
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A branch other than the main one publishes its schema and real-time offsets
under ``branch/branch-<name>``, where a snapshot written to the file system goes
as well; the main branch keeps these files under the table path. A
version-managed catalog can hold the latest snapshot of either branch alone.
Data and manifests are shared by all branches under the table path,
which is why expiration is not supported on a table with branches, as noted
above.

A write or a commit aimed at a branch reads that schema rather than the
catalog's, just as a read of that branch does, and asks the catalog for no schema
at all, so a catalog serving only the table's own current schema still takes the
branch's snapshots. The new snapshot records the schema id published there as
well, which is the id a read of the branch resolves under
``branch/branch-<name>``; only on the main branch is the recorded id the one the
catalog reports, as its current schema may live nowhere else. Data files record
the schema they were written with, as they do on the main branch.

Addressing a branch without a catalog client
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
``UseRESTCatalogCommit`` builds the request instead of sending it, and takes the
branch from the ``branch`` option alone, as it is configured without an
identifier. The request body carries the table id but no table name and no
branch, so the branch has to appear in the URL the caller sends that body to:
the commit endpoint of ``tbl$branch_dev``, not the one of ``tbl``. A body sent
to the bare table's URL publishes the branch's snapshot on the main branch.

Authenticating with credentials of your own
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Credentials that a catalog does not issue — from a token service of your own, or
from a store Paimon has no client for — reach a file system through a
``CredentialProvider`` that you consult yourself. Implement it together with a
``CredentialProviderFactory`` that builds it and register the factory with
``REGISTER_PAIMON_FACTORY``. Build a provider with
``CredentialProviderFactory::Get``, wrap it in a ``FileSystem`` of your own that
calls ``CredentialProvider::GetCredentials`` as it signs each access, and pass
that file system in through ``Catalog::Create`` or ``TableLoadOptions::file_system``.
A file system passed this way is used as-is, so the credentials are reloaded as
they expire without it being rebuilt.

The built-in file systems — ``oss``, ``s3``, ``local``, ``jindo`` — do not consult
a provider: they sign with the static credentials of their own options. Bring a
file system of your own when the credentials expire.

.. code-block:: cpp

   class MyCredentialProvider : public paimon::CredentialProvider {
    public:
       // The factory hands over the path and the file system options of the accesses to
       // authenticate, which is where you read your own configuration from.
       MyCredentialProvider(std::string path, std::map<std::string, std::string> options)
           : path_(std::move(path)), options_(std::move(options)) {}

       // Called as each access is signed; reload the credentials before they expire.
       paimon::Result<std::map<std::string, std::string>> GetCredentials() const override {
           // Request fresh credentials for path_ from the token service in options_.
           return std::map<std::string, std::string>{
               {"fs.oss.accessKeyId", ...}, {"fs.oss.accessKeySecret", ...},
               {"fs.oss.securityToken", ...}};
       }

    private:
       std::string path_;
       std::map<std::string, std::string> options_;
   };

   class MyCredentialProviderFactory : public paimon::CredentialProviderFactory {
    public:
       const char* Identifier() const override {
           return "my-token-service";
       }

       paimon::Result<std::shared_ptr<paimon::CredentialProvider>> Create(
           const std::string& path,
           const std::map<std::string, std::string>& options) const override {
           return std::make_shared<MyCredentialProvider>(path, options);
       }
   };

   REGISTER_PAIMON_FACTORY(MyCredentialProviderFactory);

The credentials are file-system options, e.g. ``fs.oss.securityToken``, and are
merged over the options the file system was configured with, so a provider serves
only what rotates and the endpoint and the rest stay in the options. The options
the factory is called with are those of the accesses to authenticate, which is
where an implementation reads its own configuration, such as the address of the
token service, from.

Merge the credentials into your file system's options with
``CredentialProvider::MergeOptionsWithCredentials`` rather than by hand, so they are
shaped the same way the built-in data token file system shapes them. The default
overlays the credentials key by key over the base options, mirroring the Java
client. A provider that knows the file system its credentials are for overrides
``MergeOptionsWithCredentials`` to normalize option aliases or clear the stale
bucket-scoped variants the fresh credentials replace.

All factories share one identifier space, so an identifier a file system factory
already takes — ``oss``, ``s3``, ``local``, ``jindo`` — would replace it; name the
provider after where its credentials come from instead.
