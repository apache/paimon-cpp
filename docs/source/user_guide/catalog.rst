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
  When omitted, an endpoint containing ``dlfnext`` selects ``openapi`` and other
  endpoints select ``default``.
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
publishes snapshots through the server. Pass the catalog and table identifier
to ``CommitContextBuilder::WithCatalog`` to commit on local or object storage
without an atomic snapshot-file rename:

.. code-block:: cpp

   // Writers and committers share ownership of the catalog.
   std::shared_ptr<paimon::Catalog> shared_catalog(std::move(catalog));

   // Load the table's identity before preparing changes.
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::Table> table,
                          shared_catalog->GetTable(paimon::Identifier("db", "tbl")));

   paimon::CommitContextBuilder builder(table_path, "commit-user");
   builder.WithCatalog(shared_catalog, paimon::Identifier("db", "tbl"));
   // CatalogUuid() stays null when the server supplies no id.
   if (std::optional<std::string> table_uuid = table->CatalogUuid()) {
       builder.WithTableId(table_uuid.value());
   }
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::CommitContext> context, builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FileStoreCommit> commit,
                          paimon::FileStoreCommit::Create(std::move(context)));
   PAIMON_RETURN_NOT_OK(commit->Commit(commit_messages));

Configure native writers with the same catalog and identifier. They load the
current schema and latest snapshot from the catalog, including committed
offsets when a real-time writer is created:

.. code-block:: cpp

   paimon::WriteContextBuilder write_builder(table_path, "commit-user");
   write_builder.WithCatalog(shared_catalog, paimon::Identifier("db", "tbl"));
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::WriteContext> write_context,
                          write_builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FileStoreWrite> writer,
                          paimon::FileStoreWrite::Create(std::move(write_context)));

For real-time writes, also set ``WithStreamingMode(true)`` and
``WithRealtimeContext(...)`` with ``realtime.enabled=true``. After
``CommitWithProgress`` succeeds, pass its returned snapshot id to
``writer->RefreshCommittedSnapshot`` to release committed in-memory data.
The refresh reads the current snapshot from the catalog. If another commit has
already advanced the latest snapshot, the requested snapshot must be published
under the table path. Recreating the writer restores offsets from the catalog's
current snapshot.

Both builders use the file system of the table being written or committed for
manifests and data, so a catalog that issues temporary credentials per table
serves them through ``Catalog::GetTableFileSystem``. ``WithFileSystem`` overrides
it; writers also allow ``WithFileSystemSchemeToIdentifierMap`` to override
file-system selection. For format tables, use ``WriteContextBuilder(FormatTable)``
instead.

A file system passed to ``WithFileSystem`` is used exactly as it is: neither the
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
   write_builder.WithCatalog(shared_catalog, paimon::Identifier("db", "tbl"))
       .WithFileSystem(my_fs);

The snapshot being committed carries a uuid generated on the client, and the
commit names the snapshot it is based on by that snapshot's uuid, so the server
can tell a commit that raced another. A commit which lost such a race is
rebased on the new latest snapshot and retried, bounded by ``commit.max-retries``
and ``commit.timeout`` as a file-system commit is.

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

Read ``Table::CatalogUuid()`` before preparing changes and pass it to
``WithTableId`` so the server can reject commits to a dropped and recreated table.
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
To read a native table with the per-table temporary credentials a catalog issues,
pass the catalog and table identifier to ``ReadContextBuilder::WithCatalog`` or
``ScanContextBuilder::WithCatalog``. This is a shorthand for asking the catalog
for that table's file system and passing it in through ``WithFileSystem``:

.. code-block:: cpp

   // Readers and scanners share ownership of the catalog.
   std::shared_ptr<paimon::Catalog> shared_catalog(std::move(catalog));

   paimon::ScanContextBuilder scan_builder(table_path);
   scan_builder.WithCatalog(shared_catalog, paimon::Identifier("db", "tbl"));
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::ScanContext> scan_context,
                          scan_builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::TableScan> scan,
                          paimon::TableScan::Create(std::move(scan_context)));

   paimon::ReadContextBuilder read_builder(table_path);
   read_builder.WithCatalog(shared_catalog, paimon::Identifier("db", "tbl"));
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::ReadContext> read_context,
                          read_builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::TableRead> read,
                          paimon::TableRead::Create(std::move(read_context)));

When ``Finish()`` builds the context, it asks the catalog for the table's file
system through ``Catalog::GetTableFileSystem`` and uses it as-is for the schema,
snapshots, manifests and data under the table path, so it signs every access with
the table's credentials and reloads them as they expire. An explicit
``WithFileSystem`` takes precedence, so the catalog is not asked. A catalog that
issues no per-table credentials returns its catalog-level file system, so this is
also how you read with the catalog's own object-store credentials. For a format
table, use ``ReadContextBuilder(FormatTable)`` or ``ScanContextBuilder(FormatTable)``
instead; ``WithCatalog`` is rejected because the table already carries the file
system it was loaded through.

The C++ REST catalog covers the database, table, snapshot and commit operations
of the ``Catalog`` API. The parts of the Java REST catalog that have no C++
counterpart yet — altering a database or a table, views, functions, partitions,
tags, branch management and consumers — are not supported. Creating, deleting
and merging a branch is what branch management covers; committing to a branch
that already exists is supported, as described below.

Committing to a branch
~~~~~~~~~~~~~~~~~~~~~~
This library creates no branch: one has to exist, with its schema published under
``branch/branch-<name>``, before a write or a commit can be aimed at it.

Naming the branch
^^^^^^^^^^^^^^^^^
A branch is addressed by the identifier of ``WithCatalog``, which
``paimon::Identifier("db", "tbl", "dev")`` builds as ``tbl$branch_dev``, so that
the catalog answers for the branch: the snapshot it loads is the branch's, and
the commit request is addressed to that branch by that identifier rather than by
anything in its body. The ``branch`` option and, for writers, ``WithBranch`` may
name the same branch as well; naming two different branches is rejected rather
than resolved, and ``tbl$branch_main``, ``branch=main`` and an empty name all
mean the main branch. Naming a branch only in the option or ``WithBranch`` while
the identifier names the bare table is rejected, as the catalog would then answer
for the table while the files were written for the branch.

Names a catalog cannot tell apart from another object are rejected where a
builder sees them: a name differing from ``main`` by case alone, such as
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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Credentials that a catalog does not issue — from a token service of your own, or
from a store Paimon has no client for — reach a file system through a
``CredentialProvider`` that you consult yourself. Implement it together with a
``CredentialProviderFactory`` that builds it and register the factory with
``REGISTER_PAIMON_FACTORY``. Build a provider with
``CredentialProviderFactory::Get``, wrap it in a ``FileSystem`` of your own that
calls ``CredentialProvider::GetCredentials`` as it signs each access, and pass
that file system in through ``Catalog::Create`` or a builder's ``WithFileSystem``.
A file system passed this way is used as-is, so the credentials are reloaded as
they expire without it being rebuilt.

The built-in file systems — ``oss``, ``s3``, ``local``, ``jindo`` — do not consult
a provider: they sign with the static credentials of their own options. Bring a
file system of your own when the credentials expire.

.. code-block:: cpp

   class MyCredentialProvider : public paimon::CredentialProvider {
    public:
       // Called as each access is signed; reload the credentials before they expire.
       paimon::Result<std::map<std::string, std::string>> GetCredentials() const override {
           return std::map<std::string, std::string>{
               {"fs.oss.accessKeyId", ...}, {"fs.oss.accessKeySecret", ...},
               {"fs.oss.securityToken", ...}};
       }
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
