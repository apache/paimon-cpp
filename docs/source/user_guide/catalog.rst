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

``Catalog::GetTable()`` returns a common ``Table`` interface for managed, format and
system tables. The managed implementation selects append-only or primary-key behavior
from the loaded schema; system tables reject writes and commits. Loading a managed
table's schema, location and catalog UUID does not require its data-format plugin;
the plugin is resolved when creating an operator that uses the format.

``table->NewCommit()``, ``NewWrite()``, ``NewRead()`` and ``NewScan()`` use the table's
bound type, path, schema and file system. ``GetSchema()`` returns that schema without
metadata I/O; reload through the catalog to refresh it. Contexts still provide execution
settings, dynamic options and optional catalog services. Branch or type options that
contradict a loaded managed table are rejected. The legacy static ``Create()`` methods
load the table from their context before delegating to these methods.
See the :ref:`Table API <cpp-api-catalog>`.

An explicit ``WithBranch()`` or branch-qualified identifier participates in the same
branch check. Omitting a branch allows the loaded table to supply it. The static
scan and read factories use an explicit context branch first, then the branch in
the schema options, and finally main.

Global system tables under ``sys`` retain their catalog for deferred row loading.
Before calling ``GetTable()`` for one, place the catalog in a ``std::shared_ptr``,
as in the example above. ``LoadTableSchema()`` can inspect the schema without
retaining the catalog.

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

A REST catalog can also give a writer a commit lease, so concurrent writers take
turns instead of racing. The table property ``rest.commit.lock-enabled = true``
turns on lease enforcement on the server, and a writer that sets
``rest.commit.acquire-lock = true``, usually as a commit context option, acquires
a lease for each publication attempt. Both default to ``false``; enabling the
table property alone does not make writers acquire, and asking to acquire without
it fails when the commit is created. A writer with an active lease keeps the
others out, including writers that do not acquire one. The lease is bound to the
table or branch, to the table UUID, which has to be passed to ``WithTableId``,
and to the exact commit user, so give each logical writer its own commit user and
keep it across recovery.

.. code-block:: cpp

   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::Table> table,
                          shared_catalog->GetTable(paimon::Identifier("db", "tbl")));
   std::optional<std::string> table_uuid = table->CatalogUuid();
   if (!table_uuid) {
       return paimon::Status::Invalid("a commit lease needs the table UUID");
   }
   paimon::CommitContextBuilder builder(table_path, "writer-1");
   builder.WithCatalog(shared_catalog, paimon::Identifier("db", "tbl"))
       .WithTableId(table_uuid.value())
       .AddOption(paimon::Options::REST_COMMIT_ACQUIRE_LOCK, "true");

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

Leases need ``WithCatalog``: a commit that only builds the commit table request
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
To read a native table through its catalog, pass the catalog and table
identifier to ``ReadContextBuilder::WithCatalog`` or
``ScanContextBuilder::WithCatalog``. The catalog then supplies:

* the table's file system, with the per-table temporary credentials the catalog
  issues, unless ``WithFileSystem`` sets one;
* whether the table enables ``query-auth.enabled``, read from the schema the
  catalog holds for the branch read;
* for a table that does, its row filters and column masks.

Passing only the file system through ``WithFileSystem`` gives neither of the
last two.

The fields a scan plans with and a read reads with do not come from the catalog,
but from the schema ``SetTableSchema`` hands in for the main branch or, otherwise,
from the latest schema published under the table path of the branch read. That
schema has to exist even when the catalog holds the current one: a table whose
schema only the catalog holds cannot be scanned or read this way yet, and when
the two differ, the fields are those of the schema under the table path. A
query-auth scan also checks column identities and rule types against the
catalog's schema on every plan:

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
``WithFileSystem`` takes precedence, so the catalog is not asked for one. A
catalog that issues no per-table credentials returns its catalog-level file
system, so this is also how you read with the catalog's own object-store
credentials. For a format table, use ``ReadContextBuilder(FormatTable)`` or
``ScanContextBuilder(FormatTable)`` instead; ``WithCatalog`` is rejected because
the table already carries the file system it was loaded through.

Row filters and column masks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A table with ``query-auth.enabled = true`` lets the catalog restrict what a read
returns with row filters and column masks, as in Java. The option is enabled when
any of these sets it:

* the latest schema under the table path;
* the options of the scan or read;
* for a scan or read built ``WithCatalog``, the schema the catalog holds for the
  branch read, which ``TableScan::Create`` and ``TableRead::Create`` load as Java
  loads the table of that branch.

A schema handed in with ``SetTableSchema`` for the main branch does not decide it
alone: unless the catalog already enabled the option, the latest schema under the
table path is checked too, so a cache taken before the option was turned on still
applies the rules.

What the rules may contain:

* Row filters support the ``AND`` and ``OR`` of the comparison, ``IN``,
  ``BETWEEN``, null, prefix, suffix, ``CONTAINS`` and ``LIKE`` predicates on a
  column referenced with ``FIELD_REF``, and the ``TRUE`` and ``FALSE`` constants
  Java's ``PredicateBuilder.alwaysTrue()`` and ``alwaysFalse()`` write on a
  ``NULL`` transform; a ``FALSE`` filter reads no row. A predicate on any other
  transform, such as ``UPPER(name)``, is refused. ``LIKE`` matches as Java's
  does: ``_`` is any one character, a line terminator included.
* Their literals may be ``BOOLEAN``, ``TINYINT``, ``SMALLINT``, ``INT``,
  ``BIGINT``, ``FLOAT``, ``DOUBLE``, ``STRING``, ``DECIMAL``, ``DATE``,
  ``TIMESTAMP`` and ``TIMESTAMP_LTZ``, the temporal ones in the ISO text format
  written by Java's ``LocalDate``, ``LocalDateTime`` and ``Instant``, with an
  instant ending in ``Z``. A year outside
  ``0000`` to ``9999`` takes a sign as in Java, a fraction at most nine digits,
  and a timestamp must lie within the range of epoch milliseconds. A filter on a
  column of another type, such as ``TIME``, is refused with ``NotImplemented``;
  a ``FIELD_REF`` mask still copies such a column. ``DOUBLE`` literals use
  full-precision parsing to preserve Java's comparison boundaries; non-finite
  floating-point literals are refused. Integer ``FLOAT`` literals preserve Java's
  direct integer-to-float rounding, including integers beyond 64 bits.
* Column masks support the ``NULL``, ``FIELD_REF``, ``CONCAT``, ``CONCAT_WS`` and
  ``SUBSTRING`` transforms, whose inputs are literals, field references or null;
  a transform nested in the input of another is refused. As in Java,
  ``SUBSTRING`` returns null if any input is null, including its position or
  length.
* A mask of a system column such as ``_ROW_ID``, which a query-auth read never
  projects, is parsed but not applied, as Java ignores one the query does not
  project. If the table schema contains an actual column with such a name, the
  query fails instead of silently ignoring that column's mask.
* Malformed rules fail the query, including invalid UTF-8 and unescaped NUL
  bytes. The ``literals`` array of an ``IN`` or ``NOT_IN`` rule must be present
  and non-null; an explicit ``[]`` remains valid. A rule, or the auth response
  carrying it, that names a JSON member twice is refused rather than read as
  either value. An escaped NUL in a string literal or column name is preserved.

What would not apply the rules fails with ``NotImplemented``: search, an explicit
read schema (``SetReadSchema``, use ``SetReadFieldNames`` instead), a projected
or filtered column outside the table schema such as ``_ROW_ID``, real-time
reads, ``GlobalIndexScan::Create``, a partition filter on a masked partition
key, a count reader, the ``$files``, ``$manifests``, ``$binlog``, ``$audit_log``
and ``$ro`` system tables, fallback reads of a query-auth table, and a fallback
branch that enables the option on a table that does not, including when only its
catalog schema or a newer schema than the cached one enables it. Each fallback
branch needs its own authorized splits and schema validation. Passing
``query-auth.enabled = false`` as a read option fails with ``Invalid`` when the
table enables the option, and changes nothing otherwise. Writing to and
committing to the table are not affected. A format table refuses the option,
whether its schema or the options of a call set it. ``sys.partitions`` leaves
out the partitions of a query-auth table, whose values its rules may hide,
including when only its latest published schema enables the option and the
catalog's schema has not caught up.
For REST tables, the catalog schema and location come from the same response,
so a concurrent table recreation cannot pair an old policy with a new location.

Planning and reading a query-auth table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Every plan of such a table, the first included, asks the catalog for the rules
of the query (``POST /v1/{prefix}/databases/{database}/tables/{table}/auth``),
checks that the columns they name still exist with the types they were written
for and are columns of the schema the scan plans with, and attaches them to each
split. A scan therefore has to be built ``WithCatalog``, and a catalog that does
not support the query auth interface is refused with ``NotImplemented``. The
scan path must be the location ``Catalog::GetTableLocation`` returns for the
table and branch the rules are asked for, so the rules of one table are never
applied to the files of another; a different path fails with ``Invalid`` when
the scan is created or, if the catalog has since bound the table elsewhere, when
it plans. A scan or read whose catalog leaves ``query-auth.enabled`` off is held
to the same location, so that setting is never taken for another table, and a
system table path such as ``tbl$branch_dev$files`` takes it from the branch it
names. An identifier in the ``sys`` database can only be used with a global
system table path (``<warehouse>/sys/<table>``); it cannot skip the catalog policy
of a data table or its per-table system tables.
The branch the identifier, the ``branch`` option, ``WithBranch()`` or a
system table path names must agree, which ``Finish()`` checks before it asks the
catalog for anything. A branch named only by the option or the path is carried
into the identifier before requesting the table's file system, so the
``query-auth.enabled`` setting, the credentials and the rules all come from the
branch the scan plans.
``Finish()`` also refuses, as for a write, a branch the catalog would read as
another object, such as ``MAIN`` or a name holding a ``$``
(see `Naming the branch`_). The request selects every column unless
``ScanContextBuilder::SetReadFieldNames`` names the columns the query reads; the
columns of its predicate and partition filter are then selected as well. The
catalog authorizes these columns by their current names, so every plan also
checks that each is still the field, by id, that the scan was created with, in
both the latest published schema and the catalog's current schema. The rules
must fit both schemas as well. A mismatch fails the plan, even when the catalog
returns no rules, so a catalog-only rename or column replacement cannot expose
old data through a stale local schema. Refresh the published or cached schema
and create the scan again after such a change. For an external table, which Java
reads without rules, the REST catalog does not ask the server for any once the
table it loads reports itself as external; a failed load or a failed request
fails the plan.

Each plan asks for the rules again, but the scan pushes the row filter, and the
predicate conjuncts on columns not masked, into the planning of its first plan.
A later plan whose row filter differs, or that masks a column of the predicate
the first one did not, fails with ``Invalid`` rather than plan with filters
pushed down under the old rules. Java can replace its row-filter pruning at each
plan, but likewise refuses a new mask on a column whose user predicate was
already pushed down. Create the paimon-cpp scan again to apply the new rules; to
resume a streaming scan, start the new one from the snapshot after the last one
read, with ``scan.mode = from-snapshot`` and ``scan.snapshot-id``.

Each split records the table location, branch and columns its rules were
authorized for. Columns are bound by name and field id in the schema the scan
planned with, including every column when the scan selected them all. The split
also records the field id of every column its rules reference. The read applies
the rules a split carries and refuses a split planned without them. It refuses
a split authorized for another table location or branch, even when their schemas
and data file names match. This check also applies to a worker without the
catalog. The read only has to know that the table enables query auth, which the
schema under the table path, ``WithCatalog`` or the read option
``query-auth.enabled = true`` tells it. It refuses a split when a column it
outputs or filters on is not among those the split was authorized for or is now
another field under the same name, as after a rename or a column added since the
plan, and when a column the rules reference was renamed, replaced or dropped
since. It compares names and field ids only: the types the rules declare are
checked against the schema read, and a rule declaring none, such as a ``NULL``
mask, only needs its column to exist. Plan again after such a schema change.
Its rows go through the row filter, then the column masks, then the read's own
predicate, so that predicate sees the masked values: all of it when
``EnablePredicateFilter`` asks for it, and otherwise, as in Java, its conjuncts
on masked columns, which no scan prunes by. The columns the rules and the
predicate read are read as well and dropped from the output. A masked column is
output as nullable and decoded. A column a mask, the row filter or that
predicate reads is decoded for it if the file reader kept it dictionary-encoded
or as a large string, and is output decoded too unless only a mask reads it.
Under a row filter no column or nested child is output dictionary-encoded, since
a dictionary would still hold the values of the rows the filter drops; otherwise
any other column is output as the file reader returned it.
Neither the predicate of a read nor the conjuncts of a scan's predicate on masked
columns prune files, whose statistics hold raw values, and a scan whose predicate
has such conjuncts does not push its limit down; nor does one whose row filter
reads data columns. A mask reading another masked column and a rule paimon-cpp
does not know fail the query.

Query-auth splits across runtimes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Plan and read a table with ``query-auth.enabled = true`` in the same runtime.
paimon-cpp can deserialize supported Java query-auth splits but refuses to read
them. Java rejects the envelope of a split planned by paimon-cpp when
deserializing it.

A split planned by paimon-cpp serializes in an envelope of its own, the Java
``SplitSerializer`` bytes of the ``QueryAuthSplit`` followed by that scope, which
Java rejects as a corrupted split rather than reading it without the scope. A
``QueryAuthSplit`` Java serialized does not record which columns its rules were
authorized for, so paimon-cpp deserializes it with the same rules but refuses to
read it: its rules may be those of other columns than the read outputs. Written
back, it takes the Java format, though not necessarily the bytes Java wrote: a
null rule list or map becomes an empty one and the masks are ordered by column.
Only a Java split of ``SplitSerializer`` version 1 wrapping a ``DataSplit`` or an
``IndexedSplit``, whose rules paimon-cpp supports, deserializes; one whose
``authResult`` is null is refused.

Query-auth examples
^^^^^^^^^^^^^^^^^^^
These examples use ``shared_catalog`` from `Reading through the catalog`_.

.. code-block:: cpp

   using namespace paimon;

   // Plan with the columns and the filter the read uses.
   ScanContextBuilder scan_builder(table_path);
   scan_builder.WithCatalog(shared_catalog, Identifier("db", "tbl"))
       .SetReadFieldNames({"name", "phone"})
       .SetPredicate(predicate);
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> scan,
                          TableScan::Create(std::move(scan_context)));
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, scan->CreatePlan());

   // A worker without the catalog tells the read that the table enables query auth.
   ReadContextBuilder read_builder(table_path);
   read_builder.AddOption(Options::QUERY_AUTH_ENABLED, "true")
       .SetReadFieldNames({"name", "phone"})
       .SetPredicate(predicate)
       .EnablePredicateFilter(true);
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> read,
                          TableRead::Create(std::move(read_context)));
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, read->CreateReader(plan->Splits()));

The read does not take the branch from the splits or from
``query-auth.enabled``: a worker reading a branch names it itself, as the scan
did. As for a scan, a branch named by the ``branch`` option or ``WithBranch`` is
carried into the identifier of a read built ``WithCatalog``, so the catalog
hands out the credentials of that branch.

.. code-block:: cpp

   // Plan branch "dev"; Identifier("db", "tbl", "dev") names it as well.
   ScanContextBuilder dev_scan_builder(table_path);
   dev_scan_builder.WithCatalog(shared_catalog, Identifier("db", "tbl"))
       .AddOption(Options::BRANCH, "dev")
       .SetReadFieldNames({"name", "phone"});
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> dev_scan_context,
                          dev_scan_builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> dev_scan,
                          TableScan::Create(std::move(dev_scan_context)));
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> dev_plan, dev_scan->CreatePlan());

   // The worker reads the same branch.
   ReadContextBuilder dev_read_builder(table_path);
   dev_read_builder.AddOption(Options::QUERY_AUTH_ENABLED, "true")
       .AddOption(Options::BRANCH, "dev")
       .SetReadFieldNames({"name", "phone"});
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> dev_read_context,
                          dev_read_builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> dev_read,
                          TableRead::Create(std::move(dev_read_context)));
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> dev_reader,
                          dev_read->CreateReader(dev_plan->Splits()));

The C++ REST catalog covers the database, table, snapshot and commit operations
of the ``Catalog`` API, and the query auth of a table. The parts of the Java REST
catalog that have no C++ counterpart yet — altering a database or a table, views,
functions, partitions, tags, branch management and consumers — are not supported.
Creating, deleting and merging a branch is what branch management covers;
committing to a branch that already exists is supported, as described below.

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

Identifier aliases such as ``tbl$branch_MAIN`` are normalized to the main branch,
as is ``paimon::Identifier("db", "tbl", "MAIN")``. This normalization applies to
identifiers; explicit option values keep their case. Thus ``branch=MAIN`` is
rejected for catalog scans, reads, writes and commits because the catalog treats it as main
while file-system metadata would use a separate directory. Names containing a
``$``, such as ``branch=dev$options``, are also rejected for catalog operations
because they are ambiguous with system-table identifiers.

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
catalog's and asks the catalog for no schema at all, so a catalog serving only
the table's own current schema still takes the branch's snapshots. A scan or
read also uses the fields of the schema published under the branch, but through
``WithCatalog`` it asks for the catalog's branch schema to check
``query-auth.enabled`` (see `Reading through the catalog`_).

The new snapshot records the schema id published under the branch, which is the
id a read of the branch resolves under ``branch/branch-<name>``; only on the main
branch is the recorded id the one the catalog reports, as its current schema may
live nowhere else. Data files record the schema they were written with, as they
do on the main branch.

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
that file system in through ``Catalog::Create`` or a builder's ``WithFileSystem``.
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
