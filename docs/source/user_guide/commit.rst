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

Commit
==========================================

Commit is a critical stage in Paimon's write path. It is responsible for generating
Snapshot files that describe the current state of a Paimon table. This document
provides a detailed analysis of the Paimon Commit process.

Commit Process Overview
--------------------------

The input to Commit is a ``CommitMessage``, which is produced by the write
operation through ``PrepareCommit``. It records all data files generated during
the write phase.

The Commit process consists of the following steps:

1. Collect file changes
2. Compact (merge) Manifest files
3. Generate the Base Manifest List
4. Generate new Manifest files and the Delta Manifest List
5. Prepare and publish the Snapshot

Detailed Process
-------------------

Collect File Changes
~~~~~~~~~~~~~~~~~~~~~~~~

During Commit, the system extracts key information from ``CommitMessages``—such as
file name, operation type (``ADD`` or ``DELETE``), the file's Partition and Bucket—
and converts them into ``ManifestEntry`` records.

A ``ManifestEntry`` represents a single operation record in a manifest file and
corresponds to a change to one file.

Paimon snapshots track two manifest list files:

- Base Manifest List: Describes the data that existed prior to the current Snapshot.
  Because there may be multiple manifest files, the base manifest list records
  metadata for all original manifest files.
- Delta Manifest List: Records the changes (adds/deletes) produced by the current Commit.

Compact (Merge) Manifest Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To control the number and size of manifest files, the system determines whether
existing manifest files should be compacted prior to generating a new Snapshot.

Compaction starts by fetching the latest Snapshot and using its base and delta
manifest lists to locate all relevant manifest files.

Two compaction strategies are used: Full Compaction and Minor Compaction.

Full Compaction
^^^^^^^^^^^^^^^

Full Compaction is attempted first. The system iterates over candidate files and
classifies them as follows:

- Base files: If a file has no ``DELETE`` operations and its size exceeds the
  target file size (default 8 MB), the file is categorized as base.
- Delta files: Remaining files are categorized as delta. The system computes the
  total size of delta files; if the total exceeds the Full Compaction threshold
  (default 16 MB), the delta files are merged.

Minor Compaction
^^^^^^^^^^^^^^^^

If Full Compaction's conditions are not met, Minor Compaction is attempted:

- The system iterates over all files, skipping any file larger than the target file size.
- Whenever the accumulated size of selected files exceeds the target file size, those files are merged.
- If there are still unmerged files and their count exceeds the minimum compaction trigger
  threshold (default 30 files), a merge is triggered.

Compaction Rules
^^^^^^^^^^^^^^^^

1. If duplicate ``ADD`` operations for the same file are discovered, an error is raised.
2. ``ADD`` and ``DELETE`` for the same file neutralize each other.

Generate the Base Manifest List
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After compaction (which may or may not be triggered every time), the system obtains
a consolidated set of manifest file metadata. This metadata is written into a new
manifest list file, forming the Snapshot's base manifest list.

Generate New Manifest Files and the Delta Manifest List
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The initially collected file change information is written into new manifest files.
Metadata for these newly created manifest files is then written into the delta
manifest list.

Prepare and Publish the Snapshot
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After the above steps are completed, the system generates a new Snapshot and performs
the following operations:

1. Determine the new Snapshot ID based on the latest ``SnapshotId + 1``.
2. Record metadata such as ``schema id``, ``commit time``, and ``total record count``.
3. If preparation fails before publication, the newly generated manifest files and
   manifest list files are cleaned up. If publication returns an error and its
   outcome is uncertain, the metadata files are retained because the snapshot may
   already have been accepted.

Snapshot publication depends on the commit mode:

* **Direct file-system commit:** On non-object-store paths, the Snapshot is
  published through an atomic rename. After publication, the committer writes
  the ``LATEST`` hint file to reduce list operations when fetching the latest
  Snapshot.
* **Catalog-mediated commit:** When ``CommitContextBuilder::WithCatalog()``
  supplies a catalog that manages table versions, the committer sends the
  Snapshot and change statistics to that catalog for publication. This path
  does not rename a local Snapshot file or write a local ``LATEST`` hint.
* **REST request preparation:** ``UseRESTCatalogCommit(true)`` builds a commit
  request without publishing the Snapshot. The caller retrieves the JSON with
  ``GetLastCommitTableRequest()`` and sends it to the REST catalog. A successful
  local ``Commit()`` in this mode means the request was prepared, not that the
  catalog accepted it.

Object-store paths require one of the catalog modes. See :doc:`write` for
configuration and :doc:`catalog` for catalog publication and recovery semantics.
