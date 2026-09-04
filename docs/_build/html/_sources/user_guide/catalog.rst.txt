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

The C++ REST catalog covers the database, table and snapshot operations of the
``Catalog`` API. The parts of the Java REST catalog that have no C++ counterpart
yet — altering a database or a table, views, functions, partitions, tags, branch
management and consumers — are not supported.
