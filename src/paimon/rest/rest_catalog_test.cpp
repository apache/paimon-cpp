/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/rest/rest_catalog.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/table.h"
#include "paimon/catalog_options.h"
#include "paimon/commit_context.h"
#include "paimon/commit_message.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/catalog/commit_table_request.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/rest/mock_rest_server.h"
#include "paimon/rest/rest_api.h"
#include "paimon/schema/schema.h"
#include "paimon/table/format/format_table.h"
#include "paimon/testing/utils/snapshot_test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

constexpr const char kToken[] = "test-token";
constexpr const char kPrefix[] = "paimon";
constexpr const char kWarehouse[] = "wh1";
// Expiration of the data tokens the mock issues, far enough in the future that they are
// never refreshed within a test.
constexpr int64_t kDataTokenExpiresAtMillis = 4102444800000;

// The in-memory catalog state behind the mock rest server.
struct MockCatalogState {
    struct TableData {
        std::string schema_json;
        int64_t schema_id = 0;
        std::string path;
        std::string id = "1";
        std::map<std::string, std::string> token = {{"fs.oss.accessKeyId", "ak-1"}};
    };
    std::map<std::string, std::map<std::string, TableData>> databases;
    // headers of the last request, with lower-cased names
    std::map<std::string, std::string> last_headers;
    // paths the data token endpoint was called at, in order
    std::vector<std::string> token_requests;
    // when set, every request except "/v1/config" fails with this http code
    std::optional<int32_t> force_error_code;
    // how many times a single table has been fetched, so a caller that needs the path and the
    // schema together can be held to one round trip
    int32_t get_table_requests = 0;
    // how many create-table requests reached the server, so a schema the client should have
    // refused can be shown never to have been sent
    int32_t create_table_requests = 0;
    std::string last_commit_body;
    std::string last_commit_table;
    bool refuse_commit = false;
    bool break_commit_response = false;
    bool commit_response_without_outcome = false;
    bool accept_commit_then_report_unavailable = false;
    int32_t accepted_commit_response_code = 503;
    int32_t commit_requests = 0;
    std::optional<std::string> current_snapshot;
    bool null_snapshot_response = false;
    std::string last_snapshot_table;
    // guards all fields above: the handler runs on the server's accept thread while
    // tests seed and inspect the state
    std::mutex mutex;
};

MockRestServer::Response JsonResponse(int32_t code, const std::string& body) {
    MockRestServer::Response response;
    response.code = code;
    response.body = body;
    return response;
}

MockRestServer::Response MockError(int32_t code, const std::string& resource_type,
                                   const std::string& resource_name, const std::string& message) {
    ErrorResponse error(resource_type, resource_name, message, code);
    return JsonResponse(code, error.ToJsonString().value());
}

std::string SnapshotJson(int64_t id) {
    return fmt::format(
        R"({{"version":3,"id":{},"schemaId":0,"baseManifestList":"bml","deltaManifestList":"dml",)"
        R"("commitUser":"user1","commitIdentifier":1,"commitKind":"APPEND","timeMillis":100,)"
        R"("totalRecordCount":10,"deltaRecordCount":1}})",
        id);
}

std::string TableResponseJson(const std::string& name, const MockCatalogState::TableData& table) {
    std::string id_member = table.id.empty() ? "" : fmt::format(R"("id":"{}",)", table.id);
    return fmt::format(
        R"({{{}"name":"{}","path":"{}","isExternal":false,"schemaId":{},"schema":{},)"
        R"("owner":"owner1","updatedAt":123}})",
        id_member, name, table.path, table.schema_id, table.schema_json);
}

// Serves `names` one item per page to exercise the pagination loop of the client.
std::pair<std::vector<std::string>, std::optional<std::string>> PageOf(
    const std::vector<std::string>& names, const MockRestServer::Request& request) {
    size_t index = 0;
    auto token_iter = request.query_params.find(RestApi::kQueryParamPageToken);
    if (token_iter != request.query_params.end()) {
        // A malformed token must not throw: an exception on the accept thread would
        // terminate the test binary.
        index = static_cast<size_t>(
            StringUtils::StringToValue<uint64_t>(token_iter->second).value_or(0));
    }
    std::vector<std::string> page;
    std::optional<std::string> next_page_token;
    if (index < names.size()) {
        page.push_back(names[index]);
        if (index + 1 < names.size()) {
            next_page_token = std::to_string(index + 1);
        }
    }
    return {page, next_page_token};
}

// Implements the subset of the rest catalog protocol used by `RestCatalog` on top of
// `MockCatalogState`.
MockRestServer::Response HandleCatalogRequest(MockCatalogState* state,
                                              const MockRestServer::Request& request) {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->last_headers = request.headers;
    auto auth_iter = request.headers.find("authorization");
    if (auth_iter == request.headers.end() ||
        auth_iter->second != std::string("Bearer ") + kToken) {
        return MockError(401, "", "", "invalid token");
    }
    if (request.path == "/v1/config") {
        auto warehouse_iter = request.query_params.find("warehouse");
        if (warehouse_iter == request.query_params.end() || warehouse_iter->second != kWarehouse) {
            return MockError(400, "", "", "unexpected warehouse");
        }
        ConfigResponse config(
            {{RestApi::kOptionUrlPrefix, kPrefix},
             {"header.x-server-header", "from-config"},
             {"table-default.write-only", "true"},
             {"table-default.bucket", "8"}},
            {{"server-override", "from-server"}, {"header.x-shared-header", "from-config"}});
        return JsonResponse(200, config.ToJsonString().value());
    }
    if (state->force_error_code) {
        return MockError(state->force_error_code.value(), "", "", "injected failure");
    }
    const std::string base = std::string("/v1/") + kPrefix;
    if (request.path.rfind(base, 0) != 0) {
        return MockError(404, "", "", "unknown path " + request.path);
    }
    std::string rest = request.path.substr(base.size());

    if (rest == "/databases") {
        if (request.method == "GET") {
            std::vector<std::string> names;
            for (const auto& [name, tables] : state->databases) {
                names.push_back(name);
            }
            auto [page, next_page_token] = PageOf(names, request);
            ListDatabasesResponse response(page, next_page_token);
            return JsonResponse(200, response.ToJsonString().value());
        }
        if (request.method == "POST") {
            CreateDatabaseRequest create_request("", {});
            if (!RapidJsonUtil::FromJsonString(request.body, &create_request).ok()) {
                return MockError(400, "", "", "bad create database request");
            }
            if (state->databases.count(create_request.GetName()) > 0) {
                return MockError(409, ErrorResponse::kResourceTypeDatabase,
                                 create_request.GetName(), "database already exists");
            }
            state->databases[create_request.GetName()] = {};
            return JsonResponse(200, "");
        }
        return MockError(400, "", "", "unsupported method");
    }

    if (rest == "/tables/rename" && request.method == "POST") {
        RenameTableRequest rename_request("", "", "", "");
        if (!RapidJsonUtil::FromJsonString(request.body, &rename_request).ok()) {
            return MockError(400, "", "", "bad rename table request");
        }
        auto db_iter = state->databases.find(rename_request.GetSourceDatabase());
        if (db_iter == state->databases.end() ||
            db_iter->second.count(rename_request.GetSourceTable()) == 0) {
            return MockError(404, ErrorResponse::kResourceTypeTable,
                             rename_request.GetSourceTable(), "table not found");
        }
        auto& dest_tables = state->databases[rename_request.GetDestinationDatabase()];
        if (dest_tables.count(rename_request.GetDestinationTable()) > 0) {
            return MockError(409, ErrorResponse::kResourceTypeTable,
                             rename_request.GetDestinationTable(), "table already exists");
        }
        dest_tables[rename_request.GetDestinationTable()] =
            db_iter->second[rename_request.GetSourceTable()];
        db_iter->second.erase(rename_request.GetSourceTable());
        return JsonResponse(200, "");
    }

    const std::string databases_prefix = "/databases/";
    if (rest.rfind(databases_prefix, 0) != 0) {
        return MockError(404, "", "", "unknown path " + request.path);
    }
    std::string remainder = rest.substr(databases_prefix.size());
    size_t tables_pos = remainder.find("/tables");

    const std::string token_suffix = "/token";
    if (tables_pos != std::string::npos && remainder.size() > token_suffix.size() &&
        remainder.compare(remainder.size() - token_suffix.size(), token_suffix.size(),
                          token_suffix) == 0) {
        // The mock issues credentials for any table, so that a test can ask for the token
        // of a branch or of a table it did not seed.
        state->token_requests.push_back(request.path);
        std::map<std::string, std::string> credentials = {{"fs.oss.accessKeyId", "ak-1"}};
        auto database = state->databases.find(remainder.substr(0, tables_pos));
        if (database != state->databases.end()) {
            size_t table_start = tables_pos + std::strlen("/tables/");
            std::string table_name =
                remainder.substr(table_start, remainder.size() - table_start - token_suffix.size());
            auto table = database->second.find(table_name);
            if (table != database->second.end()) {
                credentials = table->second.token;
            }
        }
        GetTableTokenResponse token(credentials, kDataTokenExpiresAtMillis);
        return JsonResponse(200, token.ToJsonString().value());
    }

    if (tables_pos == std::string::npos) {
        const std::string& db_name = remainder;
        auto db_iter = state->databases.find(db_name);
        if (request.method == "GET") {
            if (db_iter == state->databases.end()) {
                return MockError(404, ErrorResponse::kResourceTypeDatabase, db_name,
                                 "database not found");
            }
            std::string body = fmt::format(
                R"({{"id":"1","name":"{}","location":"{}/{}.db","options":{{"dbk":"dbv"}}}})",
                db_name, kWarehouse, db_name);
            return JsonResponse(200, body);
        }
        if (request.method == "DELETE") {
            if (db_iter == state->databases.end()) {
                return MockError(404, ErrorResponse::kResourceTypeDatabase, db_name,
                                 "database not found");
            }
            state->databases.erase(db_iter);
            return JsonResponse(200, "");
        }
        return MockError(400, "", "", "unsupported method");
    }

    std::string db_name = remainder.substr(0, tables_pos);
    auto db_iter = state->databases.find(db_name);
    if (db_iter == state->databases.end()) {
        return MockError(404, ErrorResponse::kResourceTypeDatabase, db_name, "database not found");
    }
    auto& tables = db_iter->second;
    std::string table_part = remainder.substr(tables_pos + std::strlen("/tables"));

    if (table_part.empty()) {
        if (request.method == "GET") {
            std::vector<std::string> names;
            for (const auto& [name, table] : tables) {
                names.push_back(name);
            }
            auto [page, next_page_token] = PageOf(names, request);
            ListTablesResponse response(page, next_page_token);
            return JsonResponse(200, response.ToJsonString().value());
        }
        if (request.method == "POST") {
            state->create_table_requests++;
            CreateTableRequest create_request("", "", "");
            if (!RapidJsonUtil::FromJsonString(request.body, &create_request).ok()) {
                return MockError(400, "", "", "bad create table request");
            }
            if (tables.count(create_request.GetTable()) > 0) {
                return MockError(409, ErrorResponse::kResourceTypeTable, create_request.GetTable(),
                                 "table already exists");
            }
            MockCatalogState::TableData table;
            table.schema_json = create_request.GetSchemaJson();
            table.schema_id = 0;
            table.path = fmt::format("{}/{}.db/{}", kWarehouse, db_name, create_request.GetTable());
            tables[create_request.GetTable()] = table;
            return JsonResponse(200, "");
        }
        return MockError(400, "", "", "unsupported method");
    }

    std::string table_name = table_part.substr(1);
    bool list_snapshots = false;
    bool commit_table = false;
    bool load_snapshot = false;
    const std::string snapshots_suffix = "/snapshots";
    const std::string snapshot_suffix = "/snapshot";
    const std::string commit_suffix = "/commit";
    if (table_name.size() > snapshots_suffix.size() &&
        table_name.compare(table_name.size() - snapshots_suffix.size(), snapshots_suffix.size(),
                           snapshots_suffix) == 0) {
        table_name = table_name.substr(0, table_name.size() - snapshots_suffix.size());
        list_snapshots = true;
    } else if (table_name.size() > commit_suffix.size() &&
               table_name.compare(table_name.size() - commit_suffix.size(), commit_suffix.size(),
                                  commit_suffix) == 0) {
        table_name = table_name.substr(0, table_name.size() - commit_suffix.size());
        commit_table = true;
    } else if (table_name.size() > snapshot_suffix.size() &&
               table_name.compare(table_name.size() - snapshot_suffix.size(),
                                  snapshot_suffix.size(), snapshot_suffix) == 0) {
        table_name = table_name.substr(0, table_name.size() - snapshot_suffix.size());
        load_snapshot = true;
    }
    auto table_iter = tables.find(table_name);
    if (table_iter == tables.end()) {
        return MockError(404, ErrorResponse::kResourceTypeTable, table_name, "table not found");
    }
    if (commit_table) {
        if (request.method != "POST") {
            return MockError(400, "", "", "unsupported method");
        }
        state->last_commit_body = request.body;
        state->last_commit_table = table_name;
        ++state->commit_requests;
        if (state->accept_commit_then_report_unavailable) {
            if (state->commit_requests == 1) {
                Result<CommitTableRequest> taken = CommitTableRequest::FromJsonString(request.body);
                if (taken.ok()) {
                    Result<std::string> json = taken.value().GetSnapshot().ToJsonString();
                    if (json.ok()) {
                        state->current_snapshot = json.value();
                    }
                }
                if (state->accepted_commit_response_code == 200) {
                    return JsonResponse(200, R"({"success": null})");
                }
                if (state->accepted_commit_response_code == 204) {
                    return JsonResponse(204, "");
                }
                MockRestServer::Response response = MockError(state->accepted_commit_response_code,
                                                              "", "", "commit outcome unknown");
                response.headers["Location"] = request.path;
                return response;
            }
            bool based_on_held = false;
            Result<CommitTableRequest> later = CommitTableRequest::FromJsonString(request.body);
            if (later.ok() && state->current_snapshot) {
                Result<Snapshot> held = Snapshot::FromJsonString(state->current_snapshot.value());
                based_on_held =
                    held.ok() && later.value().GetBaseSnapshotUuid() == held.value().Uuid();
                if (based_on_held) {
                    Result<std::string> json = later.value().GetSnapshot().ToJsonString();
                    if (json.ok()) {
                        state->current_snapshot = json.value();
                    }
                }
            }
            return JsonResponse(200, CommitTableResponse(based_on_held).ToJsonString().value());
        }
        if (state->break_commit_response) {
            return JsonResponse(200, "not json");
        }
        if (state->commit_response_without_outcome) {
            return JsonResponse(200, R"({"success": null})");
        }
        CommitTableResponse response(!state->refuse_commit);
        return JsonResponse(200, response.ToJsonString().value());
    }
    if (load_snapshot) {
        if (request.method != "GET") {
            return MockError(400, "", "", "unsupported method");
        }
        state->last_snapshot_table = table_name;
        if (state->null_snapshot_response) {
            return JsonResponse(200, R"({"snapshot": null})");
        }
        if (!state->current_snapshot) {
            return MockError(404, ErrorResponse::kResourceTypeSnapshot, table_name,
                             "snapshot not found");
        }
        return JsonResponse(200, fmt::format(R"({{"snapshot":{{"snapshot":{},"recordCount":1,)"
                                             R"("fileSizeInBytes":2,"fileCount":3,)"
                                             R"("lastFileCreationTime":4}}}})",
                                             state->current_snapshot.value()));
    }
    if (list_snapshots) {
        // two pages, out of order to exercise pagination and sorting
        auto token_iter = request.query_params.find(RestApi::kQueryParamPageToken);
        if (token_iter == request.query_params.end()) {
            return JsonResponse(
                200, fmt::format(R"({{"snapshots":[{}],"nextPageToken":"1"}})", SnapshotJson(2)));
        }
        return JsonResponse(200, fmt::format(R"({{"snapshots":[{}]}})", SnapshotJson(1)));
    }
    if (request.method == "GET") {
        state->get_table_requests++;
        return JsonResponse(200, TableResponseJson(table_name, table_iter->second));
    }
    if (request.method == "DELETE") {
        tables.erase(table_iter);
        return JsonResponse(200, "");
    }
    return MockError(400, "", "", "unsupported method");
}

}  // namespace

class RestCatalogTest : public ::testing::Test {
 protected:
    void SetUp() override {
        state_ = std::make_shared<MockCatalogState>();
        ASSERT_OK_AND_ASSIGN(
            server_, MockRestServer::Start([state = state_](const MockRestServer::Request& req) {
                return HandleCatalogRequest(state.get(), req);
            }));
        options_ = {
            {CatalogOptions::METASTORE, "rest"},
            {CatalogOptions::URI, server_->GetBaseUri()},
            {CatalogOptions::TOKEN_PROVIDER, "bear"},
            {CatalogOptions::TOKEN, kToken},
            {Options::FILE_SYSTEM, "local"},
            // mock_format is linked statically into the test binary, so its factory is
            // registered in the binary's own registry even when the real format plugin
            // dylibs register into a different one (macOS two-level namespace)
            {Options::FILE_FORMAT, "mock_format"},
            {"header.x-client-header", "from-client"},
            {"header.x-shared-header", "from-client"},
        };
    }

    void TearDown() override {
        if (server_) {
            server_->Stop();
        }
    }

    Result<std::unique_ptr<RestCatalog>> CreateRestCatalog(
        const RestHttpClient::Config& http_config = RestHttpClient::Config()) {
        return RestCatalog::Create(kWarehouse, options_, nullptr, http_config);
    }

    static RestHttpClient::Config FastRetryConfig() {
        RestHttpClient::Config config;
        config.retry_base_delay_ms = 1;
        return config;
    }

    Status CreateSampleTable(Catalog* catalog, const Identifier& identifier,
                             bool ignore_if_exists = false) {
        std::shared_ptr<arrow::Schema> schema =
            arrow::schema({arrow::field("f0", arrow::int32(), /*nullable=*/false),
                           arrow::field("f1", arrow::utf8())});
        struct ArrowSchema c_schema;
        if (!arrow::ExportSchema(*schema, &c_schema).ok()) {
            return Status::Invalid("failed to export arrow schema");
        }
        Status status =
            catalog->CreateTable(identifier, &c_schema, /*partition_keys=*/{},
                                 /*primary_keys=*/{"f0"}, {{"bucket", "2"}}, ignore_if_exists);
        // CreateTable takes ownership of the exported schema only once it reaches
        // arrow::ImportSchema, which an identifier rejected by its checks never does
        if (c_schema.release != nullptr) {
            c_schema.release(&c_schema);
        }
        return status;
    }

    // Seeds `schema_json` as table `table_name` of "db1" behind the mock server and
    // expects loading the table to fail with an Invalid status carrying
    // `expected_message`.
    void ExpectBrokenSchemaRejected(Catalog* catalog, const std::string& table_name,
                                    const std::string& schema_json,
                                    const std::string& expected_message) {
        MockCatalogState::TableData table_data;
        table_data.schema_json = schema_json;
        table_data.path = "wh1/db1.db/" + table_name;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            state_->databases["db1"][table_name] = table_data;
        }
        Status status = catalog->GetTable(Identifier("db1", table_name)).status();
        ASSERT_TRUE(status.IsInvalid()) << status.ToString();
        ASSERT_NOK_WITH_MSG(status, expected_message);
    }

    std::shared_ptr<MockCatalogState> state_;
    std::unique_ptr<MockRestServer> server_;
    std::map<std::string, std::string> options_;

    // Path the data token of `database`.`table` is requested at.
    static std::string TokenPath(const std::string& database, const std::string& table) {
        return fmt::format("/v1/{}/databases/{}/tables/{}/token", kPrefix, database, table);
    }

    std::vector<std::string> TokenRequests() {
        std::lock_guard<std::mutex> lock(state_->mutex);
        return state_->token_requests;
    }
};

TEST_F(RestCatalogTest, CreateMergesServerConfig) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    const std::map<std::string, std::string>& merged = catalog->GetOptions();
    ASSERT_EQ(kPrefix, merged.at(RestApi::kOptionUrlPrefix));
    ASSERT_EQ("from-server", merged.at("server-override"));
    ASSERT_EQ(kWarehouse, catalog->GetRootPath());
    ASSERT_NE(nullptr, catalog->GetFileSystem());
}

TEST_F(RestCatalogTest, TableFileSystemWithoutDataToken) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         catalog->GetTableFileSystem(Identifier("db1", "t1")));
    // without the data token the catalog wide credentials are used for the data as well
    ASSERT_EQ(catalog->GetFileSystem(), fs);
}

TEST_F(RestCatalogTest, TableFileSystemWithDataToken) {
    options_[CatalogOptions::DATA_TOKEN_ENABLED] = "true";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         catalog->GetTableFileSystem(Identifier("db1", "t1")));
    ASSERT_NE(nullptr, fs);
    ASSERT_NE(catalog->GetFileSystem(), fs);

    // building it asks the server for nothing: the credentials of the table it is bound to
    // are loaded when it is first used
    ASSERT_TRUE(TokenRequests().empty());
    ASSERT_OK_AND_ASSIGN(bool exists, fs->Exists("/no-such-file"));
    ASSERT_FALSE(exists);
    ASSERT_EQ(std::vector<std::string>({TokenPath("db1", "t1")}), TokenRequests());

    // a system table reads the files of the table it belongs to, so it is served the
    // credentials of that table
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> system_table_fs,
                         catalog->GetTableFileSystem(Identifier("db1", "t1$snapshots")));
    ASSERT_OK(system_table_fs->Exists("/no-such-file").status());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> other_table_fs,
                         catalog->GetTableFileSystem(Identifier("db1", "t2")));
    ASSERT_OK(other_table_fs->Exists("/no-such-file").status());
    ASSERT_EQ(std::vector<std::string>(
                  {TokenPath("db1", "t1"), TokenPath("db1", "t1"), TokenPath("db1", "t2")}),
              TokenRequests());
}

TEST_F(RestCatalogTest, TableFileSystemIsBoundToTheTableItWasAskedFor) {
    options_[CatalogOptions::DATA_TOKEN_ENABLED] = "true";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());

    // the database and the table are addressed separately, so identifiers that print the
    // same must not be served the credentials of one another
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> dotted_database,
                         catalog->GetTableFileSystem(Identifier("db1.a", "t1")));
    ASSERT_OK(dotted_database->Exists("/no-such-file").status());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> dotted_table,
                         catalog->GetTableFileSystem(Identifier("db1", "a.t1")));
    ASSERT_OK(dotted_table->Exists("/no-such-file").status());
    ASSERT_EQ(std::vector<std::string>({TokenPath("db1.a", "t1"), TokenPath("db1", "a.t1")}),
              TokenRequests());
}

TEST_F(RestCatalogTest, TableFileSystemNormalizesTheBranch) {
    options_[CatalogOptions::DATA_TOKEN_ENABLED] = "true";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());

    // the main branch is the table itself, so it shares the credentials
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> main_branch_fs,
                         catalog->GetTableFileSystem(Identifier("db1", "t1$branch_main")));
    ASSERT_OK(main_branch_fs->Exists("/no-such-file").status());

    // another branch is addressed as its own object on the server, so it gets its own
    // credentials
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> branch_fs,
                         catalog->GetTableFileSystem(Identifier("db1", "t1$branch_b1")));
    ASSERT_OK(branch_fs->Exists("/no-such-file").status());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> branch_system_fs,
                         catalog->GetTableFileSystem(Identifier("db1", "t1$branch_b1$snapshots")));
    ASSERT_OK(branch_system_fs->Exists("/no-such-file").status());
    ASSERT_EQ(std::vector<std::string>({TokenPath("db1", "t1"), TokenPath("db1", "t1$branch_b1"),
                                        TokenPath("db1", "t1$branch_b1")}),
              TokenRequests());
}

TEST_F(RestCatalogTest, TableFileSystemIsNotRetainedPerTable) {
    options_[CatalogOptions::DATA_TOKEN_ENABLED] = "true";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());

    // The catalog remembers no file system of a table, so a table dropped and recreated at
    // another location is never served the credentials of the dropped one. What is cached
    // and bounded are the file systems built from the credentials, keyed by them.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> first,
                         catalog->GetTableFileSystem(Identifier("db1", "t1")));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> second,
                         catalog->GetTableFileSystem(Identifier("db1", "t1")));
    ASSERT_NE(first, second);

    ASSERT_OK(first->Exists("/no-such-file").status());
    ASSERT_OK(second->Exists("/no-such-file").status());
    ASSERT_EQ(std::vector<std::string>({TokenPath("db1", "t1"), TokenPath("db1", "t1")}),
              TokenRequests());
}

TEST_F(RestCatalogTest, RecreatedTableLoadsNewCredentialsBeforeOldTokenExpires) {
    options_[CatalogOptions::DATA_TOKEN_ENABLED] = "true";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    Identifier identifier("db1", "t1");
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(CreateSampleTable(catalog.get(), identifier));
    ASSERT_OK_AND_ASSIGN(std::string old_location, catalog->GetTableLocation(identifier));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> first,
                         catalog->GetTableFileSystem(identifier));
    std::shared_ptr<RestTokenFileSystem> first_token_fs =
        std::dynamic_pointer_cast<RestTokenFileSystem>(first);
    ASSERT_NE(nullptr, first_token_fs);
    ASSERT_OK_AND_ASSIGN(RestToken old_token, first_token_fs->ValidToken());
    ASSERT_EQ("ak-1", old_token.token.at("fs.oss.accessKeyId"));

    ASSERT_OK(catalog->DropTable(identifier, /*ignore_if_not_exists=*/false));
    ASSERT_OK_AND_ASSIGN(bool exists, catalog->TableExists(identifier));
    ASSERT_FALSE(exists);
    ASSERT_OK(CreateSampleTable(catalog.get(), identifier));
    std::string new_location = old_location + "-recreated";
    {
        // the server hands the recreated table another location and other credentials, while
        // the expiration of the credentials of the dropped table stays the same
        std::lock_guard<std::mutex> lock(state_->mutex);
        MockCatalogState::TableData& table = state_->databases.at("db1").at("t1");
        table.path = new_location;
        table.token = {{"fs.oss.accessKeyId", "ak-2"}};
    }
    ASSERT_OK_AND_ASSIGN(std::string location, catalog->GetTableLocation(identifier));
    ASSERT_EQ(new_location, location);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> second,
                         catalog->GetTableFileSystem(identifier));
    ASSERT_NE(first, second);
    std::shared_ptr<RestTokenFileSystem> second_token_fs =
        std::dynamic_pointer_cast<RestTokenFileSystem>(second);
    ASSERT_NE(nullptr, second_token_fs);
    ASSERT_OK_AND_ASSIGN(RestToken new_token, second_token_fs->ValidToken());
    ASSERT_EQ("ak-2", new_token.token.at("fs.oss.accessKeyId"));
    ASSERT_EQ(old_token.expires_at_millis, new_token.expires_at_millis);
    ASSERT_EQ(kDataTokenExpiresAtMillis, new_token.expires_at_millis);
    ASSERT_EQ(std::vector<std::string>({TokenPath("db1", "t1"), TokenPath("db1", "t1")}),
              TokenRequests());
}

TEST_F(RestCatalogTest, CatalogFactoryMetastoreDispatch) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Catalog> catalog, Catalog::Create(kWarehouse, options_));
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> databases, catalog->ListDatabases());
    ASSERT_TRUE(databases.empty());
    options_[CatalogOptions::METASTORE] = "something-else";
    Status status = Catalog::Create(kWarehouse, options_).status();
    ASSERT_TRUE(status.IsInvalid()) << status.ToString();
    ASSERT_NOK_WITH_MSG(status, "unsupported metastore");
}

TEST_F(RestCatalogTest, CreateWithWrongTokenFails) {
    options_[CatalogOptions::TOKEN] = "wrong-token";
    ASSERT_NOK_WITH_MSG(CreateRestCatalog().status(), "not authorized");
}

TEST_F(RestCatalogTest, CreateRejectsInvalidOptions) {
    // all rejected by client side validation, before any request reaches the server
    const std::map<std::string, std::string> valid_options = options_;

    options_.erase(CatalogOptions::URI);
    ASSERT_NOK_WITH_MSG(CreateRestCatalog().status(), "'uri' must be configured");

    options_ = valid_options;
    options_.erase(CatalogOptions::TOKEN_PROVIDER);
    ASSERT_NOK_WITH_MSG(CreateRestCatalog().status(), "'token.provider' must be configured");

    options_ = valid_options;
    options_[CatalogOptions::TOKEN_PROVIDER] = "unsupported";
    Status unsupported_provider = CreateRestCatalog().status();
    ASSERT_TRUE(unsupported_provider.IsNotImplemented()) << unsupported_provider.ToString();
    ASSERT_NOK_WITH_MSG(unsupported_provider, "unsupported token provider");

    options_ = valid_options;
    options_.erase(CatalogOptions::TOKEN);
    ASSERT_NOK_WITH_MSG(CreateRestCatalog().status(), "bear token provider");
}

TEST_F(RestCatalogTest, DatabaseOperations) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());

    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateDatabase("db2", {}, /*ignore_if_exists=*/false));
    Status duplicated = catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false);
    ASSERT_TRUE(duplicated.IsExist()) << duplicated.ToString();
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/true));

    // the mock server returns one database per page
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> databases, catalog->ListDatabases());
    ASSERT_EQ((std::vector<std::string>{"db1", "db2"}), databases);

    ASSERT_OK_AND_ASSIGN(bool exists, catalog->DatabaseExists("db1"));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(exists, catalog->DatabaseExists("db3"));
    ASSERT_FALSE(exists);

    ASSERT_OK_AND_ASSIGN(std::string db1_location, catalog->GetDatabaseLocation("db1"));
    ASSERT_EQ("wh1/db1.db", db1_location);
    // the location is resolved on the server, so an unknown database is reported as an error
    Status no_location = catalog->GetDatabaseLocation("db3").status();
    ASSERT_TRUE(no_location.IsNotExist()) << no_location.ToString();
    // the virtual "sys" database is never asked about and has no location
    ASSERT_OK_AND_ASSIGN(std::string sys_location, catalog->GetDatabaseLocation("sys"));
    ASSERT_EQ("", sys_location);

    ASSERT_OK(catalog->DropDatabase("db2", /*ignore_if_not_exists=*/false, /*cascade=*/false));
    ASSERT_OK(catalog->DropDatabase("db2", /*ignore_if_not_exists=*/true, /*cascade=*/false));
    Status missing = catalog->DropDatabase("db2", /*ignore_if_not_exists=*/false,
                                           /*cascade=*/false);
    ASSERT_TRUE(missing.IsNotExist()) << missing.ToString();

    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t1")));
    ASSERT_NOK_WITH_MSG(
        catalog->DropDatabase("db1", /*ignore_if_not_exists=*/false, /*cascade=*/false),
        "non-empty database");
    // cascade drop skips the emptiness check
    ASSERT_OK(catalog->DropDatabase("db1", /*ignore_if_not_exists=*/false, /*cascade=*/true));
    ASSERT_OK_AND_ASSIGN(exists, catalog->DatabaseExists("db1"));
    ASSERT_FALSE(exists);
}

TEST_F(RestCatalogTest, TableOperations) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    Identifier identifier("db1", "t1");

    Status missing_db = CreateSampleTable(catalog.get(), Identifier("db_missing", "t1"));
    ASSERT_TRUE(missing_db.IsNotExist()) << missing_db.ToString();

    ASSERT_OK(CreateSampleTable(catalog.get(), identifier));
    Status duplicated = CreateSampleTable(catalog.get(), identifier);
    ASSERT_TRUE(duplicated.IsExist()) << duplicated.ToString();
    ASSERT_OK(CreateSampleTable(catalog.get(), identifier, /*ignore_if_exists=*/true));

    ASSERT_OK_AND_ASSIGN(std::vector<std::string> tables, catalog->ListTables("db1"));
    ASSERT_EQ((std::vector<std::string>{"t1"}), tables);
    Status list_missing = catalog->ListTables("db_missing").status();
    ASSERT_TRUE(list_missing.IsNotExist()) << list_missing.ToString();

    ASSERT_OK_AND_ASSIGN(bool exists, catalog->TableExists(identifier));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("db1", "t2")));
    ASSERT_FALSE(exists);

    ASSERT_OK_AND_ASSIGN(std::string location, catalog->GetTableLocation(identifier));
    ASSERT_EQ("wh1/db1.db/t1", location);

    int32_t requests_before_get_table = 0;
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        requests_before_get_table = state_->get_table_requests;
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog->GetTable(identifier));
    ASSERT_EQ("t1", table->Name());
    ASSERT_EQ("1", table->Uuid());
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ(requests_before_get_table + 1, state_->get_table_requests);
    }
    std::shared_ptr<DataSchema> schema =
        std::dynamic_pointer_cast<DataSchema>(table->LatestSchema());
    ASSERT_NE(nullptr, schema);
    ASSERT_EQ((std::vector<std::string>{"f0", "f1"}), schema->FieldNames());
    ASSERT_EQ((std::vector<std::string>{"f0"}), schema->PrimaryKeys());
    ASSERT_EQ(0, schema->Id());
    // options are enriched with the table path and audit info from the server
    ASSERT_EQ("wh1/db1.db/t1", schema->Options().at("path"));
    ASSERT_EQ("owner1", schema->Options().at("owner"));

    // "table-default." options of the merged config apply only where the caller left the
    // option unset: "write-only" is taken from the config, "bucket" keeps the value passed
    // to CreateTable instead of the configured "table-default.bucket" of 8
    ASSERT_EQ("true", schema->Options().at("write-only"));
    ASSERT_EQ("2", schema->Options().at("bucket"));

    // timeMillis is backed by the server's audit "updatedAt" instead of the current
    // time, keeping the conversion deterministic
    std::shared_ptr<TableSchema> table_schema =
        std::dynamic_pointer_cast<TableSchema>(table->LatestSchema());
    ASSERT_NE(nullptr, table_schema);
    ASSERT_EQ(123, table_schema->TimeMillis());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> loaded_schema,
                         catalog->LoadTableSchema(identifier));
    ASSERT_EQ((std::vector<std::string>{"f0", "f1"}), loaded_schema->FieldNames());
    Status schema_missing = catalog->LoadTableSchema(Identifier("db1", "t2")).status();
    ASSERT_TRUE(schema_missing.IsNotExist()) << schema_missing.ToString();

    // a schema comment of the server response is carried into the table schema
    MockCatalogState::TableData commented;
    commented.schema_json = R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                            R"( "partitionKeys": [], "primaryKeys": [], "options": {},)"
                            R"( "comment": "a table comment"})";
    commented.path = "wh1/db1.db/commented";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["commented"] = commented;
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> commented_table,
                         catalog->GetTable(Identifier("db1", "commented")));
    ASSERT_EQ("a table comment", commented_table->LatestSchema()->Comment().value_or(""));

    ASSERT_OK(catalog->RenameTable(identifier, Identifier("db1", "t2"),
                                   /*ignore_if_not_exists=*/false));
    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("db1", "t2")));
    ASSERT_TRUE(exists);
    ASSERT_OK(catalog->RenameTable(identifier, Identifier("db1", "t3"),
                                   /*ignore_if_not_exists=*/true));
    Status rename_missing = catalog->RenameTable(identifier, Identifier("db1", "t3"),
                                                 /*ignore_if_not_exists=*/false);
    ASSERT_TRUE(rename_missing.IsNotExist()) << rename_missing.ToString();

    ASSERT_OK(catalog->DropTable(Identifier("db1", "t2"), /*ignore_if_not_exists=*/false));
    ASSERT_OK(catalog->DropTable(Identifier("db1", "t2"), /*ignore_if_not_exists=*/true));
    Status drop_missing = catalog->DropTable(Identifier("db1", "t2"),
                                             /*ignore_if_not_exists=*/false);
    ASSERT_TRUE(drop_missing.IsNotExist()) << drop_missing.ToString();
}

TEST_F(RestCatalogTest, TableUuidFallsBackToFullNameWithoutServerId) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, false));
    Identifier identifier("db1", "t1");
    ASSERT_OK(CreateSampleTable(catalog.get(), identifier));
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["t1"].id.clear();
    }

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog->GetTable(identifier));
    ASSERT_EQ("db1.t1", table->Uuid());
    ASSERT_EQ(table->CatalogUuid(), std::nullopt);

    ASSERT_OK_AND_ASSIGN(bool committed,
                         catalog->CommitSnapshot(identifier, table->CatalogUuid(), std::nullopt,
                                                 BuildTestSnapshot(1), {}));
    ASSERT_TRUE(committed);
    std::string commit_body;
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        commit_body = state_->last_commit_body;
    }
    ASSERT_OK_AND_ASSIGN(CommitTableRequest sent, CommitTableRequest::FromJsonString(commit_body));
    ASSERT_EQ(sent.GetTableId(), std::nullopt);
    ASSERT_NE(commit_body.find("\"tableId\": null"), std::string::npos) << commit_body;

    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["t1"].id = "server-side-id";
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> identified, catalog->GetTable(identifier));
    ASSERT_EQ(identified->CatalogUuid(), std::optional<std::string>("server-side-id"));
    ASSERT_EQ("server-side-id", identified->Uuid());
}

TEST_F(RestCatalogTest, ClientAndServerHeadersAreSent) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> databases, catalog->ListDatabases());
    // "header." options from both the client and the merged server config are sent as
    // http headers on every request
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ("from-client", state_->last_headers.at("x-client-header"));
        ASSERT_EQ("from-config", state_->last_headers.at("x-server-header"));
        // when the client and the server config set the same "header." option, the
        // merged config wins (overrides > client options > defaults)
        ASSERT_EQ("from-config", state_->last_headers.at("x-shared-header"));
        ASSERT_EQ(std::string("Bearer ") + kToken, state_->last_headers.at("authorization"));
    }
    // a request carrying a body declares the json content type (set before the auth
    // headers are merged, so a signing auth provider covers it)
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ("application/json", state_->last_headers.at("content-type"));
    }
}

TEST_F(RestCatalogTest, SystemTableSchema) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t1")));

    // the "options" system table has a static schema and needs no file system access
    Identifier system_identifier("db1", "t1$options");
    ASSERT_OK_AND_ASSIGN(bool exists, catalog->TableExists(system_identifier));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("db1", "t9$options")));
    ASSERT_FALSE(exists);
    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("db1", "t1$unsupported")));
    ASSERT_FALSE(exists);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> schema,
                         catalog->LoadTableSchema(system_identifier));
    ASSERT_EQ((std::vector<std::string>{"key", "value"}), schema->FieldNames());

    int32_t requests_before = 0;
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        requests_before = state_->get_table_requests;
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog->GetTable(system_identifier));
    ASSERT_EQ("t1$options", table->Name());
    ASSERT_EQ((std::vector<std::string>{"key", "value"}), table->LatestSchema()->FieldNames());
    ASSERT_EQ("db1.t1$options", table->Uuid());
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ(requests_before + 1, state_->get_table_requests);
    }

    Status unsupported = catalog->LoadTableSchema(Identifier("db1", "t1$unsupported")).status();
    ASSERT_TRUE(unsupported.IsNotExist()) << unsupported.ToString();
}

TEST_F(RestCatalogTest, BranchTableLoadsBranchSchemaFromServer) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t1")));

    // the identifier sent to the server keeps the branch, so the server resolves it and
    // returns the branch's own schema. The path it reports is the data table root, not
    // the branch subdirectory: readers derive "<path>/branch/branch-<name>" from the
    // branch option, so a branch path here would be applied twice
    MockCatalogState::TableData branch_data;
    branch_data.schema_json = R"({"fields": [{"id": 0, "name": "b0", "type": "INT NOT NULL"}],)"
                              R"( "partitionKeys": [], "primaryKeys": [], "options": {}})";
    branch_data.schema_id = 3;
    branch_data.path = "wh1/db1.db/t1";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["t1$branch_b1"] = branch_data;
    }

    Identifier branch_identifier("db1", "t1$branch_b1");
    ASSERT_OK_AND_ASSIGN(bool exists, catalog->TableExists(branch_identifier));
    ASSERT_TRUE(exists);
    // a branch the server does not know is missing instead of silently falling back to
    // the main table
    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("db1", "t1$branch_missing")));
    ASSERT_FALSE(exists);

    ASSERT_OK_AND_ASSIGN(std::string location, catalog->GetTableLocation(branch_identifier));
    ASSERT_EQ("wh1/db1.db/t1", location);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog->GetTable(branch_identifier));
    ASSERT_EQ("t1$branch_b1", table->Name());
    std::shared_ptr<DataSchema> schema =
        std::dynamic_pointer_cast<DataSchema>(table->LatestSchema());
    ASSERT_NE(nullptr, schema);
    // the branch's own schema and schema id, not the main table's
    ASSERT_EQ((std::vector<std::string>{"b0"}), schema->FieldNames());
    ASSERT_EQ(3, schema->Id());
    ASSERT_EQ("b1", schema->Options().at(Options::BRANCH));

    // the default branch is addressed as the bare table: "t1$branch_main" resolves to
    // "t1" and carries no branch option
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> main_table,
                         catalog->GetTable(Identifier("db1", "t1$branch_main")));
    std::shared_ptr<DataSchema> main_schema =
        std::dynamic_pointer_cast<DataSchema>(main_table->LatestSchema());
    ASSERT_NE(nullptr, main_schema);
    ASSERT_EQ((std::vector<std::string>{"f0", "f1"}), main_schema->FieldNames());
    ASSERT_EQ(0, main_schema->Options().count(Options::BRANCH));

    // the default branch is matched ignoring case, as in the Java client, so
    // "t1$branch_MAIN" addresses the bare table too
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> main_case_table,
                         catalog->GetTable(Identifier("db1", "t1$branch_MAIN")));
    std::shared_ptr<DataSchema> main_case_schema =
        std::dynamic_pointer_cast<DataSchema>(main_case_table->LatestSchema());
    ASSERT_NE(nullptr, main_case_schema);
    ASSERT_EQ((std::vector<std::string>{"f0", "f1"}), main_case_schema->FieldNames());
    ASSERT_EQ(0, main_case_schema->Options().count(Options::BRANCH));

    // a system table on a branch resolves against the branch's data table: the system
    // suffix is stripped while the branch stays in the identifier sent to the server
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> options_schema,
                         catalog->LoadTableSchema(Identifier("db1", "t1$branch_b1$options")));
    ASSERT_EQ((std::vector<std::string>{"key", "value"}), options_schema->FieldNames());
    // a missing branch fails through the system table path too instead of silently
    // resolving against the main table
    Status missing_branch =
        catalog->LoadTableSchema(Identifier("db1", "t1$branch_missing$options")).status();
    ASSERT_TRUE(missing_branch.IsNotExist()) << missing_branch.ToString();

    ASSERT_NOK_WITH_MSG(catalog->DropTable(branch_identifier, /*ignore_if_not_exists=*/false),
                        "branch table");
    ASSERT_NOK_WITH_MSG(catalog->RenameTable(branch_identifier, Identifier("db1", "t2"),
                                             /*ignore_if_not_exists=*/false),
                        "branch table");
    ASSERT_NOK_WITH_MSG(CreateSampleTable(catalog.get(), Identifier("db1", "t2$branch_b1")),
                        "branch table");
}

TEST_F(RestCatalogTest, CreateRefusesAFormatTableThisClientCouldNotOpen) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));

    // The server takes schemas this library cannot open. Without the same checks the file system
    // catalog runs, creating through this client would succeed and opening the very same table
    // through it would fail.
    auto create = [&catalog](const std::string& name, const std::shared_ptr<arrow::Schema>& schema,
                             const std::vector<std::string>& partition_keys,
                             const std::map<std::string, std::string>& options) {
        struct ArrowSchema c_schema;
        EXPECT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        Status status = catalog->CreateTable(Identifier("db1", name), &c_schema, partition_keys,
                                             /*primary_keys=*/{}, options, false);
        if (c_schema.release != nullptr) {
            c_schema.release(&c_schema);
        }
        return status;
    };
    const std::map<std::string, std::string> format_table = {{Options::TYPE, "format-table"},
                                                             {Options::FILE_FORMAT, "parquet"}};

    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->create_table_requests = 0;
    }

    // Every column a partition column leaves the data files with nothing in them.
    std::shared_ptr<arrow::Schema> only_partitions =
        arrow::schema({arrow::field("dt", arrow::utf8())});
    ASSERT_NOK_WITH_MSG(create("only_partitions", only_partitions, {"dt"}, format_table),
                        "every one of its columns");

    // A partition type this library does not support.
    std::shared_ptr<arrow::Schema> timestamp_partition =
        arrow::schema({arrow::field("f0", arrow::int32()),
                       arrow::field("dt", arrow::timestamp(arrow::TimeUnit::MICRO))});
    ASSERT_NOK_WITH_MSG(create("timestamp_partition", timestamp_partition, {"dt"}, format_table),
                        "cannot be TIMESTAMP/DECIMAL/BLOB");

    // A format table format with no reader here.
    std::shared_ptr<arrow::Schema> good =
        arrow::schema({arrow::field("f0", arrow::int32()), arrow::field("dt", arrow::utf8())});
    std::map<std::string, std::string> csv_table = format_table;
    csv_table[Options::FILE_FORMAT] = "csv";
    ASSERT_NOK_WITH_MSG(create("csv_table", good, {"dt"}, csv_table),
                        "not supported by paimon-cpp yet");

    // None of them was sent: the table must not exist on the server for another client to find.
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ(0, state_->create_table_requests);
    }

    // And a schema this library can open is created and opens.
    ASSERT_OK(create("fine", good, {"dt"}, format_table));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         catalog->GetFormatTable(Identifier("db1", "fine")));
    ASSERT_EQ(FormatTable::Format::PARQUET, table->GetFormat());
}

TEST_F(RestCatalogTest, TableResponseWithoutAPathIsRejected) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));

    MockCatalogState::TableData no_path;
    no_path.schema_json = R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                          R"( "partitionKeys": [], "primaryKeys": [],)"
                          R"( "options": {"type": "format-table", "file.format": "parquet"}})";
    no_path.path = "";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["no_path"] = no_path;
    }

    // Everything built from the response reads and writes below the path. A table whose paths
    // would be checked against an empty one has no boundary at all, so the response is refused
    // rather than turned into a table. What the response said is not repeated back: a body may
    // carry credentials, so a failure to read one names the request and nothing else.
    ASSERT_NOK_WITH_MSG(catalog->GetFormatTable(Identifier("db1", "no_path")),
                        "failed to deserialize the response");
    ASSERT_NOK_WITH_MSG(catalog->GetTableLocation(Identifier("db1", "no_path")),
                        "failed to deserialize the response");
}

TEST_F(RestCatalogTest, FormatTableIsLoadedInOneRequest) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));

    MockCatalogState::TableData table_data;
    table_data.schema_json = R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"},)"
                             R"( {"id": 1, "name": "dt", "type": "STRING"}],)"
                             R"( "partitionKeys": ["dt"], "primaryKeys": [],)"
                             R"( "options": {"type": "format-table", "file.format": "parquet"}})";
    table_data.schema_id = 7;
    table_data.path = "wh1/db1.db/fmt";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["fmt"] = table_data;
        state_->get_table_requests = 0;
    }

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         catalog->GetFormatTable(Identifier("db1", "fmt")));
    ASSERT_EQ("wh1/db1.db/fmt", table->Location());
    ASSERT_EQ(FormatTable::Format::PARQUET, table->GetFormat());
    ASSERT_EQ((std::vector<std::string>{"dt"}), table->PartitionKeys());
    // The location and the schema come from one response, so they cannot describe two different
    // states of the table.
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ(1, state_->get_table_requests);
    }

    // This catalog holds the schema itself, so everything below the location is data - a
    // directory named "schema" there is a partition value, not metadata to skip.
    ASSERT_FALSE(table->LocationCarriesPaimonMetadata());

    ASSERT_NOK_WITH_MSG(catalog->GetTable(Identifier("db1", "fmt")), "Cannot open format table");

    // A managed table stays out of this path, and so does a system table.
    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t1")));
    ASSERT_NOK_WITH_MSG(catalog->GetFormatTable(Identifier("db1", "t1")), "is not a format table");
    ASSERT_NOK_WITH_MSG(catalog->GetFormatTable(Identifier("db1", "t1$snapshots")),
                        "for system table");
    // The database is checked before the table name is parsed, so a malformed name under "sys"
    // is refused as a system table rather than surfacing a parse error.
    ASSERT_NOK_WITH_MSG(catalog->GetFormatTable(Identifier("sys", "all_table_options")),
                        "for system table");
    ASSERT_NOK_WITH_MSG(catalog->GetFormatTable(Identifier("sys", "a$b$c$d")), "for system table");
}

TEST_F(RestCatalogTest, FormatTableWithAnUnusableSchemaIsRejectedOnLoad) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));

    // A schema from a rest catalog never passed through table creation in this library, so the
    // checks that run there have to run again on load. Without that these would be accepted here
    // and fail only when the first reader or writer was built.
    const std::string default_fields = R"([{"id": 0, "name": "f0", "type": "INT NOT NULL"},)"
                                       R"( {"id": 1, "name": "dt", "type": "STRING"}])";
    auto seed = [this](const std::string& name, const std::string& options,
                       const std::string& partition_keys, const std::string& fields) {
        MockCatalogState::TableData table_data;
        table_data.schema_json = R"({"fields": )" + fields + R"(, "partitionKeys": )" +
                                 partition_keys + R"(, "primaryKeys": [], "options": )" + options +
                                 "}";
        table_data.path = "wh1/db1.db/" + name;
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"][name] = table_data;
    };

    // A format table format with no reader here.
    seed("csv_table", R"({"type": "format-table", "file.format": "csv"})", R"(["dt"])",
         default_fields);
    ASSERT_NOK_WITH_MSG(catalog->GetFormatTable(Identifier("db1", "csv_table")),
                        "not supported by paimon-cpp yet");

    // A row count no file could ever reach.
    seed("bad_rows",
         R"({"type": "format-table", "file.format": "parquet", "target-file-row-num": "0"})",
         R"(["dt"])", default_fields);
    ASSERT_NOK_WITH_MSG(catalog->GetFormatTable(Identifier("db1", "bad_rows")),
                        "should be at least 1");

    // The failure names the table, which is all a caller holding several has to go on.
    ASSERT_NOK_WITH_MSG(catalog->GetFormatTable(Identifier("db1", "bad_rows")), "db1.bad_rows");

    // The structural rules run here too, not only the format-table ones: a partition key that is
    // not a field of the schema is a table nothing could ever read.
    seed("bad_partition_key", R"({"type": "format-table", "file.format": "parquet"})",
         R"(["nosuchfield"])", default_fields);
    ASSERT_NOK_WITH_MSG(catalog->GetFormatTable(Identifier("db1", "bad_partition_key")),
                        "should include all partition fields");

    // Two schemas this library cannot open, refused when the table is opened rather than when a
    // reader is first built. Both are in the user guide's list of limits.
    seed("timestamp_partition", R"({"type": "format-table", "file.format": "parquet"})",
         R"(["dt"])",
         R"([{"id": 0, "name": "f0", "type": "INT NOT NULL"},)"
         // Not a raw string: `TIMESTAMP(6)"` holds the sequence that would end one.
         " {\"id\": 1, \"name\": \"dt\", \"type\": \"TIMESTAMP(6)\"}]");
    ASSERT_NOK_WITH_MSG(catalog->GetFormatTable(Identifier("db1", "timestamp_partition")),
                        "cannot be TIMESTAMP/DECIMAL/BLOB");

    seed("only_partitions", R"({"type": "format-table", "file.format": "parquet"})", R"(["dt"])",
         R"([{"id": 0, "name": "dt", "type": "STRING"}])");
    ASSERT_NOK_WITH_MSG(catalog->GetFormatTable(Identifier("db1", "only_partitions")),
                        "every one of its columns");

    // An option this library does not honour keeps its own status code, so a caller can still
    // tell "not supported yet" from "bad table".
    seed("catalog_partitions",
         R"({"type": "format-table", "file.format": "parquet",)"
         R"( "metastore.partitioned-table": "true"})",
         R"(["dt"])", default_fields);
    Status not_implemented =
        catalog->GetFormatTable(Identifier("db1", "catalog_partitions")).status();
    ASSERT_TRUE(not_implemented.IsNotImplemented()) << not_implemented.ToString();
}

TEST_F(RestCatalogTest, NestedSchemaHighestFieldId) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    MockCatalogState::TableData table_data;
    table_data.schema_json = R"({
        "fields": [
            {"id": 0, "name": "f0", "type": "INT NOT NULL"},
            {"id": 1, "name": "s", "type": {"type": "ROW",
                "fields": [{"id": 3, "name": "inner", "type": "INT"}]}},
            {"id": 2, "name": "arr", "type": {"type": "ARRAY",
                "element": {"type": "ROW",
                            "fields": [{"id": 7, "name": "deep", "type": "BIGINT"}]}}},
            {"id": 4, "name": "m", "type": {"type": "MAP",
                "key": {"type": "ROW NOT NULL",
                        "fields": [{"id": 8, "name": "k", "type": "INT NOT NULL"}]},
                "value": {"type": "ROW",
                          "fields": [{"id": 9, "name": "v", "type": "INT"}]}}}
        ],
        "partitionKeys": [],
        "primaryKeys": [],
        "options": {}
    })";
    table_data.schema_id = 5;
    table_data.path = "wh1/db1.db/nested";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["nested"] = table_data;
    }

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         catalog->GetTable(Identifier("db1", "nested")));
    std::shared_ptr<DataSchema> schema =
        std::dynamic_pointer_cast<DataSchema>(table->LatestSchema());
    ASSERT_NE(nullptr, schema);
    ASSERT_EQ(5, schema->Id());
    // 9 lives inside the value row of the map: ROW, ARRAY element and MAP key/value
    // must all be traversed
    ASSERT_EQ(9, schema->HighestFieldId());
    ASSERT_EQ((std::vector<std::string>{"f0", "s", "arr", "m"}), schema->FieldNames());
}

TEST_F(RestCatalogTest, ListTablesPaged) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t1")));
    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t2")));
    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t3")));
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> tables, catalog->ListTables("db1"));
    ASSERT_EQ((std::vector<std::string>{"t1", "t2", "t3"}), tables);
}

TEST_F(RestCatalogTest, BrokenSchemaRejected) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    ExpectBrokenSchemaRejected(catalog.get(), "duplicate",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"},)"
                               R"( {"id": 0, "name": "f1", "type": "STRING"}],)"
                               R"( "partitionKeys": [], "primaryKeys": [], "options": {}})",
                               "duplicated");
    // an id inside a nested row colliding with an outer field id is a duplicate too
    ExpectBrokenSchemaRejected(catalog.get(), "duplicate_nested",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"},)"
                               R"( {"id": 1, "name": "s", "type": {"type": "ROW",)"
                               R"( "fields": [{"id": 0, "name": "inner", "type": "INT"}]}}],)"
                               R"( "partitionKeys": [], "primaryKeys": [], "options": {}})",
                               "duplicated");
    // a field without an integer id must fail the conversion instead of being
    // silently skipped when computing highestFieldId
    ExpectBrokenSchemaRejected(catalog.get(), "no_id",
                               R"({"fields": [{"name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": [], "primaryKeys": [], "options": {}})",
                               "integer id");
    ExpectBrokenSchemaRejected(catalog.get(), "string_id",
                               R"({"fields": [{"id": "0", "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": [], "primaryKeys": [], "options": {}})",
                               "integer id");
    // a field that is not an object fails too instead of being silently skipped
    ExpectBrokenSchemaRejected(catalog.get(), "non_object",
                               R"({"fields": [1],)"
                               R"( "partitionKeys": [], "primaryKeys": [], "options": {}})",
                               "must be an object");
    // a missing or wrong-typed member is a visible failure instead of silently
    // defaulting to an empty value (e.g. loading a partitioned table as
    // unpartitioned)
    ExpectBrokenSchemaRejected(catalog.get(), "missing_partition_keys",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "primaryKeys": [], "options": {}})",
                               "missing 'partitionKeys'");
    ExpectBrokenSchemaRejected(catalog.get(), "missing_primary_keys",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": [], "options": {}})",
                               "missing 'primaryKeys'");
    ExpectBrokenSchemaRejected(catalog.get(), "missing_options",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": [], "primaryKeys": []})",
                               "missing 'options'");
    ExpectBrokenSchemaRejected(catalog.get(), "wrong_typed_partition_keys",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": {}, "primaryKeys": [], "options": {}})",
                               "'partitionKeys' is not an array");
    ExpectBrokenSchemaRejected(catalog.get(), "wrong_typed_primary_keys",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": [], "primaryKeys": "f0", "options": {}})",
                               "'primaryKeys' is not an array");
    ExpectBrokenSchemaRejected(catalog.get(), "wrong_typed_options",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": [], "primaryKeys": [], "options": []})",
                               "'options' is not an object");
}

TEST_F(RestCatalogTest, PartitionKeysRoundTrip) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("f0", arrow::int32(), /*nullable=*/false),
                       arrow::field("f1", arrow::utf8(), /*nullable=*/false)});
    struct ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    // The server's config sets a `table-default.bucket`, which would make this a bucketed append
    // table - and one of those needs a bucket key it has not been given.
    ASSERT_OK(catalog->CreateTable(Identifier("db1", "pt"), &c_schema,
                                   /*partition_keys=*/{"f1"}, /*primary_keys=*/{},
                                   {{"bucket", "-1"}},
                                   /*ignore_if_exists=*/false));
    // the partition keys survive both the create request and the load response conversion
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog->GetTable(Identifier("db1", "pt")));
    std::shared_ptr<DataSchema> loaded =
        std::dynamic_pointer_cast<DataSchema>(table->LatestSchema());
    ASSERT_NE(nullptr, loaded);
    ASSERT_EQ((std::vector<std::string>{"f1"}), loaded->PartitionKeys());
}

TEST_F(RestCatalogTest, ServerErrorIsPropagatedNotMappedToAbsent) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->force_error_code = 500;
    }
    // a server failure surfaces as an error instead of "does not exist"
    Status db_status = catalog->DatabaseExists("db1").status();
    ASSERT_NOK_WITH_MSG(db_status, "server error");
    Status table_status = catalog->TableExists(Identifier("db1", "t1")).status();
    ASSERT_NOK_WITH_MSG(table_status, "server error");
}

TEST_F(RestCatalogTest, SystemTableChecks) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_NOK_WITH_MSG(catalog->CreateDatabase("sys", {}, false), "system database");
    ASSERT_NOK_WITH_MSG(catalog->DropDatabase("sys", false, false), "system database");
    ASSERT_NOK_WITH_MSG(catalog->DropTable(Identifier("sys", "t"), false), "system table");
    ASSERT_NOK_WITH_MSG(
        catalog->RenameTable(Identifier("db1", "t1$snapshots"), Identifier("db1", "t2"), false),
        "system table");
}

TEST_F(RestCatalogTest, SystemDatabase) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    // the "sys" database and its global system tables are resolved locally without
    // contacting the server, like in FileSystemCatalog
    ASSERT_OK_AND_ASSIGN(bool exists, catalog->DatabaseExists("sys"));
    ASSERT_TRUE(exists);

    ASSERT_OK_AND_ASSIGN(std::vector<std::string> sys_tables, catalog->ListTables("sys"));
    ASSERT_TRUE(std::find(sys_tables.begin(), sys_tables.end(), "tables") != sys_tables.end());

    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("sys", "tables")));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("sys", "unsupported")));
    ASSERT_FALSE(exists);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> schema,
                         catalog->LoadTableSchema(Identifier("sys", "tables")));
    ASSERT_FALSE(schema->FieldNames().empty());
    Status missing = catalog->LoadTableSchema(Identifier("sys", "unsupported")).status();
    ASSERT_TRUE(missing.IsNotExist()) << missing.ToString();
}

TEST_F(RestCatalogTest, ListSnapshots) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    Identifier identifier("db1", "t1");
    ASSERT_OK(CreateSampleTable(catalog.get(), identifier));

    ASSERT_OK_AND_ASSIGN(std::vector<SnapshotInfo> snapshots,
                         catalog->ListSnapshots(identifier, ""));
    ASSERT_EQ(2, snapshots.size());
    // fetched via two pages and sorted by snapshot id
    ASSERT_EQ(1, snapshots[0].snapshot_id);
    ASSERT_EQ(2, snapshots[1].snapshot_id);
    ASSERT_EQ("user1", snapshots[0].commit_user);
    ASSERT_EQ(SnapshotInfo::CommitKind::APPEND, snapshots[0].commit_kind);

    // the default branch is addressed as the bare table, so passing it explicitly
    // equals the branch-less call
    ASSERT_OK_AND_ASSIGN(std::vector<SnapshotInfo> main_snapshots,
                         catalog->ListSnapshots(identifier, "main"));
    ASSERT_EQ(2, main_snapshots.size());
    // the match ignores case, as in the Java client, so "MAIN" is the default branch too
    ASSERT_OK_AND_ASSIGN(std::vector<SnapshotInfo> main_case_snapshots,
                         catalog->ListSnapshots(identifier, "MAIN"));
    ASSERT_EQ(2, main_case_snapshots.size());
    // a blank name is the main branch everywhere in this library, so it is the bare table here
    ASSERT_OK_AND_ASSIGN(std::vector<SnapshotInfo> blank_snapshots,
                         catalog->ListSnapshots(identifier, "   "));
    ASSERT_EQ(2, blank_snapshots.size());

    Status missing = catalog->ListSnapshots(Identifier("db1", "t9"), "").status();
    ASSERT_TRUE(missing.IsNotExist()) << missing.ToString();

    // a non-main branch is sent under its branch object name, so the server resolves
    // the branch and lists its own snapshots
    MockCatalogState::TableData branch_data;
    branch_data.schema_json = R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                              R"( "partitionKeys": [], "primaryKeys": [], "options": {}})";
    branch_data.path = "wh1/db1.db/t1";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["t1$branch_b1"] = branch_data;
    }
    ASSERT_OK_AND_ASSIGN(std::vector<SnapshotInfo> branch_snapshots,
                         catalog->ListSnapshots(identifier, "b1"));
    ASSERT_EQ(2, branch_snapshots.size());
    // a branch the server does not know is missing instead of silently falling back
    // to the main table
    Status missing_branch = catalog->ListSnapshots(identifier, "b_missing").status();
    ASSERT_TRUE(missing_branch.IsNotExist()) << missing_branch.ToString();

    // a branch must be passed as the branch argument, not encoded in the identifier
    ASSERT_NOK_WITH_MSG(catalog->ListSnapshots(Identifier("db1", "t1$branch_b1"), "").status(),
                        "branch table");
}

TEST_F(RestCatalogTest, FileStoreCommitIsBuiltFromTheCatalog) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    MockCatalogState::TableData table_data;
    table_data.schema_json =
        R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
        R"( "partitionKeys": [], "primaryKeys": [],)"
        R"( "options": {"file.format": "mock_format", "manifest.format": "avro"}})";
    table_data.path = dir->Str();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> rest_catalog, CreateRestCatalog());
    ASSERT_OK(rest_catalog->CreateDatabase("db1", {}, false));
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["t1"] = table_data;
    }
    ASSERT_TRUE(rest_catalog->SupportsVersionManagement());

    int32_t requests_before = 0;
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        requests_before = state_->get_table_requests;
    }
    std::shared_ptr<Catalog> shared_catalog(std::move(rest_catalog));
    CommitContextBuilder builder(dir->Str(), "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> context,
                         builder.WithCatalog(shared_catalog, Identifier("db1", "t1")).Finish());
    ASSERT_OK(FileStoreCommit::Create(std::move(context)));
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_GT(state_->get_table_requests, requests_before);
    }
}

TEST_F(RestCatalogTest, CommitSnapshot) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, false));
    Identifier identifier("db1", "t1");
    ASSERT_OK(CreateSampleTable(catalog.get(), identifier));

    ASSERT_TRUE(catalog->SupportsVersionManagement());

    Snapshot snapshot = BuildTestSnapshot(2);
    std::vector<PartitionStatistics> statistics = {
        PartitionStatistics({{"dt", "20240101"}}, 1, 541, 1, 1724090888743, -1)};

    ASSERT_OK_AND_ASSIGN(bool success,
                         catalog->CommitSnapshot(identifier, "table-uuid", "base-snapshot-uuid",
                                                 snapshot, statistics));
    ASSERT_TRUE(success);

    std::string commit_body;
    std::string commit_table;
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        commit_body = state_->last_commit_body;
        commit_table = state_->last_commit_table;
    }
    ASSERT_EQ("t1", commit_table);
    ASSERT_OK_AND_ASSIGN(CommitTableRequest request,
                         CommitTableRequest::FromJsonString(commit_body));
    ASSERT_EQ(request.GetTableId(), std::optional<std::string>("table-uuid"));
    ASSERT_EQ(request.GetBaseSnapshotUuid(), std::optional<std::string>("base-snapshot-uuid"));
    ASSERT_EQ(request.GetSnapshot().Id(), 2);
    ASSERT_EQ(request.GetStatistics(), statistics);

    MockCatalogState::TableData branch_data;
    branch_data.schema_json = R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                              R"( "partitionKeys": [], "primaryKeys": [], "options": {}})";
    branch_data.path = "wh1/db1.db/t1";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["t1$branch_b1"] = branch_data;
        state_->last_commit_table.clear();
        state_->last_commit_body.clear();
    }
    ASSERT_OK_AND_ASSIGN(bool branch_success,
                         catalog->CommitSnapshot(Identifier("db1", "t1$branch_b1"), std::nullopt,
                                                 std::nullopt, snapshot, statistics));
    ASSERT_TRUE(branch_success);
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ("t1$branch_b1", state_->last_commit_table);
    }

    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->last_commit_table.clear();
    }
    ASSERT_OK_AND_ASSIGN(bool main_success,
                         catalog->CommitSnapshot(Identifier("db1", "t1$branch_main"), std::nullopt,
                                                 std::nullopt, snapshot, statistics));
    ASSERT_TRUE(main_success);
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ("t1", state_->last_commit_table);
    }
}

TEST_F(RestCatalogTest, LoadSnapshot) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, false));
    Identifier identifier("db1", "t1");
    ASSERT_OK(CreateSampleTable(catalog.get(), identifier));

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> empty, catalog->LoadSnapshot(identifier));
    ASSERT_FALSE(empty.has_value());

    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->null_snapshot_response = true;
    }
    ASSERT_OK_AND_ASSIGN(empty, catalog->LoadSnapshot(identifier));
    ASSERT_FALSE(empty.has_value());

    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->null_snapshot_response = false;
        state_->current_snapshot = SnapshotJson(7);
    }
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> loaded, catalog->LoadSnapshot(identifier));
    ASSERT_TRUE(loaded.has_value());
    ASSERT_EQ(loaded.value().Id(), 7);
    ASSERT_EQ(loaded.value().BaseManifestList(), "bml");
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ("t1", state_->last_snapshot_table);
    }

    MockCatalogState::TableData branch_data;
    branch_data.schema_json = R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                              R"( "partitionKeys": [], "primaryKeys": [], "options": {}})";
    branch_data.path = "wh1/db1.db/t1";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["t1$branch_b1"] = branch_data;
    }
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> branch_snapshot,
                         catalog->LoadSnapshot(Identifier("db1", "t1$branch_b1")));
    ASSERT_TRUE(branch_snapshot.has_value());
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ("t1$branch_b1", state_->last_snapshot_table);
    }
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> main_snapshot,
                         catalog->LoadSnapshot(Identifier("db1", "t1$branch_main")));
    ASSERT_TRUE(main_snapshot.has_value());
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ("t1", state_->last_snapshot_table);
    }

    Status missing = catalog->LoadSnapshot(Identifier("db1", "t9")).status();
    ASSERT_TRUE(missing.IsNotExist()) << missing.ToString();

    ASSERT_NOK_WITH_MSG(catalog->LoadSnapshot(Identifier("db1", "t1$snapshots")),
                        "Cannot 'loadSnapshot' for system table");
}

TEST_F(RestCatalogTest, CommitSnapshotErrors) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, false));
    Identifier identifier("db1", "t1");
    ASSERT_OK(CreateSampleTable(catalog.get(), identifier));

    Snapshot snapshot = BuildTestSnapshot(2);
    std::vector<PartitionStatistics> statistics;

    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->refuse_commit = true;
    }
    ASSERT_OK_AND_ASSIGN(bool refused,
                         catalog->CommitSnapshot(identifier, "table-uuid", "base-snapshot-uuid",
                                                 snapshot, statistics));
    ASSERT_FALSE(refused);
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->refuse_commit = false;
    }

    Status missing = catalog
                         ->CommitSnapshot(Identifier("db1", "t9"), std::nullopt, std::nullopt,
                                          snapshot, statistics)
                         .status();
    ASSERT_TRUE(missing.IsNotExist()) << missing.ToString();

    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->break_commit_response = true;
    }
    ASSERT_NOK_WITH_MSG(
        catalog->CommitSnapshot(identifier, "table-uuid", std::nullopt, snapshot, statistics),
        "failed to deserialize the response");
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->break_commit_response = false;
    }

    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->commit_response_without_outcome = true;
    }
    ASSERT_NOK_WITH_MSG(
        catalog->CommitSnapshot(identifier, "table-uuid", std::nullopt, snapshot, statistics),
        "failed to deserialize the response");
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->commit_response_without_outcome = false;
    }

    ASSERT_NOK_WITH_MSG(catalog->CommitSnapshot(Identifier("db1", "t1$snapshots"), std::nullopt,
                                                std::nullopt, snapshot, statistics),
                        "Cannot 'commitSnapshot' for system table");
}

namespace {
Result<std::vector<std::shared_ptr<CommitMessage>>> ReadFixtureCommitMessages(
    const std::shared_ptr<FileSystem>& fs) {
    std::string bytes;
    PAIMON_RETURN_NOT_OK(
        fs->ReadFile(paimon::test::GetDataDir() +
                         "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                     &bytes));
    return CommitMessage::DeserializeList(3, bytes.data(), bytes.size(), GetDefaultPool());
}
}  // namespace

class RestCatalogCommitRecoveryTest : public RestCatalogTest,
                                      public ::testing::WithParamInterface<int32_t> {};

TEST_P(RestCatalogCommitRecoveryTest, CommitAcceptedThenLostRecoversAfterRestart) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    MockCatalogState::TableData table_data;
    table_data.schema_json = R"({"fields": [{"id": 0, "name": "f0", "type": "STRING"},)"
                             R"( {"id": 1, "name": "f1", "type": "INT"},)"
                             R"( {"id": 2, "name": "f2", "type": "INT"},)"
                             R"( {"id": 3, "name": "f3", "type": "DOUBLE"}],)"
                             R"( "partitionKeys": ["f1"], "primaryKeys": [],)"
                             R"( "options": {"file.format": "orc", "manifest.format": "avro"}})";
    table_data.path = dir->Str();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> rest_catalog,
                         CreateRestCatalog(FastRetryConfig()));
    ASSERT_OK(rest_catalog->CreateDatabase("db1", {}, false));
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["t1"] = table_data;
        state_->accept_commit_then_report_unavailable = true;
        state_->accepted_commit_response_code = GetParam();
        state_->commit_requests = 0;
    }
    Identifier identifier("db1", "t1");
    RestCatalog* raw_catalog = rest_catalog.get();
    std::shared_ptr<Catalog> catalog(std::move(rest_catalog));

    auto create_commit = [&]() -> Result<std::unique_ptr<FileStoreCommit>> {
        CommitContextBuilder builder(dir->Str(), "commit-user");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                               builder.WithCatalog(catalog, identifier).Finish());
        return FileStoreCommit::Create(std::move(context));
    };
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit, create_commit());

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> msgs,
                         ReadFixtureCommitMessages(dir->GetFileSystem()));
    ASSERT_GT(msgs.size(), 0u);
    ASSERT_NOK_WITH_MSG(commit->Commit(msgs, 7), "call FilterAndCommit");
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ(1, state_->commit_requests);
    }

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> held, raw_catalog->LoadSnapshot(identifier));
    ASSERT_TRUE(held.has_value());
    ASSERT_EQ(held.value().Id(), 1);
    ASSERT_EQ(held.value().CommitIdentifier(), 7);
    ASSERT_EQ(held.value().CommitUser(), "commit-user");
    for (const std::string& manifest_list :
         {held.value().BaseManifestList(), held.value().DeltaManifestList()}) {
        ASSERT_OK_AND_ASSIGN(bool exist, dir->GetFileSystem()->Exists(PathUtil::JoinPath(
                                             dir->Str(), "manifest/" + manifest_list)));
        ASSERT_TRUE(exist) << manifest_list;
    }

    std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>> inputs;
    inputs[7] = msgs;
    commit.reset();
    ASSERT_OK_AND_ASSIGN(commit, create_commit());
    ASSERT_OK_AND_ASSIGN(int32_t again, commit->FilterAndCommit(inputs, 10));
    ASSERT_EQ(0, again);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> recovered, raw_catalog->LoadSnapshot(identifier));
    ASSERT_EQ(recovered, held);
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ(1, state_->commit_requests);
        state_->accept_commit_then_report_unavailable = false;
    }
}

INSTANTIATE_TEST_SUITE_P(UncertainResponses, RestCatalogCommitRecoveryTest,
                         ::testing::Values(200, 204, 307, 429, 503),
                         [](const ::testing::TestParamInfo<int32_t>& info) {
                             return fmt::format("Http{}", info.param);
                         });

TEST_F(RestCatalogTest, CommitTakenThenReportedUnavailableIsNotReplayed) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog,
                         CreateRestCatalog(FastRetryConfig()));
    ASSERT_OK(catalog->CreateDatabase("db1", {}, false));
    Identifier identifier("db1", "t1");
    ASSERT_OK(CreateSampleTable(catalog.get(), identifier));

    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->accept_commit_then_report_unavailable = true;
        state_->commit_requests = 0;
    }
    const Snapshot taken = BuildTestSnapshot(2, "snapshot-uuid-2");
    Status unavailable =
        catalog->CommitSnapshot(identifier, "table-uuid", "base-snapshot-uuid", taken, {}).status();
    ASSERT_FALSE(unavailable.ok()) << "a 503 must not read as a refusal";
    ASSERT_TRUE(unavailable.IsIOError()) << unavailable.ToString();
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ(1, state_->commit_requests);
    }

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> held, catalog->LoadSnapshot(identifier));
    ASSERT_TRUE(held.has_value());
    ASSERT_EQ(held.value().Id(), 2);
    ASSERT_EQ(held.value().Uuid(), std::optional<std::string>("snapshot-uuid-2"));

    ASSERT_OK_AND_ASSIGN(bool replayed, catalog->CommitSnapshot(identifier, "table-uuid",
                                                                "base-snapshot-uuid", taken, {}));
    ASSERT_FALSE(replayed);

    ASSERT_OK_AND_ASSIGN(bool rebased,
                         catalog->CommitSnapshot(identifier, "table-uuid", "snapshot-uuid-2",
                                                 BuildTestSnapshot(3, "snapshot-uuid-3"), {}));
    ASSERT_TRUE(rebased);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> now_held, catalog->LoadSnapshot(identifier));
    ASSERT_TRUE(now_held.has_value());
    ASSERT_EQ(now_held.value().Id(), 3);
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ(3, state_->commit_requests);
        state_->accept_commit_then_report_unavailable = false;
    }
}

TEST(RestApiErrorTest, ErrorToStatus) {
    RestHttpClient::Response response;
    response.code = 404;
    response.body = R"({"message": "no table", "resourceType": "TABLE", "resourceName": "t1"})";
    response.headers["x-request-id"] = "req-123";
    Status status = RestApi::ErrorToStatus(response);
    ASSERT_TRUE(status.IsNotExist());
    ASSERT_TRUE(status.ToString().find("requestId:req-123") != std::string::npos)
        << status.ToString();
    ASSERT_NE(status.detail(), nullptr);
    ASSERT_STREQ(status.detail()->type_id(), RestErrorDetail::kTypeId);
    ASSERT_EQ(static_cast<const RestErrorDetail*>(status.detail().get())->GetResourceType(),
              ErrorResponse::kResourceTypeTable);

    response.body = R"({"message": "no snapshot", "resourceType": "SNAPSHOT",)"
                    R"( "resourceName": "t1"})";
    Status snapshot_status = RestApi::ErrorToStatus(response);
    ASSERT_TRUE(snapshot_status.IsNotExist());
    ASSERT_EQ(
        static_cast<const RestErrorDetail*>(snapshot_status.detail().get())->GetResourceType(),
        ErrorResponse::kResourceTypeSnapshot);

    response.body = R"({"message": "boom"})";
    Status plain = RestApi::ErrorToStatus(response);
    ASSERT_TRUE(
        static_cast<const RestErrorDetail*>(plain.detail().get())->GetResourceType().empty());
    response.body = R"({"message": "no table", "resourceType": "TABLE", "resourceName": "t1"})";

    response.code = 409;
    Status exist_status = RestApi::ErrorToStatus(response);
    ASSERT_TRUE(exist_status.IsExist());

    response.code = 501;
    response.body = "";
    ASSERT_TRUE(RestApi::ErrorToStatus(response).IsNotImplemented());

    // a body that is not an error object at all is reported as such, since the body
    // itself is never echoed
    response.code = 500;
    response.body = "not-a-json";
    Status unparsable = RestApi::ErrorToStatus(response);
    ASSERT_NOK_WITH_MSG(unparsable, "server error");
    ASSERT_NOK_WITH_MSG(unparsable, "unparsable error response body (http status 500)");
    ASSERT_EQ(std::string::npos, unparsable.ToString().find("not-a-json")) << unparsable.ToString();

    // the code of the error body wins over the http status when they disagree (e.g. a
    // gateway rewriting the status)
    response.code = 500;
    response.body = R"({"message": "gone", "code": 404})";
    ASSERT_TRUE(RestApi::ErrorToStatus(response).IsNotExist());

    // an error object without a message is told apart from an unparsable body, and the
    // resource info is kept
    response.code = 404;
    response.body = R"({"resourceType": "TABLE", "resourceName": "t1"})";
    Status empty_message = RestApi::ErrorToStatus(response);
    ASSERT_NOK_WITH_MSG(empty_message, "empty error message (http status 404)");
    ASSERT_TRUE(empty_message.ToString().find("resource name: t1") != std::string::npos)
        << empty_message.ToString();

    // server messages that may embed secrets are redacted as a whole
    response.code = 400;
    response.body = R"({"message": "bad option password=abc123", "code": 400})";
    Status redacted = RestApi::ErrorToStatus(response);
    ASSERT_TRUE(redacted.IsInvalid()) << redacted.ToString();
    ASSERT_TRUE(redacted.ToString().find("abc123") == std::string::npos) << redacted.ToString();
    ASSERT_TRUE(redacted.ToString().find("******") != std::string::npos) << redacted.ToString();

    // any header carrying a request id is used when x-request-id is absent
    response.code = 404;
    response.body = "";
    response.headers.clear();
    response.headers["x-amz-request-id"] = "amz-1";
    Status fallback = RestApi::ErrorToStatus(response);
    ASSERT_TRUE(fallback.ToString().find("requestId:amz-1") != std::string::npos)
        << fallback.ToString();

    // the "unknown" placeholder is not a real request id
    response.headers.clear();
    response.headers["x-request-id"] = "unknown";
    Status unknown_id = RestApi::ErrorToStatus(response);
    ASSERT_TRUE(unknown_id.ToString().find("requestId") == std::string::npos)
        << unknown_id.ToString();

    // 401/403 map to IOError; the mapped code is carried as a status detail so
    // callers can distinguish them
    response.headers.clear();
    response.code = 401;
    Status not_authorized = RestApi::ErrorToStatus(response);
    ASSERT_NOK_WITH_MSG(not_authorized, "not authorized");
    ASSERT_NE(nullptr, not_authorized.detail());
    ASSERT_EQ(std::string(RestErrorDetail::kTypeId), not_authorized.detail()->type_id());
    ASSERT_EQ(401, checked_pointer_cast<RestErrorDetail>(not_authorized.detail())->GetCode());
    response.code = 403;
    Status forbidden = RestApi::ErrorToStatus(response);
    ASSERT_NOK_WITH_MSG(forbidden, "forbidden");
    ASSERT_EQ(403, checked_pointer_cast<RestErrorDetail>(forbidden.detail())->GetCode());

    // 503 and the codes without an own mapping (e.g. 429) become IOError with a
    // message naming the code
    response.code = 503;
    ASSERT_NOK_WITH_MSG(RestApi::ErrorToStatus(response), "service unavailable");
    response.code = 429;
    ASSERT_NOK_WITH_MSG(RestApi::ErrorToStatus(response), "rest request failed with code 429");
    response.code = 418;
    ASSERT_NOK_WITH_MSG(RestApi::ErrorToStatus(response), "rest request failed with code 418");

    response.code = 302;
    Status signed_redirect = RestApi::ErrorToStatus(response, /*follow_redirects=*/false);
    ASSERT_NOK_WITH_MSG(signed_redirect, "redirect status 302");
    ASSERT_NOK_WITH_MSG(signed_redirect, "not followed for signed requests");
    ASSERT_EQ(302, checked_pointer_cast<RestErrorDetail>(signed_redirect.detail())->GetCode());
}

TEST(RestApiErrorTest, MalformedSuccessBodyFails) {
    // a 200 response whose body is not the expected json must fail, not crash or
    // return partial data
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([](const MockRestServer::Request& request) {
                             MockRestServer::Response response;
                             response.body = "not-a-json";
                             return response;
                         }));
    std::map<std::string, std::string> options = {
        {CatalogOptions::URI, server->GetBaseUri()},
        {CatalogOptions::TOKEN_PROVIDER, "bear"},
        {CatalogOptions::TOKEN, kToken},
    };
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestApi> api,
                         RestApi::Create(options, "", /*config_required=*/false));
    Status list_status = api->ListDatabases().status();
    ASSERT_NOK(list_status);
    // the body is not echoed into the error: a successful response may contain
    // credentials
    ASSERT_EQ(std::string::npos, list_status.ToString().find("not-a-json"))
        << list_status.ToString();
    ASSERT_NOK(api->GetTable(Identifier("db1", "t1")).status());

    Status config_status = RestApi::Create(options, "", /*config_required=*/true).status();
    ASSERT_NOK(config_status);
    ASSERT_EQ(std::string::npos, config_status.ToString().find("not-a-json"))
        << config_status.ToString();
}

TEST(RestApiErrorTest, PagedListingStopsOnEmptyPageWithToken) {
    // a server that keeps returning a page token with no data must not loop forever
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             request_count++;
                             MockRestServer::Response response;
                             response.body = R"({"databases":[],"nextPageToken":"more"})";
                             return response;
                         }));
    std::map<std::string, std::string> options = {
        {CatalogOptions::URI, server->GetBaseUri()},
        {CatalogOptions::TOKEN_PROVIDER, "bear"},
        {CatalogOptions::TOKEN, kToken},
    };
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestApi> api,
                         RestApi::Create(options, "", /*config_required=*/false));
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> databases, api->ListDatabases());
    ASSERT_TRUE(databases.empty());
    ASSERT_EQ(1, request_count.load());
}

}  // namespace paimon::test
