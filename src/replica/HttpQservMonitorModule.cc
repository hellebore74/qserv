/*
 * LSST Data Management System
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "replica/HttpQservMonitorModule.h"

// System headers
#include <sstream>
#include <stdexcept>

// Third party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "css/CssAccess.h"
#include "css/CssError.h"
#include "replica/Configuration.h"
#include "replica/ConfigDatabase.h"
#include "replica/DatabaseServices.h"
#include "replica/QservMgtServices.h"
#include "replica/QservStatusJob.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "partition/Chunker.h"
#include "partition/Geometry.h"
#include "lsst/sphgeom/Chunker.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv;
using namespace lsst::qserv::replica;
using namespace lsst::qserv::replica::database::mysql;

namespace {

/**
 * Extract a value of field from a result set and store it
 *
 * @param context the context for error reporting
 * @param row     the current row from the result set
 * @param column  the name of a column
 * @param obj     JSON object where to store the values
 *
 * @throws invalid_argument  if the column is not present in a result set
 * or the value of the field is 'NULL'.
 */
template <typename T>
void parseFieldIntoJson(string const& context, Row const& row, string const& column, json& obj) {
    T val;
    if (!row.get(column, val)) {
        throw invalid_argument(context + " no column '" + column + "' found in the result set");
    }
    obj[column] = val;
}

/**
 * The complementary version of the above defined function which replaces
 * 'NULL' found in a field with the specified default value.
 */
template <typename T>
void parseFieldIntoJson(string const& context, Row const& row, string const& column, json& obj,
                        T const& defaultValue) {
    if (row.isNull(column)) {
        obj[column] = defaultValue;
        return;
    }
    parseFieldIntoJson<T>(context, row, column, obj);
}

/**
 * Extract rows selected from table qservMeta.QInfo into a JSON object.
 */
void extractQInfo(Connection::Ptr const& conn, json& result) {
    if (!conn->hasResult()) return;

    Row row;
    while (conn->next(row)) {
        QueryId queryId;
        if (!row.get("queryId", queryId)) continue;

        string query, status, submitted, completed;
        row.get("query", query);
        row.get("status", status);
        row.get("submitted", submitted);
        row.get("completed", completed);

        string const queryIdStr = to_string(queryId);
        result[queryIdStr]["query"] = query;
        result[queryIdStr]["status"] = status;
        result[queryIdStr]["submitted"] = submitted;
        result[queryIdStr]["completed"] = completed;
    }
}
}  // namespace

namespace lsst::qserv::replica {

void HttpQservMonitorModule::process(Controller::Ptr const& controller, string const& taskName,
                                     HttpProcessorConfig const& processorConfig,
                                     qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                                     string const& subModuleName, HttpAuthType const authType) {
    HttpQservMonitorModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpQservMonitorModule::HttpQservMonitorModule(Controller::Ptr const& controller, string const& taskName,
                                               HttpProcessorConfig const& processorConfig,
                                               qhttp::Request::Ptr const& req,
                                               qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpQservMonitorModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "WORKERS")
        return _workers();
    else if (subModuleName == "SELECT-WORKER-BY-NAME")
        return _worker();
    else if (subModuleName == "QUERIES")
        return _userQueries();
    else if (subModuleName == "SELECT-QUERY-BY-ID")
        return _userQuery();
    else if (subModuleName == "CSS-SHARED-SCAN")
        return _cssSharedScan();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpQservMonitorModule::_workers() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    unsigned int const timeoutSec = query().optionalUInt("timeout_sec", workerResponseTimeoutSec());
    bool const keepResources = query().optionalUInt("keep_resources", 0) != 0;

    debug(__func__, "timeout_sec=" + to_string(timeoutSec));

    bool const allWorkers = true;
    auto const job = QservStatusJob::create(timeoutSec, allWorkers, controller());
    job->start();
    job->wait();

    json result;
    map<string, set<int>> schedulers2chunks;
    set<int> chunks;
    auto&& status = job->qservStatus();
    for (auto&& entry : status.workers) {
        auto&& worker = entry.first;
        bool success = entry.second;
        if (success) {
            auto info = status.info.at(worker);
            if (!keepResources) {
                info["resources"] = json::array();
            }
            result["status"][worker]["success"] = 1;
            result["status"][worker]["info"] = info;
            result["status"][worker]["queries"] = _getQueries(info);
            auto&& schedulers = info["processor"]["queries"]["blend_scheduler"]["schedulers"];
            for (auto&& scheduler : schedulers) {
                string const scheduerName = scheduler["name"];
                for (auto&& chunk2tasks : scheduler["chunk_to_num_tasks"]) {
                    int const chunk = chunk2tasks[0];
                    schedulers2chunks[scheduerName].insert(chunk);
                    chunks.insert(chunk);
                }
            }
        } else {
            result["status"][worker]["success"] = 0;
        }
    }
    json resultSchedulers2chunks;
    for (auto&& entry : schedulers2chunks) {
        auto&& scheduerName = entry.first;
        for (auto&& chunk : entry.second) {
            resultSchedulers2chunks[scheduerName].push_back(chunk);
        }
    }
    result["schedulers_to_chunks"] = resultSchedulers2chunks;
    result["chunks"] = _chunkInfo(chunks);
    return result;
}

json HttpQservMonitorModule::_worker() {
    debug(__func__);
    checkApiVersion(__func__, 18);

    auto const worker = params().at("worker");
    unsigned int const timeoutSec = query().optionalUInt("timeout_sec", workerResponseTimeoutSec());

    debug(__func__, "worker=" + worker);
    debug(__func__, "timeout_sec=" + to_string(timeoutSec));

    string const noParentJobId;
    GetStatusQservMgtRequest::CallbackType const onFinish = nullptr;

    auto const request = controller()->serviceProvider()->qservMgtServices()->status(worker, noParentJobId,
                                                                                     onFinish, timeoutSec);
    request->wait();

    json result;
    map<string, set<int>> schedulers2chunks;
    set<int> chunks;
    if (request->extendedState() == QservMgtRequest::ExtendedState::SUCCESS) {
        auto info = request->info();
        result["status"][worker]["success"] = 1;
        result["status"][worker]["info"] = info;
        result["status"][worker]["queries"] = _getQueries(info);
        auto&& schedulers = info["processor"]["queries"]["blend_scheduler"]["schedulers"];
        for (auto&& scheduler : schedulers) {
            string const scheduerName = scheduler["name"];
            for (auto&& chunk2tasks : scheduler["chunk_to_num_tasks"]) {
                int const chunk = chunk2tasks[0];
                schedulers2chunks[scheduerName].insert(chunk);
                chunks.insert(chunk);
            }
        }
    } else {
        result["status"][worker]["success"] = 0;
    }
    json resultSchedulers2chunks;
    for (auto&& entry : schedulers2chunks) {
        auto&& scheduerName = entry.first;
        for (auto&& chunk : entry.second) {
            resultSchedulers2chunks[scheduerName].push_back(chunk);
        }
    }
    result["schedulers_to_chunks"] = resultSchedulers2chunks;
    result["chunks"] = _chunkInfo(chunks);
    return result;
}

json HttpQservMonitorModule::_userQueries() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const config = controller()->serviceProvider()->config();

    string const queryStatus = query().optionalString("query_status", string());
    string const queryType = query().optionalString("query_type", string());
    unsigned int const queryAgeSec = query().optionalUInt("query_age", 0);
    unsigned int const minElapsedSec = query().optionalUInt("min_elapsed_sec", 0);
    unsigned int const timeoutSec = query().optionalUInt("timeout_sec", workerResponseTimeoutSec());
    unsigned int const limit4past = query().optionalUInt("limit4past", 1);
    string const searchPattern = query().optionalString("search_pattern", string());
    bool const searchBooleanMode = query().optionalUInt("search_boolean_mode", 0) != 0;
    bool const includeMessages = query().optionalUInt("include_messages", 0) != 0;

    debug(__func__, "query_status=" + queryStatus);
    debug(__func__, "query_type=" + queryType);
    debug(__func__, "query_age=" + to_string(queryAgeSec));
    debug(__func__, "min_elapsed_sec=" + to_string(minElapsedSec));
    debug(__func__, "timeout_sec=" + to_string(timeoutSec));
    debug(__func__, "limit4past=" + to_string(limit4past));
    debug(__func__, "search_pattern=" + searchPattern);
    debug(__func__, "search_boolean_mode=" + bool2str(searchBooleanMode));
    debug(__func__, "include_messages=" + bool2str(includeMessages));

    // Check which queries and in which schedulers are being executed
    // by Qserv workers.

    bool const allWorkers = true;
    auto const job = QservStatusJob::create(timeoutSec, allWorkers, controller());
    job->start();
    job->wait();

    map<QueryId, string> queryId2scheduler;
    auto&& status = job->qservStatus();
    for (auto&& entry : status.workers) {
        auto&& worker = entry.first;
        bool success = entry.second;
        if (success) {
            auto info = status.info.at(worker);
            auto&& schedulers = info["processor"]["queries"]["blend_scheduler"]["schedulers"];
            for (auto&& scheduler : schedulers) {
                string const scheduerName = scheduler["name"];
                for (auto&& queryId2count : scheduler["query_id_to_count"]) {
                    QueryId const queryId = queryId2count[0];
                    queryId2scheduler[queryId] = scheduerName;
                }
            }
        }
    }

    // Connect to the master database. Manage the new connection via the RAII-style
    // handler to ensure the transaction is automatically rolled-back in case of exceptions.
    ConnectionHandler const h(Connection::open(Configuration::qservCzarDbParams("qservMeta")));
    QueryGenerator const g(h.conn);

    // Get info on the ongoing queries
    json result;
    h.conn->executeInOwnTransaction(
            [&](auto conn) { result["queries"] = _currentUserQueries(conn, queryId2scheduler); });

    // Get info on the past queries matching the specified criteria.
    string constraints;
    if (queryStatus.empty()) {
        g.packCond(constraints, g.neq("status", "EXECUTING"));
    } else {
        g.packCond(constraints, g.eq("status", queryStatus));
    }
    if (!queryType.empty()) {
        g.packCond(constraints, g.eq("qType", queryType));
    }
    if (queryAgeSec > 0) {
        string const cond = g.gt(g.TIMESTAMPDIFF("SECOND", "submitted", Sql::NOW), queryAgeSec);
        g.packCond(constraints, cond);
    }
    if (minElapsedSec > 0) {
        string const cond = g.gt(g.TIMESTAMPDIFF("SECOND", "submitted", "completed"), minElapsedSec);
        g.packCond(constraints, cond);
    }
    if (!searchPattern.empty()) {
        string const mode = searchBooleanMode ? "BOOLEAN" : "NATURAL LANGUAGE";
        g.packCond(constraints, g.matchAgainst("query", searchPattern, mode));
    }
    h.conn->executeInOwnTransaction([&](auto conn) {
        result["queries_past"] = _pastUserQueries(conn, constraints, limit4past, includeMessages);
    });
    return result;
}

json HttpQservMonitorModule::_userQuery() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const queryId = stoull(params().at("id"));
    bool const includeMessages = query().optionalUInt("include_messages", 0) != 0;

    debug(__func__, "id=" + to_string(queryId));
    debug(__func__, "include_messages=" + bool2str(includeMessages));

    json result;

    // Connect to the master database
    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    ConnectionHandler const h(Connection::open(Configuration::qservCzarDbParams("qservMeta")));
    QueryGenerator const g(h.conn);

    h.conn->executeInOwnTransaction([&](auto conn) {
        unsigned int const limit4past = 0;
        result["queries_past"] =
                _pastUserQueries(conn, g.eq("queryId", queryId), limit4past, includeMessages);
    });
    return result;
}

json HttpQservMonitorModule::_currentUserQueries(Connection::Ptr& conn,
                                                 map<QueryId, string> const& queryId2scheduler) {
    QueryGenerator const g(conn);
    string const query =
            g.select(g.id("QStatsTmp", Sql::STAR), g.as(g.UNIX_TIMESTAMP("queryBegin"), "queryBegin_sec"),
                     g.as(g.UNIX_TIMESTAMP("lastUpdate"), "lastUpdate_sec"), g.as(Sql::NOW, "samplingTime"),
                     g.as(g.UNIX_TIMESTAMP(Sql::NOW), "samplingTime_sec"), g.id("QInfo", "query")) +
            g.from("QStatsTmp", "QInfo") +
            g.where(g.eq(g.id("QStatsTmp", "queryId"), g.id("QInfo", "queryId"))) +
            g.orderBy(make_pair(g.id("QStatsTmp", "queryBegin"), "DESC"));
    conn->execute(query);

    json result = json::array();
    if (conn->hasResult()) {
        Row row;
        while (conn->next(row)) {
            json resultRow;
            ::parseFieldIntoJson<QueryId>(__func__, row, "queryId", resultRow);
            ::parseFieldIntoJson<int>(__func__, row, "totalChunks", resultRow);
            ::parseFieldIntoJson<int>(__func__, row, "completedChunks", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "queryBegin", resultRow);
            ::parseFieldIntoJson<long>(__func__, row, "queryBegin_sec", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "lastUpdate", resultRow);
            ::parseFieldIntoJson<long>(__func__, row, "lastUpdate_sec", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "samplingTime", resultRow);
            ::parseFieldIntoJson<long>(__func__, row, "samplingTime_sec", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "query", resultRow);

            // Optionally, add the name of corresponding worker scheduler
            // if the one was already known for the query.

            QueryId const queryId = resultRow["queryId"];
            auto itr = queryId2scheduler.find(queryId);
            if (itr != queryId2scheduler.end()) {
                resultRow["scheduler"] = itr->second;
            }
            result.push_back(resultRow);
        }
    }
    return result;
}

json HttpQservMonitorModule::_pastUserQueries(Connection::Ptr& conn, string const& constraint,
                                              unsigned int limit4past, bool includeMessages) {
    json result = json::array();
    QueryGenerator const g(conn);
    string const query = g.select(Sql::STAR, g.as(g.UNIX_TIMESTAMP("submitted"), "submitted_sec"),
                                  g.as(g.UNIX_TIMESTAMP("completed"), "completed_sec"),
                                  g.as(g.UNIX_TIMESTAMP("returned"), "returned_sec")) +
                         g.from("QInfo") + g.where(constraint) + g.orderBy(make_pair("submitted", "DESC")) +
                         g.limit(limit4past);

    conn->execute(query);
    if (conn->hasResult()) {
        Row row;
        while (conn->next(row)) {
            json resultRow;
            ::parseFieldIntoJson<QueryId>(__func__, row, "queryId", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "qType", resultRow);
            ::parseFieldIntoJson<int>(__func__, row, "czarId", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "user", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "query", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "qTemplate", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "qMerge", resultRow, "");
            ::parseFieldIntoJson<string>(__func__, row, "status", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "submitted", resultRow);
            ::parseFieldIntoJson<long>(__func__, row, "submitted_sec", resultRow);
            ::parseFieldIntoJson<string>(__func__, row, "completed", resultRow, "");
            ::parseFieldIntoJson<long>(__func__, row, "completed_sec", resultRow, 0);
            ::parseFieldIntoJson<string>(__func__, row, "returned", resultRow, "");
            ::parseFieldIntoJson<long>(__func__, row, "returned_sec", resultRow, 0);
            ::parseFieldIntoJson<string>(__func__, row, "messageTable", resultRow, "");
            ::parseFieldIntoJson<string>(__func__, row, "resultLocation", resultRow, "");
            ::parseFieldIntoJson<string>(__func__, row, "resultQuery", resultRow, "");
            ::parseFieldIntoJson<long>(__func__, row, "chunkCount", resultRow, 0);
            ::parseFieldIntoJson<uint64_t>(__func__, row, "collectedBytes", resultRow, 0);
            ::parseFieldIntoJson<uint64_t>(__func__, row, "collectedRows", resultRow, 0);
            ::parseFieldIntoJson<uint64_t>(__func__, row, "finalRows", resultRow, 0);
            resultRow["messages"] = json::array();
            result.push_back(resultRow);
        }
        if (includeMessages) {
            for (auto& queryInfo : result) {
                string const query = g.select(Sql::STAR) + g.from("QMessages") +
                                     g.where(g.eq("queryId", queryInfo["queryId"].get<QueryId>())) +
                                     g.orderBy(make_pair("timestamp", "ASC"));
                conn->execute(query);
                if (conn->hasResult()) {
                    while (conn->next(row)) {
                        json resultRow;
                        ::parseFieldIntoJson<QueryId>(__func__, row, "queryId", resultRow);
                        ::parseFieldIntoJson<string>(__func__, row, "msgSource", resultRow);
                        ::parseFieldIntoJson<int>(__func__, row, "chunkId", resultRow);
                        ::parseFieldIntoJson<int>(__func__, row, "code", resultRow);
                        ::parseFieldIntoJson<string>(__func__, row, "message", resultRow);
                        ::parseFieldIntoJson<string>(__func__, row, "severity", resultRow);
                        ::parseFieldIntoJson<uint64_t>(__func__, row, "timestamp", resultRow);
                        queryInfo["messages"].push_back(resultRow);
                    }
                }
            }
        }
    }
    return result;
}

json HttpQservMonitorModule::_getQueries(json& workerInfo) const {
    // Find identifiers of all queries in the wait queues of all schedulers
    set<QueryId> qids;
    for (auto&& scheduler : workerInfo.at("processor").at("queries").at("blend_scheduler").at("schedulers")) {
        for (auto&& entry : scheduler.at("query_id_to_count")) {
            qids.insert(entry[0].get<QueryId>());
        }
    }

    // Connect to the master database
    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    auto const config = controller()->serviceProvider()->config();
    ConnectionHandler const h(Connection::open(Configuration::qservCzarDbParams("qservMeta")));
    QueryGenerator const g(h.conn);

    // Extract descriptions of those queries from qservMeta
    json result;
    if (!qids.empty()) {
        string const query = g.select(Sql::STAR) + g.from("QInfo") + g.where(g.in("queryId", qids));
        h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
            conn->execute(query);
            ::extractQInfo(conn, result);
        });
    }
    return result;
}

json HttpQservMonitorModule::_cssSharedScan() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    // Results are packed into the dictionary: family->database->table-sharedScan
    json resultSharedScan;
    auto const config = controller()->serviceProvider()->config();
    auto const cssAccess = qservCssAccess();
    for (auto&& familyName : config->databaseFamilies()) {
        bool const allDatabases = true;
        for (auto&& databaseName : config->databases(familyName, allDatabases)) {
            auto const database = config->databaseInfo(databaseName);
            // Do not include special tables into the report.
            vector<string> sharedScanTables;
            for (auto&& tableName : database.tables()) {
                auto const table = database.findTable(tableName);
                if (table.isPartitioned && !table.isRefMatch) {
                    sharedScanTables.emplace_back(table.name);
                    // Set the empty object as the default result for each table.
                    resultSharedScan[familyName][database.name][table.name] = json::object();
                }
            }
            // Override the default values for tables for which the shared scan
            // parameters were explicitly set.
            if (cssAccess->containsDb(database.name)) {
                for (auto&& tableName : sharedScanTables) {
                    if (cssAccess->containsTable(database.name, tableName)) {
                        try {
                            css::ScanTableParams const params =
                                    cssAccess->getScanTableParams(database.name, tableName);
                            resultSharedScan[familyName][database.name][tableName] =
                                    json::object({{"lockInMem", params.lockInMem ? 1 : 0},
                                                  {"scanRating", params.scanRating}});
                        } catch (css::NoSuchTable const&) {
                            // CSS key for the shared scans may not exist yet even if the table
                            // is known to CSS.
                            ;
                        }
                    }
                }
            }
        }
    }
    json result = json::object();
    result["css"]["shared_scan"] = resultSharedScan;
    return result;
}

json HttpQservMonitorModule::_chunkInfo(set<int> const& chunks) const {
    json result;
    auto const config = controller()->serviceProvider()->config();
    for (auto&& familyName : config->databaseFamilies()) {
        auto&& familyInfo = config->databaseFamilyInfo(familyName);
        /*
         * TODO: both versions of the 'Chunker' class need to be used due to non-overlapping
         * functionality and the interface.  The one from the spherical geometry packages
         * provides a simple interface for validating chunk numbers, meanwhile the other
         * one allows to extract spatial parameters of chunks. This duality will be
         * addressed later after migrating package 'partition' to use geometry utilities
         * of package 'sphgeom'.
         */
        lsst::sphgeom::Chunker const sphgeomChunker(familyInfo.numStripes, familyInfo.numSubStripes);
        lsst::partition::Chunker const partitionChunker(familyInfo.overlap, familyInfo.numStripes,
                                                        familyInfo.numSubStripes);
        for (auto&& chunk : chunks) {
            if (sphgeomChunker.valid(chunk)) {
                json chunkGeometry;
                auto&& box = partitionChunker.getChunkBounds(chunk);
                chunkGeometry["lat_min"] = box.getLatMin();
                chunkGeometry["lat_max"] = box.getLatMax();
                chunkGeometry["lon_min"] = box.getLonMin();
                chunkGeometry["lon_max"] = box.getLonMax();
                result[to_string(chunk)][familyInfo.name] = chunkGeometry;
            }
        }
    }
    return result;
}

}  // namespace lsst::qserv::replica
