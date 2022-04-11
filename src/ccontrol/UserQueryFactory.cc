// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2017 AURA/LSST.
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
#include "ccontrol/UserQueryFactory.h"

// System headers
#include <cassert>
#include <cstdlib>
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/ConfigError.h"
#include "ccontrol/ConfigMap.h"
#include "ccontrol/ParseRunner.h"
#include "ccontrol/UserQueryAsyncResult.h"
#include "ccontrol/UserQueryDrop.h"
#include "ccontrol/UserQueryFlushChunksCache.h"
#include "ccontrol/UserQueryInvalid.h"
#include "ccontrol/UserQueryProcessList.h"
#include "ccontrol/UserQueryResources.h"
#include "ccontrol/UserQuerySelect.h"
#include "ccontrol/UserQuerySelectCountStar.h"
#include "ccontrol/UserQueryType.h"
#include "css/CssAccess.h"
#include "css/KvInterfaceImplMem.h"
#include "czar/CzarConfig.h"
#include "mysql/MySqlConfig.h"
#include "parser/ParseException.h"
#include "qdisp/Executive.h"
#include "qdisp/MessageStore.h"
#include "qmeta/QMetaMysql.h"
#include "qmeta/QMetaSelect.h"
#include "qmeta/QStatusMysql.h"
#include "qproc/DatabaseModels.h"
#include "qproc/QuerySession.h"
#include "qproc/SecondaryIndex.h"
#include "query/FromList.h"
#include "query/SelectStmt.h"
#include "rproc/InfileMerger.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQueryFactory");
}

namespace lsst::qserv::ccontrol {

using userQuerySharedResourcesPtr = std::shared_ptr<UserQuerySharedResources>;

/**
 * @brief Determine if the table name in the FROM statement refers to PROCESSLIST table.
 *
 * @param stmt SelectStmt representing the query.
 * @param defaultDb Default database name, may be empty.
 * @return true if the query refers only to the PROCESSLIST table.
 * @return false if the query does not refer only to the PROCESSLIST table.
 */
bool _stmtRefersToProcessListTable(query::SelectStmt::Ptr& stmt, std::string defaultDb) {
    auto const& tableRefList = stmt->getFromList().getTableRefList();
    if (tableRefList.size() != 1) return false;
    auto const& tblRef = tableRefList[0];
    std::string const& db = tblRef->getDb().empty() ? defaultDb : tblRef->getDb();
    if (UserQueryType::isProcessListTable(db, tblRef->getTable())) return true;
    return false;
}

/**
 * @brief Make a UserQueryProcessList (or UserQueryInvalid) from given parameters.
 *
 * @param stmt The SelectStmt representing the query.
 * @param sharedResources Resources used by UserQueryFactory to create UserQueries.
 * @param userQueryId Unique string identifying the query.
 * @param resultDb Name of the databse that will contain results.
 * @param aQuery The original query string.
 * @param async If the query is to be run asynchronously.
 * @return std::shared_ptr<UserQuery>, will be a UserQueryProcessList or UserQueryInvalid.
 */
std::shared_ptr<UserQuery> _makeUserQueryProcessList(query::SelectStmt::Ptr& stmt,
                                                     userQuerySharedResourcesPtr& sharedResources,
                                                     std::string const& userQueryId,
                                                     std::string const& resultDb, std::string const& aQuery,
                                                     bool async) {
    if (async) {
        // no point supporting async for these
        auto uq = std::make_shared<UserQueryInvalid>("SUBMIT is not allowed with query: " + aQuery);
        return uq;
    }
    LOGS(_log, LOG_LVL_DEBUG, "SELECT query is a PROCESSLIST");
    try {
        return std::make_shared<UserQueryProcessList>(stmt, sharedResources->resultDbConn.get(),
                                                      sharedResources->qMetaSelect,
                                                      sharedResources->qMetaCzarId, userQueryId, resultDb);
    } catch (std::exception const& exc) {
        return std::make_shared<UserQueryInvalid>(exc.what());
    }
}

/**
 * @brief Determine if the qmeta database has a metadata table with chunks & row
 *        counts that represents the table in the FROM statement for a SELECT
 *        COUNT(*) query.
 *
 * @param stmt The SelectStmt representing the query.
 * @param sharedResources Resources used by UserQueryFactory to create UserQueries.
 * @param defaultDb Default database name, may be empty.
 * @param rowsTable Output variable, will be set to the name of the rows table if it
 *                  exists, otherwise will be set to an empty string.
 * @return true if the qmeta table containing the row counts is present in qmeta.
 * @return false if the table is not present in qmeta.
 */
bool qmetaHasDataForSelectCountStarQuery(query::SelectStmt::Ptr const& stmt,
                                         userQuerySharedResourcesPtr& sharedResources,
                                         std::string const& defaultDb, std::string& rowsTable) {
    auto const& tableRefList = stmt->getFromList().getTableRefList();
    // by definition a simple COUNT(*) should have exactly one table ref.
    assert(tableRefList.size() > 0);
    auto const& tableRefPtr = tableRefList[0];
    assert(tableRefPtr != nullptr);
    auto fromDb = tableRefPtr->getDb();
    if (fromDb.empty()) {
        fromDb = defaultDb;
    }
    auto const& fromTable = tableRefPtr->getTable();
    rowsTable = fromDb + "__" + fromTable + "__rows";
    // TODO consider using QMetaSelect instead of making a new connection.
    auto cnx = sql::SqlConnectionFactory::make(sharedResources->czarConfig.getMySqlQmetaConfig());
    sql::SqlErrorObject err;
    auto tableExists = cnx->tableExists(rowsTable, err);
    LOGS(_log, LOG_LVL_DEBUG,
         *stmt << " rows table: " << rowsTable << (tableExists ? " exists" : " does not exist"));
    if (not tableExists) rowsTable = "";
    return tableExists;
}

std::shared_ptr<UserQuerySharedResources> makeUserQuerySharedResources(
        czar::CzarConfig const& czarConfig, std::shared_ptr<qproc::DatabaseModels> const& dbModels,
        std::string const& czarName) {
    return std::make_shared<UserQuerySharedResources>(
            czarConfig,
            css::CssAccess::createFromConfig(czarConfig.getCssConfigMap(), czarConfig.getEmptyChunkPath()),
            czarConfig.getMySqlResultConfig(),
            std::make_shared<qproc::SecondaryIndex>(czarConfig.getMySqlQmetaConfig()),
            std::make_shared<qmeta::QMetaMysql>(czarConfig.getMySqlQmetaConfig()),
            std::make_shared<qmeta::QStatusMysql>(czarConfig.getMySqlQStatusDataConfig()),
            std::make_shared<qmeta::QMetaSelect>(czarConfig.getMySqlQmetaConfig()),
            sql::SqlConnectionFactory::make(czarConfig.getMySqlResultConfig()), dbModels, czarName,
            czarConfig.getInteractiveChunkLimit());
}

////////////////////////////////////////////////////////////////////////
UserQueryFactory::UserQueryFactory(czar::CzarConfig const& czarConfig,
                                   qproc::DatabaseModels::Ptr const& dbModels, std::string const& czarName)
        : _userQuerySharedResources(makeUserQuerySharedResources(czarConfig, dbModels, czarName)) {
    _executiveConfig = std::make_shared<qdisp::ExecutiveConfig>(
            czarConfig.getXrootdFrontendUrl(), czarConfig.getQMetaSecondsBetweenChunkUpdates());

    // When czar crashes/exits while some queries are still in flight they
    // are left in EXECUTING state in QMeta. We want to cleanup that state
    // to avoid confusion. Note that when/if clean czar restart is implemented
    // we'll need a new logic to restart query processing.
    _userQuerySharedResources->queryMetadata->cleanup(_userQuerySharedResources->qMetaCzarId);

    // Add logging context with czar ID
    qmeta::CzarId qMetaCzarId = _userQuerySharedResources->qMetaCzarId;
    LOG_MDC_INIT([qMetaCzarId]() { LOG_MDC("CZID", std::to_string(qMetaCzarId)); });
}

UserQuery::Ptr UserQueryFactory::newUserQuery(std::string const& aQuery, std::string const& defaultDb,
                                              qdisp::SharedResources::Ptr const& qdispSharedResources,
                                              std::string const& userQueryId, std::string const& msgTableName,
                                              std::string const& resultDb) {
    // result location could potentially be specified by SUBMIT command, for now
    // we keep it empty which means that UserQuerySelect uses default result table.
    std::string resultLocation;

    // First check for SUBMIT and strip it
    std::string query = aQuery;

    std::string stripped;
    bool async = false;
    if (UserQueryType::isSubmit(query, stripped)) {
        // SUBMIT is only allowed with SELECT for now, complain if anything else is there
        if (!UserQueryType::isSelect(stripped)) {
            auto uq = std::make_shared<UserQueryInvalid>("Invalid or unsupported query: " + query);
            return uq;
        }
        async = true;
        query = stripped;
    }

    std::string dbName, tableName;
    bool full = false;
    QueryId userJobId = 0;

    if (UserQueryType::isSelect(query)) {
        // Processing regular select query
        bool sessionValid = true;
        std::string errorExtra;

        // Parse SELECT

        ParseRunner::Ptr parser;
        try {
            parser = std::make_shared<ParseRunner>(query);
        } catch (parser::ParseException& e) {
            return std::make_shared<UserQueryInvalid>(std::string("ParseException:") + e.what());
        }
        auto stmt = parser->getSelectStmt();

        // handle special database/table names
        if (_stmtRefersToProcessListTable(stmt, defaultDb)) {
            return _makeUserQueryProcessList(stmt, _userQuerySharedResources, userQueryId, resultDb, aQuery,
                                             async);
        }

        /// Determine if a SelectStmt is a simple COUNT(*) query and can be run as an optimized query.
        /// It may not be runnable as an optimzed simple COUNT(*) query because:
        /// * The queryMeta tables do not have the required information.
        /// * The option to run optimized COUNT(*) queries is turned off.
        /// * It is not a COUNT(*) query.
        /// * It is a COUNT(*) query but is too complex for the simple optimization.
        std::string rowsTable;
        std::string countSpelling;
        if (UserQueryType::isSimpleCountStar(stmt, countSpelling) &&
            qmetaHasDataForSelectCountStarQuery(stmt, _userQuerySharedResources, defaultDb, rowsTable)) {
            auto uq = std::make_shared<UserQuerySelectCountStar>(
                    _userQuerySharedResources->resultDbConn, _userQuerySharedResources->qMetaSelect,
                    userQueryId, rowsTable, resultDb, countSpelling, async);
            LOGS(_log, LOG_LVL_DEBUG, "make UserQuerySelectCountStar");
            return uq;
        }

        // This is a regular SELECT for qserv

        // Currently using the database for results to get schema information.
        auto qs = std::make_shared<qproc::QuerySession>(_userQuerySharedResources->css,
                                                        _userQuerySharedResources->databaseModels, defaultDb,
                                                        _userQuerySharedResources->interactiveChunkLimit);
        try {
            qs->analyzeQuery(query, stmt);
        } catch (...) {
            errorExtra = "Unknown failure occurred setting up QuerySession (query is invalid).";
            LOGS(_log, LOG_LVL_ERROR, errorExtra);
            sessionValid = false;
        }
        if (!qs->getError().empty()) {
            LOGS(_log, LOG_LVL_ERROR, "Invalid query: " << qs->getError());
            sessionValid = false;
        }

        auto messageStore = std::make_shared<qdisp::MessageStore>();
        std::shared_ptr<qdisp::Executive> executive;
        std::shared_ptr<rproc::InfileMergerConfig> infileMergerConfig;
        if (sessionValid) {
            executive = qdisp::Executive::create(*_executiveConfig, messageStore, qdispSharedResources,
                                                 _userQuerySharedResources->queryStatsData, qs);
            infileMergerConfig = std::make_shared<rproc::InfileMergerConfig>(
                    _userQuerySharedResources->czarConfig, _userQuerySharedResources->mysqlResultConfig);
        }

        auto uq = std::make_shared<UserQuerySelect>(
                qs, messageStore, executive, _userQuerySharedResources->databaseModels, infileMergerConfig,
                _userQuerySharedResources->secondaryIndex, _userQuerySharedResources->queryMetadata,
                _userQuerySharedResources->queryStatsData, _userQuerySharedResources->semaMgrConnections,
                _userQuerySharedResources->qMetaCzarId, errorExtra, async, resultDb);
        if (sessionValid) {
            uq->qMetaRegister(resultLocation, msgTableName);
            uq->setupChunking();
            uq->setupMerger();
            uq->saveResultQuery();
        }
        return uq;
    } else if (UserQueryType::isSelectResult(query, userJobId)) {
        auto uq = std::make_shared<UserQueryAsyncResult>(userJobId, _userQuerySharedResources->qMetaCzarId,
                                                         _userQuerySharedResources->queryMetadata,
                                                         _userQuerySharedResources->resultDbConn.get());
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryAsyncResult: userJobId=" << userJobId);
        return uq;
    } else if (UserQueryType::isDropTable(query, dbName, tableName)) {
        // processing DROP TABLE
        if (dbName.empty()) {
            dbName = defaultDb;
        }
        auto uq = std::make_shared<UserQueryDrop>(_userQuerySharedResources->css, dbName, tableName,
                                                  _userQuerySharedResources->resultDbConn.get(),
                                                  _userQuerySharedResources->queryMetadata,
                                                  _userQuerySharedResources->qMetaCzarId);
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryDrop: " << dbName << "." << tableName);
        return uq;
    } else if (UserQueryType::isDropDb(query, dbName)) {
        // processing DROP DATABASE
        auto uq = std::make_shared<UserQueryDrop>(_userQuerySharedResources->css, dbName, std::string(),
                                                  _userQuerySharedResources->resultDbConn.get(),
                                                  _userQuerySharedResources->queryMetadata,
                                                  _userQuerySharedResources->qMetaCzarId);
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryDrop: db=" << dbName);
        return uq;
    } else if (UserQueryType::isFlushChunksCache(query, dbName)) {
        auto uq = std::make_shared<UserQueryFlushChunksCache>(_userQuerySharedResources->css, dbName,
                                                              _userQuerySharedResources->resultDbConn.get());
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryFlushChunksCache: " << dbName);
        return uq;
    } else if (UserQueryType::isShowProcessList(query, full)) {
        LOGS(_log, LOG_LVL_DEBUG, "make UserQueryProcessList: full=" << (full ? 'y' : 'n'));
        try {
            return std::make_shared<UserQueryProcessList>(full, _userQuerySharedResources->resultDbConn.get(),
                                                          _userQuerySharedResources->qMetaSelect,
                                                          _userQuerySharedResources->qMetaCzarId, userQueryId,
                                                          resultDb);
        } catch (std::exception const& exc) {
            return std::make_shared<UserQueryInvalid>(exc.what());
        }
    } else if (UserQueryType::isCall(query)) {
        auto parser = std::make_shared<ParseRunner>(
                query, _userQuerySharedResources->makeUserQueryResources(userQueryId, resultDb));
        return parser->getUserQuery();
    } else {
        // something that we don't recognize
        auto uq = std::make_shared<UserQueryInvalid>("Invalid or unsupported query: " + query);
        return uq;
    }
}

}  // namespace lsst::qserv::ccontrol
