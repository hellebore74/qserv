// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2018 LSST Corporation.
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

#ifndef LSST_QSERV_CCONTROL_USERQUERYSELECT_H
#define LSST_QSERV_CCONTROL_USERQUERYSELECT_H
/**
  * @file
  *
  * @brief Umbrella container for user query state
   *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <cstdint>
#include <memory>
#include <mutex>

// Third-party headers

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "css/StripingParams.h"
#include "qmeta/QInfo.h"
#include "qmeta/QStatus.h"
#include "qmeta/types.h"
#include "qproc/ChunkSpec.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace qdisp {
    class Executive;
    class MessageStore;
    class QdispPool;
}
namespace qmeta {
    class QMeta;
}
namespace qproc {
    class DatabaseModels;
    class QuerySession;
    class SecondaryIndex;
}
namespace query {
    class ColumnRef;
    class SelectStmt;
}
namespace rproc {
    class InfileMerger;
    class InfileMergerConfig;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace ccontrol {

/// UserQuerySelect : implementation of the UserQuery for regular SELECT statements.
class UserQuerySelect : public UserQuery {
public:

    UserQuerySelect(std::shared_ptr<qproc::QuerySession> const& qs,
                    std::shared_ptr<qdisp::MessageStore> const& messageStore,
                    std::shared_ptr<qdisp::Executive> const& executive,
                    std::shared_ptr<qproc::DatabaseModels> const& dbModels,
                    std::shared_ptr<rproc::InfileMergerConfig> const& infileMergerConfig,
                    std::shared_ptr<qproc::SecondaryIndex> const& secondaryIndex,
                    std::shared_ptr<qmeta::QMeta> const& queryMetadata,
                    std::shared_ptr<qmeta::QStatus> const& queryStatsData,
                    qmeta::CzarId czarId,
                    std::shared_ptr<qdisp::QdispPool> const& qdispPool,
                    std::string const& errorExtra,
                    bool async,
                    std::string const& resultDb);

    UserQuerySelect(UserQuerySelect const&) = delete;
    UserQuerySelect& operator=(UserQuerySelect const&) = delete;

    /**
     *  @param resultLocation:  Result location, if empty use result table with unique
     *                          name generated from query ID.
     *  @param msgTableName:  Message table name.
     */
    void qMetaRegister(std::string const& resultLocation, std::string const& msgTableName);

    // Accessors

    /// @return a non-empty string describing the current error state
    /// Returns an empty string if no errors have been detected.
    std::string getError() const override;

    /// Begin execution of the query over all ChunkSpecs added so far.
    void submit() override;

    /// Wait until the query has completed execution.
    /// @return the final execution state.
    QueryState join() override;

    /// Stop a query in progress (for immediate shutdowns)
    void kill() override;

    /// Release resources related to user query
    void discard() override;

    // Delegate objects
    std::shared_ptr<qdisp::MessageStore> getMessageStore() override {
        return _messageStore; }

    /// @return Name of the result table for this query, can be empty
    std::string getResultTableName() const override { return _resultTable; }

    /// @return Result location for this query, can be empty
    std::string getResultLocation() const override { return _resultLoc; }

    /// @return get the SELECT statement to be executed by proxy
    std::string getResultQuery() const override;

    std::string getQueryIdString() const override;

    /// @return this query's QueryId.
    QueryId getQueryId() const override { return _qMetaQueryId; }

    /// @return True if query is async query
    bool isAsync() const override { return _async; }

    void setupChunking();

    /// set up the merge table (stores results from workers)
    /// @throw UserQueryError if the merge table can't be set up (maybe the user query is not valid?). The
    /// exception's what() message will be returned to the user.
    void setupMerger();

    /// save the result query in the query metadata
    void saveResultQuery();

private:
    /// @return ORDER BY part of SELECT statement that gets executed by the proxy
    std::string _getResultOrderBy() const;

    /**
     * @brief Operates on _infileMergerConfig.mergeStmt.
     *        In the merge statement expand SELECT * to named columns (if there is a SELECT *).
     */
    void _expandSelectStarInMergeStatment();

    /**
     * @brief Operates on _infileMergerConfig.mergeStmt.
     *        Verifies that the columns used by clauses in the merge statement are represented in the select
     *        list.
     *
     * @throws UserQueryError if there is a column in the statement that is not in the select list.
     */
    void _verifyColumnsInMergeStatement();

    void _discardMerger();
    void _qMetaUpdateStatus(qmeta::QInfo::QStatus qStatus);
    void _qMetaAddChunks(std::vector<int> const& chunks);

    // Delegate classes
    std::shared_ptr<qproc::QuerySession> _qSession;
    std::shared_ptr<qdisp::MessageStore> _messageStore;
    std::shared_ptr<qdisp::Executive> _executive;
    std::shared_ptr<qproc::DatabaseModels> _databaseModels;
    std::shared_ptr<rproc::InfileMergerConfig> _infileMergerConfig;
    std::shared_ptr<rproc::InfileMerger> _infileMerger;
    std::shared_ptr<qproc::SecondaryIndex> _secondaryIndex;
    std::shared_ptr<qmeta::QMeta> _queryMetadata;
    std::shared_ptr<qmeta::QStatus> _queryStatsData;

    qmeta::CzarId _qMetaCzarId; ///< Czar ID in QMeta database
    QueryId _qMetaQueryId{0};      ///< Query ID in QMeta database
    std::shared_ptr<qdisp::QdispPool> _qdispPool;
    /// QueryId in a standard string form, initially set to unknown.
    std::string _queryIdStr{QueryIdHelper::makeIdStr(0, true)};
    bool _killed{false};
    std::mutex _killMutex;
    mutable std::string _errorExtra;    ///< Additional error information
    std::string _resultTable;   ///< Result table name
    std::string _resultLoc;     ///< Result location
    std::string _resultDb;      ///< Result database (todo is this the same as resultLoc??)
    bool _async;                ///< true for async query
};

}}} // namespace lsst::qserv:ccontrol

#endif // LSST_QSERV_CCONTROL_USERQUERYSELECT_H
