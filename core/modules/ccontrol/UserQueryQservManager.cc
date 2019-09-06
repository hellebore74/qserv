// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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
#include "ccontrol/UserQueryQservManager.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qdisp/MessageStore.h"
#include "sql/SqlBulkInsert.h"
#include "sql/SqlConnection.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQueryQservManager");

}


namespace lsst {
namespace qserv {
namespace ccontrol {


UserQueryQservManager::UserQueryQservManager(UserQueryConfig const& queryConfig,
                                             std::vector<std::string> const& args)
        : _resultTableName("qserv_manager_" + queryConfig.userQueryId),
          _messageStore(std::make_shared<qdisp::MessageStore>()), // TODO I don't understand how this is hooked up to anything. Is it just an interface for log?
          _resultDbConn(queryConfig.resultDbConn),
          _resultDb(queryConfig.resultDb)
{
    if (args.size() != 1) {
        throw UserQueryError("Expected exactly one argument to CALL QSERV_MANAGER");
    }
    _value = args[0];
}


void UserQueryQservManager::submit() {
    // create result table, one could use formCreateTable() method
    // to build statement but it does not set NULL flag on TIMESTAMP columns
    std::string createTable = "CREATE TABLE " + _resultTableName + "(response BLOB)"; // The columns must match resColumns, below.
    LOGS(_log, LOG_LVL_TRACE, "creating result table: " << createTable);
    sql::SqlErrorObject errObj;
    if (!_resultDbConn->runQuery(createTable, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "failed to create result table: " << errObj.errMsg());
        std::string message = "Internal failure, failed to create result table: " + errObj.errMsg();
        _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
        return;
    }

    // For now just insert the parsed argument to QSERV_MANAGER into the result table.

    std::vector<std::string> resColumns({"response"}); // this must match the schema in the CREATE TABLE statement above.
    sql::SqlBulkInsert bulkInsert(_resultDbConn.get(), _resultTableName, resColumns);
    std::vector<std::string> values = { _value };
    if (!bulkInsert.addRow(values, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "error updating result table: " << errObj.errMsg());
        std::string message = "Internal failure, error updating result table: " + errObj.errMsg();
        _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
        return;
    }
    if (!bulkInsert.flush(errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "error updating result table: " << errObj.errMsg());
        std::string message = "Internal failure, error updating result table: " + errObj.errMsg();
        _messageStore->addMessage(-1, 1051, message, MessageSeverity::MSG_ERROR);
        _qState = ERROR;
        return;
    }
    _qState = SUCCESS;
}


std::string UserQueryQservManager::getResultQuery() const {
    std::string ret = "SELECT * FROM " + _resultDb + "." + _resultTableName;
    return ret;
}


}}} // lsst::qserv::ccontrol
