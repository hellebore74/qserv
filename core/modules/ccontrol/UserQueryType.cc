/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */


// Class header
#include "ccontrol/UserQueryType.h"

// System headers

// Third-party headers
#include "boost/regex.hpp"
#include "boost/algorithm/string/case_conv.hpp"

// LSST headers
#include "lsst/log/Log.h"


namespace {


LOG_LOGGER _log = LOG_GET("lsst.qserv.ccontrol.UserQueryType");


// regex for DROP {DATABASE|SCHEMA} dbname; db name can be in quotes;
// db name will be in group 3.
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _dropDbRe(R"(^drop\s+(database|schema)\s+(["`]?)(\w+)\2\s*;?\s*$)",
                       boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);


// regex for DROP TABLE [dbname.]table; both table and db names can be in quotes;
// db name will be in group 3, table name in group 5.
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _dropTableRe(R"(^drop\s+table\s+((["`]?)(\w+)\2[.])?(["`]?)(\w+)\4\s*;?\s*$)",
                          boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);


// regex for SELECT *
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _selectRe(R"(^select\s+.+$)",
                       boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);


// regex for FLUSH QSERV_CHUNKS_CACHE [FOR database]
// Note that parens around whole string are not part of the regex but raw string literal
// db name will be in group 3.
boost::regex _flushEmptyRe(R"(^flush\s+qserv_chunks_cache(\s+for\s+(["`]?)(\w+)\2)?\s*;?\s*$)",
                           boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);


// regex for SHOW [FULL] PROCESSLIST
// if FULL is present then group 1 is non-empty
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _showProcessListRe(R"(^show\s+(full\s+)?processlist$)",
                                boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);


// regex for SUBMIT ...
// group 1 is the query without SUBMIT prefix
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _submitRe(R"(^submit\s+(.+)$)",
                       boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);


// regex for SELECT * FROM QSERV_RESULT(12345)
// group 1 is the query ID number
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _selectResultRe(R"(^select\s+\*\s+from\s+qserv_result\s*\(\s*(\d+)\s*\)$)",
                             boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);


// regex for KILL [QUERY|CONNECTION] 12345
// group 1 is the thread ID number
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _killRe(R"(^kill\s+(?:QUERY\s+|CONNECTION\s+)?(\d+)\s*$)",
                     boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);


// regex for CANCEL 12345
// group 1 is the query ID number
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _cancelRe(R"(^cancel\s+(\d+)\s*$)",
                       boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);


// regex for CALL
// Note that parens around whole string are not part of the regex but raw string literal
boost::regex _callRe(R"(^call\s+.+$)",
                       boost::regex::ECMAScript | boost::regex::icase | boost::regex::optimize);


} // namespace


namespace lsst {
namespace qserv {
namespace ccontrol {


/// Returns true if query is DROP DATABASE
bool UserQueryType::isDropDb(std::string const& query, std::string& dbName) {
    LOGS(_log, LOG_LVL_TRACE, "isDropDb: " << query);
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _dropDbRe);
    if (match) {
        dbName = sm.str(3);
        LOGS(_log, LOG_LVL_TRACE, "isDropDb: match: " << dbName);
    }
    return match;
}


/// Returns true if query is DROP TABLE
bool UserQueryType::isDropTable(std::string const& query, std::string& dbName, std::string& tableName) {
    LOGS(_log, LOG_LVL_TRACE, "isDropTable: " << query);
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _dropTableRe);
    if (match) {
        dbName = sm.str(3);
        tableName = sm.str(5);
        LOGS(_log, LOG_LVL_TRACE, "isDropTable: match: " << dbName << "." << tableName);
    }
    return match;
}


/// Returns true if query is regular SELECT (not isSelectResult())
bool UserQueryType::isSelect(std::string const& query) {
    LOGS(_log, LOG_LVL_TRACE, "isSelect: " << query);
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _selectRe);
    if (match) {
        LOGS(_log, LOG_LVL_TRACE, "isSelect: match");
        if (boost::regex_match(query, sm, _selectResultRe)) {
            LOGS(_log, LOG_LVL_TRACE, "isSelect: match select result");
            match = false;
        }
    }
    return match;
}


/// Returns true if query is FLUSH QSERV_CHUNKS_CACHE [FOR database]
bool UserQueryType::isFlushChunksCache(std::string const& query, std::string& dbName) {
    LOGS(_log, LOG_LVL_TRACE, "isFlushChunksCache: " << query);
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _flushEmptyRe);
    if (match) {
        dbName = sm.str(3);
        LOGS(_log, LOG_LVL_TRACE, "isFlushChunksCache: match: " << dbName);
    }
    return match;
}


/// Returns true if query is SHOW [FULL] PROCESSLIST
bool UserQueryType::isShowProcessList(std::string const& query, bool& full) {
    LOGS(_log, LOG_LVL_TRACE, "isShowProcessList: " << query);
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _showProcessListRe);
    if (match) {
        full = sm.length(1) != 0;
        LOGS(_log, LOG_LVL_TRACE, "isShowProcessList: full: " << (full ? 'y' : 'n'));
    }
    return match;
}


/// Returns true if table name refers to PROCESSLIST table
bool UserQueryType::isProcessListTable(std::string const& dbName, std::string const& tblName) {
    return boost::to_upper_copy(dbName) == "INFORMATION_SCHEMA" &&
            boost::to_upper_copy(tblName) == "PROCESSLIST";
}


/// Returns true if query is SUBMIT ...
bool UserQueryType::isSubmit(std::string const& query, std::string& stripped) {
     LOGS(_log, LOG_LVL_TRACE, "isSubmit: " << query);
     boost::smatch sm;
     bool match = boost::regex_match(query, sm, _submitRe);
     if (match) {
         stripped = sm.str(1);
         LOGS(_log, LOG_LVL_TRACE, "isSubmit: match: " << stripped);
     }
     return match;
}


/// Returns true if query is SELECT * FROM QSERV_RESULT(...)
bool UserQueryType::isSelectResult(std::string const& query, QueryId& queryId) {
     LOGS(_log, LOG_LVL_TRACE, "isSelectResult: " << query);
     boost::smatch sm;
     bool match = boost::regex_match(query, sm, _selectResultRe);
     if (match) {
         queryId = std::stoull(sm.str(1));
         LOGS(_log, LOG_LVL_TRACE, "isSelectResult: queryId: " << queryId);
     }
     return match;
}


// Returns true if query is KILL [QUERY|CONNECTION] NNN
bool UserQueryType::isKill(std::string const& query, int& threadId) {
    LOGS(_log, LOG_LVL_TRACE, "isKill: " << query);
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _killRe);
    if (match) {
        threadId = std::stoi(sm.str(1));
        LOGS(_log, LOG_LVL_TRACE, "isKill: threadId: " << threadId);
    }
    return match;
}


// Returns true if query is CANCEL NNN
bool UserQueryType::isCancel(std::string const& query, QueryId& queryId) {
    LOGS(_log, LOG_LVL_TRACE, "isCancel: " << query);
    boost::smatch sm;
    bool match = boost::regex_match(query, sm, _cancelRe);
    if (match) {
        queryId = std::stoull(sm.str(1));
        LOGS(_log, LOG_LVL_TRACE, "isCancel: queryId: " << queryId);
    }
    return match;
}


bool UserQueryType::isCall(std::string const& query) {
    LOGS(_log, LOG_LVL_TRACE, "isCall: " << query);
    return boost::regex_match(query, _callRe);
}


}}} // namespace lsst::qserv::ccontrol
