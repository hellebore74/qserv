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
#include "replica/ControllerApp.h"

// System headers
#include <fstream>
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "replica/ChunkedTable.h"
#include "replica/DeleteRequest.h"
#include "replica/DisposeRequest.h"
#include "replica/EchoRequest.h"
#include "replica/FindRequest.h"
#include "replica/FindAllRequest.h"
#include "replica/DirectorIndexRequest.h"
#include "replica/ReplicationRequest.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"
#include "replica/SqlAlterTablesRequest.h"
#include "replica/SqlCreateDbRequest.h"
#include "replica/SqlCreateIndexesRequest.h"
#include "replica/SqlCreateTableRequest.h"
#include "replica/SqlCreateTablesRequest.h"
#include "replica/SqlDeleteDbRequest.h"
#include "replica/SqlDeleteTablePartitionRequest.h"
#include "replica/SqlDeleteTableRequest.h"
#include "replica/SqlDisableDbRequest.h"
#include "replica/SqlDropIndexesRequest.h"
#include "replica/SqlGetIndexesRequest.h"
#include "replica/SqlEnableDbRequest.h"
#include "replica/SqlGrantAccessRequest.h"
#include "replica/SqlQueryRequest.h"
#include "replica/SqlRemoveTablePartitionsRequest.h"
#include "replica/SqlRowStatsRequest.h"
#include "replica/SqlSchemaUtils.h"
#include "replica/StatusRequest.h"
#include "replica/StopRequest.h"
#include "util/BlockPost.h"

using namespace std;
using namespace lsst::qserv::replica;

namespace {

string const description =
        "This application allows launching Controller requests, and it's meant"
        " for both testing all known types of requests and for various manual fix up"
        " operations in a replication setup.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;

/**
 * This is the helper class for submitting the management requests
 * which are meant to request a status or stop an on-going request.
 */
class ManagementRequestLauncher {
public:
    // All (but the normal one) forms of construction are prohibited

    ManagementRequestLauncher() = delete;
    ManagementRequestLauncher(ManagementRequestLauncher const&) = delete;
    ManagementRequestLauncher& operator=(ManagementRequestLauncher const&) = delete;

    ManagementRequestLauncher(Controller::Ptr const& controller, string workerName, string affectedRequestId,
                              int priority, bool doNotTrackRequest)
            : _controller(controller),
              _workerName(workerName),
              _affectedRequestId(affectedRequestId),
              _priority(priority),
              _doNotTrackRequest(doNotTrackRequest) {}
    ~ManagementRequestLauncher() = default;

    template <typename REQUEST>
    typename REQUEST::Ptr status() const {
        return _controller->statusById<REQUEST>(_workerName, _affectedRequestId, REQUEST::extendedPrinter,
                                                !_doNotTrackRequest);
    }

    template <typename REQUEST>
    typename REQUEST::Ptr stop() const {
        return _controller->stopById<REQUEST>(_workerName, _affectedRequestId, REQUEST::extendedPrinter,
                                              !_doNotTrackRequest);
    }

private:
    /// Pointer to the controller for launching requests
    Controller::Ptr const _controller;

    /// The name of a worker which will execute a request
    std::string const _workerName;

    /// An identifier of a request for operations over known requests
    std::string const _affectedRequestId;

    /// The priority level of a request
    int const _priority;

    /// Do not track requests waiting before they finish
    bool const _doNotTrackRequest;
};

}  // namespace

namespace lsst::qserv::replica {

ControllerApp::Ptr ControllerApp::create(int argc, char* argv[]) {
    return Ptr(new ControllerApp(argc, argv));
}

ControllerApp::ControllerApp(int argc, char* argv[])
        : Application(argc, argv, description, injectDatabaseOptions, boostProtobufVersionCheck,
                      enableServiceProvider) {
    _configureParser();
}

void ControllerApp::_configureParser() {
    parser().commands("request",
                      {"REPLICATE",
                       "DELETE",
                       "FIND",
                       "FIND_ALL",
                       "ECHO",
                       "SQL_ALTER_TABLES",
                       "SQL_QUERY",
                       "SQL_CREATE_DATABASE",
                       "SQL_DELETE_DATABASE",
                       "SQL_ENABLE_DATABASE",
                       "SQL_DISABLE_DATABASE",
                       "SQL_GRANT_ACCESS",
                       "SQL_CREATE_TABLE",
                       "SQL_CREATE_TABLES",
                       "SQL_DELETE_TABLE",
                       "SQL_REMOVE_TABLE_PARTITIONS",
                       "SQL_DELETE_TABLE_PARTITION",
                       "SQL_CREATE_TABLE_INDEXES",
                       "SQL_DROP_TABLE_INDEXES",
                       "SQL_GET_TABLE_INDEXES",
                       "SQL_TABLE_ROW_STATS",
                       "INDEX",
                       "STATUS",
                       "STOP",
                       "DISPOSE",
                       "SERVICE_SUSPEND",
                       "SERVICE_RESUME",
                       "SERVICE_STATUS",
                       "SERVICE_REQUESTS",
                       "SERVICE_DRAIN",
                       "SERVICE_RECONFIG"},
                      _requestType)
            .required("worker", "The name of a worker.", _workerName)
            .option("cancel-delay-milliseconds",
                    "The number of milliseconds to wait before cancelling (if the number of not 0)"
                    " the earlier made request.",
                    _cancelDelayMilliseconds)
            .option("priority", "The priority level of a request", _priority)
            .flag("do-not-track", "Do not track requests by waiting before they finish.", _doNotTrackRequest)
            .flag("allow-duplicates",
                  "Allow requests which duplicate the previously made one. This applies"
                  " to requests which change the replica disposition at a worker, and only"
                  " for those requests which are still in the worker's queues.",
                  _allowDuplicates)
            .flag("do-not-save-replica",
                  "The flag which (if used) prevents the application from saving replica info in a database."
                  " This may significantly speed up the application in setups where the number of chunks is "
                  "on"
                  " a scale of one million, or exceeds it.",
                  _doNotSaveReplicaInfo)
            .flag("compute-check-sum",
                  " automatically compute and store in the database check/control sums for"
                  " all files of the found replica.",
                  _computeCheckSum);
    _configureParserCommandREPLICATE();
    _configureParserCommandDELETE();
    _configureParserCommandFIND();
    _configureParserCommandFINDALL();
    _configureParserCommandECHO();
    _configureParserCommandSQL();
    _configureParserCommandINDEX();
    _configureParserCommandSTATUS();
    _configureParserCommandSTOP();
    _configureParserCommandDISPOSE();
    _configureParserCommandSERVICE();
}

void ControllerApp::_configureParserCommandREPLICATE() {
    parser().command("REPLICATE")
            .description("Create a new replica of a chunk in a scope of database.")
            .required("source-worker", "The name of a source worker which has a replica to be cloned.",
                      _sourceWorkerName)
            .required("database", "The name of a database which has a chunk.", _databaseName)
            .required("chunk", "The number of a chunk.", _chunkNumber);
}

void ControllerApp::_configureParserCommandDELETE() {
    parser().command("DELETE")
            .description("Delete an existing replica of a chunk in a scope of database.")
            .required("database", "The name of a database which has a chunk.", _databaseName)
            .required("chunk", "The number of a chunk.", _chunkNumber);
}

void ControllerApp::_configureParserCommandFIND() {
    parser().command("FIND")
            .description("Find info on an existing replica of a chunk in a scope of database.")
            .required("database", "The name of a database which has a chunk.", _databaseName)
            .required("chunk", "The number of a chunk.", _chunkNumber);
}

void ControllerApp::_configureParserCommandFINDALL() {
    parser().command("FIND_ALL")
            .description("Find info on all replicas in a scope of database.")
            .required("database", "The name of a database which has chunks.", _databaseName);
}

void ControllerApp::_configureParserCommandECHO() {
    parser().command("ECHO")
            .description(
                    "Probe a worker service by sending a data string to be echoed back after"
                    " an optional delay introduced by the worker.")
            .required("data", "The data string to be sent to a worker with the request.", _echoData)
            .optional("delay",
                      "The optional delay (milliseconds) to be made by a worker before replying"
                      " to requests. If a value of the parameter is set to 0 then the request will be"
                      " answered immediately upon its reception by the worker.",
                      _echoDelayMilliseconds);
}

void ControllerApp::_configureParserCommandSQL() {
    parser().command("SQL_ALTER_TABLES")
            .description(
                    "Ask a worker service to execute the 'ALTER TABLE <table> ...' query against"
                    " select tables of a database, get a result set (if any) back and print it as a table.")
            .required("database", "The name of an existing database where the tables are residing.",
                      _sqlDatabase)
            .required("table", "The name of an existing table to be affected by the operation.", _sqlTable)
            .required("alter-spec",
                      "A specification of the change following 'ALTER TABLE <table> ...' to be executed"
                      " against each select table of teh requested database by a worker.",
                      _sqlAlterSpec);

    parser().command("SQL_QUERY")
            .description(
                    "Ask a worker service to execute a query against its database, get a result"
                    " set (if any) back and print it as a table")
            .required("query", "The query to be executed by a worker against its database.", _sqlQuery)
            .required("user", "The name of a user for establishing a connection with the worker's database.",
                      _sqlUser)
            .required("password",
                      "A password which is used along with the user name for establishing a connection"
                      " with the worker's database.",
                      _sqlPassword)
            .option("max-rows",
                    "The optional cap on a number of rows to be extracted by a worker from a result"
                    " set. If a value of the parameter is set to 0 then no explicit limit will be"
                    " be enforced.",
                    _sqlMaxRows)
            .option("tables-page-size",
                    "The number of rows in the table of a query result set (0 means no pages).",
                    _sqlPageSize);

    parser().command("SQL_CREATE_DATABASE")
            .required("database", "The name of a database to be created.", _sqlDatabase);

    parser().command("SQL_DELETE_DATABASE")
            .required("database", "The name of a database to be deleted.", _sqlDatabase);

    parser().command("SQL_ENABLE_DATABASE")
            .required("database", "The name of a database to be enabled at Qserv workers.", _sqlDatabase);

    parser().command("SQL_DISABLE_DATABASE")
            .required("database", "The name of a database to be disable at Qserv workers.", _sqlDatabase);

    parser().command("SQL_GRANT_ACCESS")
            .required("database", "The name of a database to be accessed.", _sqlDatabase)
            .required("user", "The name of a user to be affected by the operation.", _sqlUser);

    parser().command("SQL_CREATE_TABLE")
            .required("database", "The name of an existing database where the table will be created.",
                      _sqlDatabase)
            .required("table", "The name of a table to be created.", _sqlTable)
            .required("engine", "The name of a MySQL engine for the new table", _sqlEngine)
            .required("schema-file",
                      "The name of a file where column definitions of the table schema will be"
                      " read from. If symbol '-' is passed instead of the file name then column"
                      " definitions will be read from the Standard Input File. The file is required"
                      " to have the following format: <column-name> <type>",
                      _sqlSchemaFile)
            .option("partition-by-column",
                    "The name of a column which is used for creating the table based on"
                    " the MySQL partitioning mechanism,",
                    _sqlPartitionByColumn);

    parser().command("SQL_CREATE_TABLES")
            .required("database", "The name of an existing database where the table will be created.",
                      _sqlDatabase)
            .required("table", "The name of a table to be created.", _sqlTable)
            .required("engine", "The name of a MySQL engine for the new table", _sqlEngine)
            .required("schema-file",
                      "The name of a file where column definitions of the table schema will be"
                      " read from. If symbol '-' is passed instead of the file name then column"
                      " definitions will be read from the Standard Input File. The file is required"
                      " to have the following format: <column-name> <type>",
                      _sqlSchemaFile)
            .option("partition-by-column",
                    "The name of a column which is used for creating the table based on"
                    " the MySQL partitioning mechanism,",
                    _sqlPartitionByColumn);

    parser().command("SQL_DELETE_TABLE")
            .required("database", "The name of an existing database where the table is residing.",
                      _sqlDatabase)
            .required("table", "The name of an existing table to be deleted.", _sqlTable);

    parser().command("SQL_REMOVE_TABLE_PARTITIONS")
            .required("database", "The name of an existing database where the table is residing.",
                      _sqlDatabase)
            .required("table", "The name of an existing table to be affected by the operation.", _sqlTable);

    parser().command("SQL_DELETE_TABLE_PARTITION")
            .required("database", "The name of an existing database where the table is residing.",
                      _sqlDatabase)
            .required("table", "The name of an existing table to be affected by the operation.", _sqlTable)
            .required("transaction",
                      "An identifier of a super-transaction corresponding to a partition"
                      " to be dropped from the table. The transaction must exist, and it"
                      " should be in the ABORTED state.",
                      _transactionId);

    parser().command("SQL_CREATE_TABLE_INDEXES")
            .required("database", "The name of an existing database where the table is residing.",
                      _sqlDatabase)
            .required("table", "The name of an existing table to be affected by the operation.", _sqlTable)
            .required("name", "The name of an index to be created.", _sqlIndexName)
            .required("type-specification", "The type specification of an index.", _sqlIndexSpecStr,
                      {"DEFAULT", "UNIQUE", "FULLTEXT", "SPATIAL"})
            .required("columns-file", "The name of a file where to read definitions of the index's columns.",
                      _sqlIndexColumnsFile)
            .optional("comment", "The optional comment explaining an index.", _sqlIndexComment);

    parser().command("SQL_DROP_TABLE_INDEXES")
            .required("database", "The name of an existing database where the table is residing.",
                      _sqlDatabase)
            .required("table", "The name of an existing table to be affected by the operation.", _sqlTable)
            .required("name", "The name of an index to be dropped.", _sqlIndexName);

    parser().command("SQL_GET_TABLE_INDEXES")
            .required("database", "The name of an existing database where the table is residing.",
                      _sqlDatabase)
            .required("table", "The name of an existing table to be affected by the operation.", _sqlTable);

    parser().command("SQL_TABLE_ROW_STATS")
            .required("database", "The name of an existing database where the table is residing.",
                      _sqlDatabase)
            .required("table", "The base name of an existing table to be affected by the operation.",
                      _sqlTable)
            .option("chunk",
                    "The chunk number if this is the partitioned table. The parameter is ignored"
                    " for the regular tables.",
                    _chunkNumber)
            .flag("overlap", "The flag that defines a type of a table (partitioned tables only).",
                  _isOverlap);
}

void ControllerApp::_configureParserCommandINDEX() {
    parser().command("INDEX")
            .required("database", "The name of an existing database where the table is residing.",
                      _sqlDatabase)
            .required("table", "The name of an existing 'director' table to be affected by the operation.",
                      _sqlTable)
            .required("chunk", "The chunk number.", _chunkNumber)
            .option("transaction",
                    "An identifier of a super-transaction corresponding to a MySQL partition of the"
                    " 'director' table. If the option isn't used then the complete content of"
                    " the table will be scanned, and the scan won't include the super-transaction"
                    " column 'qserv_trans_id'.",
                    _transactionId)
            .flag("print-director-index",
                  "The flag that (if set to 'true') will result in printing the index onto"
                  " the standard output stream. Otherwise only the number of bytes will be printed.",
                  _printDirectorIndexData);
}

void ControllerApp::_configureParserCommandSTATUS() {
    parser().command("STATUS")
            .description("Ask a worker to return a status of a request.")
            .required("affected-request",
                      "The type of a request affected by the operation. Supported types:"
                      " REPLICATE, DELETE, FIND, FIND_ALL, ECHO, SQL_ALTER_TABLES, SQL_QUERY, "
                      "SQL_CREATE_DATABASE"
                      " SQL_DELETE_DATABASE, SQL_ENABLE_DATABASE, SQL_DISABLE_DATABASE"
                      " SQL_GRANT_ACCESS"
                      " SQL_CREATE_TABLE, SQL_CREATE_TABLES, SQL_DELETE_TABLE, SQL_REMOVE_TABLE_PARTITIONS,"
                      " SQL_DELETE_TABLE_PARTITION,"
                      " SQL_CREATE_TABLE_INDEXES, SQL_DROP_TABLE_INDEXES, SQL_GET_TABLE_INDEXES"
                      " INDEX",
                      _affectedRequest,
                      {"REPLICATE",
                       "DELETE",
                       "FIND",
                       "FIND_ALL",
                       "ECHO",
                       "SQL_ALTER_TABLES",
                       "SQL_QUERY",
                       "SQL_CREATE_DATABASE",
                       "SQL_DELETE_DATABASE",
                       "SQL_ENABLE_DATABASE",
                       "SQL_DISABLE_DATABASE",
                       "SQL_GRANT_ACCESS",
                       "SQL_CREATE_TABLE",
                       "SQL_CREATE_TABLES",
                       "SQL_DELETE_TABLE",
                       "SQL_REMOVE_TABLE_PARTITIONS",
                       "SQL_DELETE_TABLE_PARTITION",
                       "SQL_CREATE_TABLE_INDEXES",
                       "SQL_DROP_TABLE_INDEXES",
                       "SQL_GET_TABLE_INDEXES",
                       "INDEX"})
            .required("id", "A valid identifier of a request to be probed.", _affectedRequestId);
}

void ControllerApp::_configureParserCommandSTOP() {
    parser().command("STOP")
            .description("Ask a worker to stop an on-going request of the given type.")
            .required("affected-request",
                      "The type of a request affected by the operation. Supported types:"
                      " REPLICATE, DELETE, FIND, FIND_ALL, ECHO, SQL_ALTER_TABLES, SQL_QUERY, "
                      "SQL_CREATE_DATABASE"
                      " SQL_DELETE_DATABASE, SQL_ENABLE_DATABASE, SQL_DISABLE_DATABASE"
                      " SQL_GRANT_ACCESS"
                      " SQL_CREATE_TABLE, SQL_CREATE_TABLES, SQL_DELETE_TABLE, SQL_REMOVE_TABLE_PARTITIONS,"
                      " SQL_DELETE_TABLE_PARTITION,"
                      " SQL_CREATE_TABLE_INDEXES, SQL_DROP_TABLE_INDEXES, SQL_GET_TABLE_INDEXES"
                      " INDEX",
                      _affectedRequest,
                      {"REPLICATE",
                       "DELETE",
                       "FIND",
                       "FIND_ALL",
                       "ECHO",
                       "SQL_ALTER_TABLES",
                       "SQL_QUERY",
                       "SQL_CREATE_DATABASE",
                       "SQL_DELETE_DATABASE",
                       "SQL_ENABLE_DATABASE",
                       "SQL_DISABLE_DATABASE",
                       "SQL_GRANT_ACCESS",
                       "SQL_CREATE_TABLE",
                       "SQL_CREATE_TABLES",
                       "SQL_DELETE_TABLE",
                       "SQL_REMOVE_TABLE_PARTITIONS",
                       "SQL_DELETE_TABLE_PARTITION",
                       "SQL_CREATE_TABLE_INDEXES",
                       "SQL_DROP_TABLE_INDEXES",
                       "SQL_GET_TABLE_INDEXES",
                       "INDEX"})
            .required("id", "A valid identifier of a request to be stopped.", _affectedRequestId);
}

void ControllerApp::_configureParserCommandDISPOSE() {
    parser().command("DISPOSE")
            .description(
                    "Tell a worker to garbage collect the request. If the request is"
                    " still being processed then it will be stopped before being disposed.")
            .required("id", "A unique identifier of a request to be disposed.", _affectedRequestId);
}

void ControllerApp::_configureParserCommandSERVICE() {
    parser().command("SERVICE_SUSPEND")
            .description(
                    "Suspend the worker service. All ongoing requests will be cancelled and put"
                    " back into the input queue as if they had never been attempted."
                    " The service will be still accepting new requests which will be landing"
                    " in the input queue.");

    parser().command("SERVICE_RESUME").description("Resume the worker service");

    parser().command("SERVICE_STATUS")
            .description(
                    "Return a general status of the worker service. This will also include"
                    " request counters for the service's queues.");

    parser().command("SERVICE_REQUESTS")
            .description(
                    "Return the detailed status of the worker service. This will include"
                    " both request counters for the service's queues as well as an info on each"
                    " request known to the worker.");

    parser().command("SERVICE_DRAIN")
            .description(
                    "Drain all requests by stopping cancelling all ongoing requests"
                    " and emptying all queues.");

    parser().command("SERVICE_RECONFIG")
            .description(
                    "Reload worker's Configuration. Requests known to a worker won't be affected"
                    " by the operation.");
}

int ControllerApp::runImpl() {
    string const context = "ControllerApp::" + string(__func__) + " ";
    auto const controller = Controller::create(serviceProvider());
    Request::Ptr request;

    if ("REPLICATE" == _requestType) {
        request = controller->replicate(
                _workerName, _sourceWorkerName, _databaseName, _chunkNumber,
                [](Request::Ptr const& request_) { request_->print(); }, _priority, !_doNotTrackRequest,
                _allowDuplicates);

    } else if ("DELETE" == _requestType) {
        request = controller->deleteReplica(_workerName, _databaseName, _chunkNumber, Request::defaultPrinter,
                                            _priority, !_doNotTrackRequest, _allowDuplicates);

    } else if ("FIND" == _requestType) {
        request = controller->findReplica(_workerName, _databaseName, _chunkNumber, Request::defaultPrinter,
                                          _priority, _computeCheckSum, !_doNotTrackRequest);

    } else if ("FIND_ALL" == _requestType) {
        request = controller->findAllReplicas(_workerName, _databaseName, !_doNotSaveReplicaInfo,
                                              Request::defaultPrinter, _priority, !_doNotTrackRequest);

    } else if ("ECHO" == _requestType) {
        request = controller->echo(_workerName, _echoData, _echoDelayMilliseconds, Request::defaultPrinter,
                                   _priority, !_doNotTrackRequest);

    } else if ("INDEX" == _requestType) {
        bool const hasTransactions = _transactionId != numeric_limits<TransactionId>::max();
        request = controller->directorIndex(
                _workerName, _sqlDatabase, _sqlTable, _chunkNumber, hasTransactions, _transactionId,
                [&](DirectorIndexRequest::Ptr const& request_) {
                    Request::defaultPrinter(request_);
                    auto const& responseData = request_->responseData();
                    if (request_->extendedState() != Request::SUCCESS) {
                        if (!responseData.error.empty()) {
                            cerr << "A error reported by the worker: " << responseData.error << endl;
                        }
                        return;
                    }
                    if (_printDirectorIndexData) {
                        if (ifstream f(responseData.fileName); f.is_open()) {
                            cout << f.rdbuf();
                        } else {
                            cerr << "Failed to open the file: " << responseData.fileName << endl;
                        }
                    } else {
                        cout << "fileSizeBytes: " << responseData.fileSizeBytes << endl;
                    }
                },
                _priority, !_doNotTrackRequest);

    } else if ("SQL_ALTER_TABLES" == _requestType) {
        vector<string> const tables = {_sqlTable};
        request = controller->sqlAlterTables(_workerName, _sqlDatabase, tables, _sqlAlterSpec,
                                             SqlRequest::extendedPrinter, _priority, !_doNotTrackRequest);

    } else if ("SQL_QUERY" == _requestType) {
        request = controller->sqlQuery(_workerName, _sqlQuery, _sqlUser, _sqlPassword, _sqlMaxRows,
                                       SqlRequest::extendedPrinter, _priority, !_doNotTrackRequest);

    } else if ("SQL_CREATE_DATABASE" == _requestType) {
        request = controller->sqlCreateDb(_workerName, _sqlDatabase, SqlRequest::extendedPrinter, _priority,
                                          !_doNotTrackRequest);

    } else if ("SQL_DELETE_DATABASE" == _requestType) {
        request = controller->sqlDeleteDb(_workerName, _sqlDatabase, SqlRequest::extendedPrinter, _priority,
                                          !_doNotTrackRequest);

    } else if ("SQL_ENABLE_DATABASE" == _requestType) {
        request = controller->sqlEnableDb(_workerName, _sqlDatabase, SqlRequest::extendedPrinter, _priority,
                                          !_doNotTrackRequest);

    } else if ("SQL_DISABLE_DATABASE" == _requestType) {
        request = controller->sqlDisableDb(_workerName, _sqlDatabase, SqlRequest::extendedPrinter, _priority,
                                           !_doNotTrackRequest);

    } else if ("SQL_GRANT_ACCESS" == _requestType) {
        request = controller->sqlGrantAccess(_workerName, _sqlDatabase, _sqlUser, SqlRequest::extendedPrinter,
                                             _priority, !_doNotTrackRequest);

    } else if ("SQL_CREATE_TABLE" == _requestType) {
        request = controller->sqlCreateTable(_workerName, _sqlDatabase, _sqlTable, _sqlEngine,
                                             _sqlPartitionByColumn,
                                             SqlSchemaUtils::readFromTextFile(_sqlSchemaFile),
                                             SqlRequest::extendedPrinter, _priority, !_doNotTrackRequest);

    } else if ("SQL_CREATE_TABLES" == _requestType) {
        vector<string> const tables = {_sqlTable};
        request = controller->sqlCreateTables(_workerName, _sqlDatabase, tables, _sqlEngine,
                                              _sqlPartitionByColumn,
                                              SqlSchemaUtils::readFromTextFile(_sqlSchemaFile),
                                              SqlRequest::extendedPrinter, _priority, !_doNotTrackRequest);

    } else if ("SQL_DELETE_TABLE" == _requestType) {
        vector<string> const tables = {_sqlTable};
        request = controller->sqlDeleteTable(_workerName, _sqlDatabase, tables, SqlRequest::extendedPrinter,
                                             _priority, !_doNotTrackRequest);

    } else if ("SQL_REMOVE_TABLE_PARTITIONS" == _requestType) {
        vector<string> const tables = {_sqlTable};
        request = controller->sqlRemoveTablePartitions(_workerName, _sqlDatabase, tables,
                                                       SqlRequest::extendedPrinter, _priority,
                                                       !_doNotTrackRequest);

    } else if ("SQL_DELETE_TABLE_PARTITION" == _requestType) {
        vector<string> const tables = {_sqlTable};
        request = controller->sqlDeleteTablePartition(_workerName, _sqlDatabase, tables, _transactionId,
                                                      SqlRequest::extendedPrinter, _priority,
                                                      !_doNotTrackRequest);

    } else if ("SQL_CREATE_TABLE_INDEXES" == _requestType) {
        vector<string> const tables = {_sqlTable};
        request = controller->sqlCreateTableIndexes(
                _workerName, _sqlDatabase, tables, SqlRequestParams::IndexSpec(_sqlIndexSpecStr),
                _sqlIndexName, _sqlIndexComment,
                SqlSchemaUtils::readIndexSpecFromTextFile(_sqlIndexColumnsFile), SqlRequest::extendedPrinter,
                _priority, !_doNotTrackRequest);

    } else if ("SQL_DROP_TABLE_INDEXES" == _requestType) {
        vector<string> const tables = {_sqlTable};
        request =
                controller->sqlDropTableIndexes(_workerName, _sqlDatabase, tables, _sqlIndexName,
                                                SqlRequest::extendedPrinter, _priority, !_doNotTrackRequest);

    } else if ("SQL_GET_TABLE_INDEXES" == _requestType) {
        vector<string> const tables = {_sqlTable};
        request = controller->sqlGetTableIndexes(_workerName, _sqlDatabase, tables,
                                                 SqlRequest::extendedPrinter, _priority, !_doNotTrackRequest);

    } else if ("SQL_TABLE_ROW_STATS" == _requestType) {
        auto const databaseInfo = controller->serviceProvider()->config()->databaseInfo(_sqlDatabase);
        bool const isPartitioned = databaseInfo.findTable(_sqlTable).isPartitioned;
        vector<string> const tables = {
                isPartitioned ? ChunkedTable(_sqlTable, _chunkNumber, _isOverlap).name() : _sqlTable};
        request = controller->sqlRowStats(_workerName, _sqlDatabase, tables, SqlRequest::extendedPrinter,
                                          _priority, !_doNotTrackRequest);

    } else if ("STATUS" == _requestType) {
        request = _launchStatusRequest(controller);

    } else if ("STOP" == _requestType) {
        request = _launchStatusRequest(controller);

    } else if ("DISPOSE" == _requestType) {
        vector<string> const targetIds = {_affectedRequestId};
        request = controller->dispose(_workerName, targetIds, Request::defaultPrinter);

    } else if ("SERVICE_SUSPEND" == _requestType) {
        request =
                controller->suspendWorkerService(_workerName, ServiceManagementRequestBase::extendedPrinter);

    } else if ("SERVICE_RESUME" == _requestType) {
        request = controller->resumeWorkerService(_workerName, ServiceManagementRequestBase::extendedPrinter);

    } else if ("SERVICE_STATUS" == _requestType) {
        request =
                controller->statusOfWorkerService(_workerName, ServiceManagementRequestBase::extendedPrinter);

    } else if ("SERVICE_REQUESTS" == _requestType) {
        request = controller->requestsOfWorkerService(_workerName,
                                                      ServiceManagementRequestBase::extendedPrinter);

    } else if ("SERVICE_DRAIN" == _requestType) {
        request = controller->drainWorkerService(_workerName, ServiceManagementRequestBase::extendedPrinter);

    } else if ("SERVICE_RECONFIG" == _requestType) {
        request =
                controller->reconfigWorkerService(_workerName, ServiceManagementRequestBase::extendedPrinter);

    } else {
        throw logic_error(context + "unsupported request: " + _affectedRequest);
    }

    // Cancel the last request if required, or just block the thread waiting
    // before it will finish.

    if (_cancelDelayMilliseconds != 0) {
        util::BlockPost blockPost(_cancelDelayMilliseconds, _cancelDelayMilliseconds + 1);
        blockPost.wait();
        request->cancel();
    } else {
        request->wait();
    }
    return 0;
}

Request::Ptr ControllerApp::_launchStatusRequest(Controller::Ptr const& controller) const {
    ::ManagementRequestLauncher l(controller, _workerName, _affectedRequestId, _priority, _doNotTrackRequest);

    if ("REPLICATE" == _affectedRequest) return l.status<StatusReplicationRequest>();
    if ("DELETE" == _affectedRequest) return l.status<StatusDeleteRequest>();
    if ("FIND" == _affectedRequest) return l.status<StatusFindRequest>();
    if ("FIND_ALL" == _affectedRequest) return l.status<StatusFindAllRequest>();
    if ("ECHO" == _affectedRequest) return l.status<StatusEchoRequest>();
    if ("INDEX" == _affectedRequest) return l.status<StatusDirectorIndexRequest>();
    if ("SQL_ALTER_TABLES" == _affectedRequest) return l.status<StatusSqlAlterTablesRequest>();
    if ("SQL_QUERY" == _affectedRequest) return l.status<StatusSqlQueryRequest>();
    if ("SQL_CREATE_DATABASE" == _affectedRequest) return l.status<StatusSqlCreateDbRequest>();
    if ("SQL_DELETE_DATABASE" == _affectedRequest) return l.status<StatusSqlDeleteDbRequest>();
    if ("SQL_ENABLE_DATABASE" == _affectedRequest) return l.status<StatusSqlEnableDbRequest>();
    if ("SQL_DISABLE_DATABASE" == _affectedRequest) return l.status<StatusSqlDisableDbRequest>();
    if ("SQL_GRANT_ACCESS" == _affectedRequest) return l.status<StatusSqlGrantAccessRequest>();
    if ("SQL_CREATE_TABLE" == _affectedRequest) return l.status<StatusSqlCreateTableRequest>();
    if ("SQL_CREATE_TABLES" == _affectedRequest) return l.status<StatusSqlCreateTablesRequest>();
    if ("SQL_CREATE_TABLE_INDEXES" == _affectedRequest) return l.status<StatusSqlCreateIndexesRequest>();
    if ("SQL_DELETE_TABLE" == _affectedRequest) return l.status<StatusSqlDeleteTableRequest>();
    if ("SQL_DROP_TABLE_INDEXES" == _affectedRequest) return l.status<StatusSqlDropIndexesRequest>();
    if ("SQL_GET_TABLE_INDEXES" == _affectedRequest) return l.status<StatusSqlGetIndexesRequest>();
    if ("SQL_REMOVE_TABLE_PARTITIONS" == _affectedRequest)
        return l.status<StatusSqlRemoveTablePartitionsRequest>();
    if ("SQL_DELETE_TABLE_PARTITION" == _affectedRequest)
        return l.status<StatusSqlDeleteTablePartitionRequest>();

    throw logic_error("ControllerApp::" + string(__func__) + "  unsupported request: " + _affectedRequest);
}

Request::Ptr ControllerApp::_launchStopRequest(Controller::Ptr const& controller) const {
    ::ManagementRequestLauncher l(controller, _workerName, _affectedRequestId, _priority, _doNotTrackRequest);

    if ("REPLICATE" == _affectedRequest) return l.stop<StopReplicationRequest>();
    if ("DELETE" == _affectedRequest) return l.stop<StopDeleteRequest>();
    if ("FIND" == _affectedRequest) return l.stop<StopFindRequest>();
    if ("FIND_ALL" == _affectedRequest) return l.stop<StopFindAllRequest>();
    if ("ECHO" == _affectedRequest) return l.stop<StopEchoRequest>();
    if ("INDEX" == _affectedRequest) return l.stop<StopDirectorIndexRequest>();
    if ("SQL_ALTER_TABLES" == _affectedRequest) return l.stop<StopSqlAlterTablesRequest>();
    if ("SQL_QUERY" == _affectedRequest) return l.stop<StopSqlQueryRequest>();
    if ("SQL_CREATE_DATABASE" == _affectedRequest) return l.stop<StopSqlCreateDbRequest>();
    if ("SQL_DELETE_DATABASE" == _affectedRequest) return l.stop<StopSqlDeleteDbRequest>();
    if ("SQL_ENABLE_DATABASE" == _affectedRequest) return l.stop<StopSqlEnableDbRequest>();
    if ("SQL_DISABLE_DATABASE" == _affectedRequest) return l.stop<StopSqlDisableDbRequest>();
    if ("SQL_GRANT_ACCESS" == _affectedRequest) return l.stop<StopSqlGrantAccessRequest>();
    if ("SQL_CREATE_TABLE" == _affectedRequest) return l.stop<StopSqlCreateTableRequest>();
    if ("SQL_CREATE_TABLES" == _affectedRequest) return l.stop<StopSqlCreateTablesRequest>();
    if ("SQL_CREATE_TABLE_INDEXES" == _affectedRequest) return l.stop<StopSqlCreateIndexesRequest>();
    if ("SQL_DELETE_TABLE" == _affectedRequest) return l.stop<StopSqlDeleteTableRequest>();
    if ("SQL_DROP_TABLE_INDEXES" == _affectedRequest) return l.stop<StopSqlDropIndexesRequest>();
    if ("SQL_GET_TABLE_INDEXES" == _affectedRequest) return l.stop<StopSqlGetIndexesRequest>();
    if ("SQL_REMOVE_TABLE_PARTITIONS" == _affectedRequest)
        return l.stop<StopSqlRemoveTablePartitionsRequest>();
    if ("SQL_DELETE_TABLE_PARTITION" == _affectedRequest) return l.stop<StopSqlDeleteTablePartitionRequest>();

    throw logic_error("ControllerApp::" + string(__func__) + "  unsupported request: " + _affectedRequest);
}

}  // namespace lsst::qserv::replica
