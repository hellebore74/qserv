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
#include "replica/IngestServerConnection.h"

// System headers
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <functional>
#include <thread>
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "global/constants.h"
#include "replica/ChunkedTable.h"
#include "replica/Configuration.h"
#include "replica/ConfigurationIFace.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/FileUtils.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;
namespace fs = boost::filesystem;
using namespace lsst::qserv::replica;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestServerConnection");

/// The context for diagnostic & debug printouts
string const context = "INGEST-SERVER-CONNECTION  ";

bool isErrorCode(boost::system::error_code const& ec,
                 string const& scope) {
    if (ec.value() != 0) {
        if (ec == boost::asio::error::eof) {
            LOGS(_log, LOG_LVL_DEBUG, context << scope << "  ** closed **");
        } else {
            LOGS(_log, LOG_LVL_ERROR, context << scope << "  ** failed: " << ec << " **");
        }
        return true;
    }
    return false;
}


bool readIntoBuffer(boost::asio::ip::tcp::socket& socket,
                    shared_ptr<ProtocolBuffer> const& ptr,
                    size_t bytes) {
    // Make sure the buffer has enough space to accommodate the data
    // of the message. Note, this call may throw an exception which is
    // supposed to be caught by the method's caller.
    ptr->resize(bytes);

    boost::system::error_code ec;
    boost::asio::read(
        socket,
        boost::asio::buffer(
            ptr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        ec
    );
    return not ::isErrorCode(ec, __func__);
}


template <class T>
bool readMessage(boost::asio::ip::tcp::socket& socket,
                 shared_ptr<ProtocolBuffer> const& ptr,
                 size_t bytes,
                 T& message) {
    try {
        if (readIntoBuffer(socket, ptr, bytes)) {
            ptr->parse(message, bytes);
            return true;
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << __func__ << ex.what());
    }
    return false;
}
}   // namespace

namespace lsst {
namespace qserv {
namespace replica {

size_t IngestServerConnection::networkBufSizeBytes = 1024 * 1024;


IngestServerConnection::Ptr IngestServerConnection::create(
        ServiceProvider::Ptr const& serviceProvider,
        string const& workerName,
        string const& authKey,
        boost::asio::io_service& io_service) {
    return IngestServerConnection::Ptr(
        new IngestServerConnection(
            serviceProvider,
            workerName,
            authKey,
            io_service));
}


IngestServerConnection::IngestServerConnection(ServiceProvider::Ptr const& serviceProvider,
                                               string const& workerName,
                                               string const& authKey,
                                               boost::asio::io_service& io_service)
    :   _serviceProvider(serviceProvider),
        _workerName(workerName),
        _authKey(authKey),
        _workerInfo(serviceProvider->config()->workerInfo(workerName)),
        _socket(io_service),
        _bufferPtr(make_shared<ProtocolBuffer>(
            serviceProvider->config()->requestBufferSizeBytes()
        )) {
}


IngestServerConnection::~IngestServerConnection() {
    _closeFile();
}


void IngestServerConnection::beginProtocol() {
    _receiveHandshake();
}


void IngestServerConnection::_receiveHandshake() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), bytes),
        boost::asio::transfer_at_least(bytes),
        bind(&IngestServerConnection::_handshakeReceived, shared_from_this(), _1, _2)
    );
}


void IngestServerConnection::_handshakeReceived(boost::system::error_code const& ec,
                                                size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) return;

    // Now read the body of the request

    ProtocolIngestHandshakeRequest request;
    if (not ::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) return;

    _transactionId   = request.transaction_id();
    _table           = request.table();
    _chunk           = request.chunk();
    _isOverlap       = request.is_overlap();
    _columnSeparator = request.column_separator() == ProtocolIngestHandshakeRequest::COMMA ? ',' : '\t';

    // Check if the client is authorized for the operation

    if (request.auth_key() != _authKey) {
        _failed("not authorized");
        return;
    }

    // Check if a context of the request is valid
    
    try {
        auto transactionInfo = _serviceProvider->databaseServices()->transaction(_transactionId);
        if (transactionInfo.state != TransactionInfo::STARTED) {
            _failed("transaction is not active");
            return;
        }

        // Get and validate a status of the database and the table

        _databaseInfo = _serviceProvider->config()->databaseInfo(transactionInfo.database);
        if (_databaseInfo.isPublished) {
            throw invalid_argument("database '" + _databaseInfo.name + "' is already PUBLISHED");
        }
        _isPartitioned = _databaseInfo.partitionedTables.end() != find(
                _databaseInfo.partitionedTables.begin(),
                _databaseInfo.partitionedTables.end(),
                _table);
        if (not _isPartitioned) {
            if (_databaseInfo.regularTables.end() == find(
                    _databaseInfo.regularTables.begin(),
                    _databaseInfo.regularTables.end(),
                    _table)) {
                throw invalid_argument(
                        "no such table '" + _table + "' in a scope of database '" +
                        _databaseInfo.name + "'");
            }
        }

        // The next test is for the partitioned tables, and it's meant to check if
        // the chunk number is valid and it's allocated to this worker. The test will
        // also ensure that the database is in the UNPUBLISHED state.

        if (_isPartitioned) {

            vector<ReplicaInfo> replicas;       // Chunk replicas at the current worker found
                                                // among the unpublished databases only
            bool const allDatabases = false;
            bool const isPublished = false;

            _serviceProvider->databaseServices()->findWorkerReplicas(
                replicas,
                _chunk,
                _workerName,
                _databaseInfo.family,
                allDatabases,
                isPublished
            );
            if (replicas.cend() == find_if(replicas.cbegin(), replicas.cend(),
                    [&](ReplicaInfo const& replica) {
                        return replica.database() == _databaseInfo.name;
                    })) {
                throw invalid_argument(
                        "chunk " + to_string(_chunk) + " of the UNPUBLISHED database '" +
                        _databaseInfo.name + "' is not allocated to worker '" + _workerName + "'");
            }
        }
                
    } catch (DatabaseServicesNotFound const& ex) {
        _failed("invalid transaction identifier");
        return;
    } catch (invalid_argument const& ex) {
        _failed(ex.what());
        return;
    }
    
    // Create a temporary file. The algorithm will make several attempts to create
    // a temporary file name
    try {
        _fileName = FileUtils::createTemporaryFile(
            _workerInfo.loaderTmpDir,
            _databaseInfo.name + "-" + _table + "-" + to_string(_chunk) + "-" + to_string(_transactionId),
            "-%%%%-%%%%-%%%%-%%%%",
            ".csv"
        );
    } catch (exception const& ex) {
        _failed("failed to generate a unique name for a temporary file, ex: " + string(ex.what()));
        return;
    }

    _file.open(_fileName, ofstream::out);
    if (not _file.is_open()) {
        _failed("failed to create a temporary file: " + _fileName);
        return;
    }

    // Ask a client to send 1 row to begin with. An optimal number of rows will be
    // calculated based later upon a completion of that row and measuring its size.
    _sendReadyToReadData(1);
}


void IngestServerConnection::_sendResponse() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    boost::asio::async_write(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), _bufferPtr->size()),
        bind(&IngestServerConnection::_responseSent, shared_from_this(), _1, _2)
    );
}


void IngestServerConnection::_responseSent(boost::system::error_code const& ec,
                                           size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) _closeFile();
    if (not _file.is_open()) return;

    _receiveData();
}

void IngestServerConnection::_receiveData() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), bytes),
        boost::asio::transfer_at_least(bytes),
        bind(&IngestServerConnection::_dataReceived, shared_from_this(), _1, _2)
    );
}


void IngestServerConnection::_dataReceived(boost::system::error_code const& ec,
                                           size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) {
        _closeFile();
        return;
    }

    ProtocolIngestData request;
    if (not ::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) {
        _closeFile();
        return;
    }

    // Prepend each row with the transaction identifier and write it into the output file.
    // Compute the maximum length of the rows. It's value will be used on the next step
    // to advise a client on the most optimal number of rows to be sent with the next
    // batch (of rows).

    size_t rowSize = 0;
    for (int i = 0, num = request.rows_size(); i < num; ++i) {
        auto&& row = request.rows(i);
        rowSize = max(rowSize, row.size());
        _file << _transactionId << _columnSeparator << row << "\n";
        ++_totalNumRows;
    }

    ProtocolIngestResponse response;
    if (request.last()) {
        LOGS(_log, LOG_LVL_DEBUG, context << __func__ << "  _totalNumRows: " << _totalNumRows);
        
        // Make sure no unsaved rows were staying in memory before proceeding
        // to the loading phase.
        _file.flush();

        try {
            _loadDataIntoTable();
            _finished();
        } catch(exception const& ex) {
            string const error = string("data load failed: ") + ex.what();
            LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  " << error);
            _failed(error);
        }
    } else {

        size_t maxRows = 1;
        if (rowSize != 0) maxRows = max(maxRows, networkBufSizeBytes / rowSize);
        _sendReadyToReadData(maxRows);
    }
}


void IngestServerConnection::_reply(ProtocolIngestResponse::Status status,
                                    string const& msg,
                                    size_t maxRows) {
    ProtocolIngestResponse response;
    response.set_status(status);
    response.set_error(msg);
    response.set_max_rows(maxRows);

    _bufferPtr->resize();
    _bufferPtr->serialize(response);

    _sendResponse();
}


void IngestServerConnection::_loadDataIntoTable() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    // ATTENTION: the data loading method used in this implementation requires
    // that the MySQL server has (at least) the read-only access to files in
    // a folder in which the CSV file will be stored by this server. So, make
    // proper adjustments to a configuration of the Replication system.

    try {
        // The RAII connection handler automatically aborts the active transaction
        // should an exception be thrown within the block.
        database::mysql::ConnectionHandler h(
            database::mysql::Connection::open(
                database::mysql::ConnectionParams(
                    _workerInfo.dbHost,
                    _workerInfo.dbPort,
                    _workerInfo.dbUser,
                    _serviceProvider->config()->qservWorkerDatabasePassword(),
                    ""
                )
            )
        );

        string const sqlDatabase = h.conn->sqlId(_databaseInfo.name);
        string const sqlPartition = h.conn->sqlPartitionId(_transactionId);

        vector<string> tableMgtStatements;

        // Make sure no outstanding table locks exist from prior operations
        // on persistent database connections.
        tableMgtStatements.push_back("UNLOCK TABLES");

        string dataLoadStatement;

        if (_isPartitioned) {
            
            // Note, that the algorithm will create chunked tables for _ALL_ partitioned
            // tables (not just for the current one) to ensure they have representations
            // in all chunks touched by the ingest workflows. Missing representations would
            // cause Qserv to fail when processing queries involving these tables.

            for (auto&& table: _databaseInfo.partitionedTables) {

                // Chunked tables are created from the prototype table which is expected
                // to exist in the database before attempting data loading.

                bool const overlap = true;
                string const sqlProtoTable       = sqlDatabase + "." + h.conn->sqlId(table);
                string const sqlTable            = sqlDatabase + "." + h.conn->sqlId(ChunkedTable(table, _chunk, not overlap).name());
                string const sqlFullOverlapTable = sqlDatabase + "." + h.conn->sqlId(ChunkedTable(table, _chunk, overlap).name());

                string const tablesToBeCreated[] = {
                    sqlTable,
                    sqlFullOverlapTable,
                    sqlDatabase + "." + h.conn->sqlId(ChunkedTable(table, lsst::qserv::DUMMY_CHUNK, not overlap).name()),
                    sqlDatabase + "." + h.conn->sqlId(ChunkedTable(table, lsst::qserv::DUMMY_CHUNK, overlap).name())
                };
                for (auto&& table: tablesToBeCreated) {
                    tableMgtStatements.push_back(
                        "CREATE TABLE IF NOT EXISTS " + table + " LIKE " + sqlProtoTable
                    );
                    tableMgtStatements.push_back(
                        "ALTER TABLE " + table + " ADD PARTITION IF NOT EXISTS (PARTITION " + sqlPartition +
                            " VALUES IN (" + to_string(_transactionId) + "))"
                    );
                }

                // An additional step for the current request's table
                if (table == _table) {
                    dataLoadStatement =
                        "LOAD DATA INFILE " + h.conn->sqlValue(_fileName) +
                            " INTO TABLE " + (_isOverlap ? sqlFullOverlapTable : sqlTable) +
                            " FIELDS TERMINATED BY " + h.conn->sqlValue(string() + _columnSeparator);
                }
            }
        } else {

            // Regular tables are expected to exist in the database before
            // attempting data loading.

            string const sqlTable = sqlDatabase + "." + h.conn->sqlId(_table);

            tableMgtStatements.push_back(
                "ALTER TABLE " + sqlTable + " ADD PARTITION IF NOT EXISTS (PARTITION " + sqlPartition +
                    " VALUES IN (" + to_string(_transactionId) + "))"
            );
            dataLoadStatement =
                "LOAD DATA INFILE " + h.conn->sqlValue(_fileName) +
                    " INTO TABLE " + sqlTable +
                    " FIELDS TERMINATED BY " + h.conn->sqlValue(string() + _columnSeparator);
        }
        for (auto&& statement: tableMgtStatements) {
            LOGS(_log, LOG_LVL_DEBUG, context << __func__ << "  statement: " << statement);
        }
        LOGS(_log, LOG_LVL_DEBUG, context << __func__ << "  statement: " << dataLoadStatement);

        // Allow retries for the table management statements in case of deadlocks.
        // Deadlocks may happen when two or many threads are attempting to create
        // or modify partitioned tables, or at a presence of other threads loading
        // data into these tables.
        //
        // TODO: the experimental limit for the maximum number of retries may need
        //       to be made unlimited, or be limited by some configurable timeout.
        int const maxRetries = 1;
        int numRetries = 0;
        while (true) {
            try {
                h.conn->execute([&tableMgtStatements](decltype(h.conn) const& conn_) {
                    conn_->begin();
                    for (auto&& statement: tableMgtStatements) {
                        conn_->execute(statement);
                    }
                    conn_->commit();
                });
                break;
            } catch (database::mysql::LockDeadlock const& ex) {
                if (h.conn->inTransaction()) h.conn->rollback();
                if (numRetries < maxRetries) {
                    LOGS(_log, LOG_LVL_WARN, context << __func__ << "  exception: " << ex.what());
                    ++numRetries;
                } else {
                    LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  maximum number of retries "
                            << maxRetries << " for avoiding table management deadlocks has been reached."
                            << " Aborting the file loading operation.");
                    throw;
                }
            }
        }

        // Load table contribution
        if (dataLoadStatement.empty()) {
            throw std::runtime_error(context + string(__func__) + "  no data loading statement generated");
        }
        h.conn->execute([&dataLoadStatement](decltype(h.conn) const& conn_) {
            conn_->begin();
            conn_->execute(dataLoadStatement);
            conn_->commit();
        });

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  exception: " << ex.what());
        throw;
    }
}


void IngestServerConnection::_closeFile() {
    if (_file.is_open()) {
        _file.close();
        boost::system::error_code ec;
        fs::remove(_fileName, ec);
        if (ec.value() != 0) {
            LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  file removal failed: " << ec.message());
        }
    }
}

}}} // namespace lsst::qserv::replica
