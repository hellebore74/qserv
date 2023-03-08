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
#include "wbase/FileChannelShared.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "proto/ProtoHeaderWrap.h"
#include "proto/worker.pb.h"
#include "wbase/Task.h"
#include "util/MultiError.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;
namespace proto = lsst::qserv::proto;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.FileChannelShared");
}  // namespace

namespace lsst::qserv::wbase {

FileChannelShared::Ptr FileChannelShared::create(shared_ptr<wbase::SendChannel> const& sendChannel,
                                                 shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                                                 shared_ptr<proto::TaskMsg> const& taskMsg) {
    return shared_ptr<FileChannelShared>(new FileChannelShared(sendChannel, transmitMgr, taskMsg));
}

FileChannelShared::FileChannelShared(shared_ptr<wbase::SendChannel> const& sendChannel,
                                     shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                                     shared_ptr<proto::TaskMsg> const& taskMsg)
        : ChannelShared(sendChannel, transmitMgr, taskMsg->czarid()) {}

FileChannelShared::~FileChannelShared() {
    if (!_fileName.empty() && _file.is_open()) {
        boost::system::error_code ec;
        fs::remove_all(fs::path(_fileName), ec);
        if (ec.value() != 0) {
            LOGS(_log, LOG_LVL_WARN,
                 "FileChannelShared::" << __func__ << " failed to remove the result file '" << _fileName
                                       << "', ec: " << ec << ".");
        }
    }
}

bool FileChannelShared::buildAndTransmitResult(MYSQL_RES* mResult, shared_ptr<Task> const& task,
                                               util::MultiError& multiErr, atomic<bool>& cancelled) {
    // Lock the transmit mutex until finished processing the result set.
    lock_guard<mutex> const tMtxLock(tMtx);
    bool hasMoreRows = true;
    while (hasMoreRows && !cancelled) {
        // Keep reading rows and converting those into messages while
        // there are still any left in the result set. If 'hasMoreRows' is set
        // then the internal Protobuf result message is full and it needs
        // to be emptied by calling 'prepTransmit' before reading the rest of
        // the result set.
        hasMoreRows = _processNextBatchOfRows(tMtxLock, mResult, task, multiErr);
        if (hasMoreRows) {
            // Can't be the last message as we still have more rows
            // to be extracted from the result set.
            bool const lastIn = false;
            if (!prepTransmit(tMtxLock, task, cancelled, lastIn)) {
                LOGS(_log, LOG_LVL_ERROR,
                     "FileChannelShared::" << __func__
                                           << " Could not package/transmit intermediate results.");
                return true;
            }
        } else {
            // If 'lastIn', this is the last transmit in a logical group of tasks
            // (which may have more than one member if sub-chunks are involved)
            // and it needs to be added.
            bool const lastIn = transmitTaskLast();
            if (lastIn && !prepTransmit(tMtxLock, task, cancelled, lastIn)) {
                LOGS(_log, LOG_LVL_ERROR,
                     "FileChannelShared::" << __func__ << " Could not package/transmit the final message.");
                return true;
            }
        }
    }
    return false;
}

bool FileChannelShared::prepTransmit(lock_guard<mutex> const& tMtxLock, shared_ptr<Task> const& task,
                                     bool cancelled, bool lastIn) {
    if (lastIn) {
        _file.flush();
        _file.close();
    }
    return ChannelShared::prepTransmit(tMtxLock, task, cancelled, lastIn);
}

bool FileChannelShared::_processNextBatchOfRows(lock_guard<mutex> const& tMtxLock, MYSQL_RES* mResult,
                                                shared_ptr<Task> const& task, util::MultiError& multiErr) {
    // Initialize transmitData, if needed.
    initTransmit(tMtxLock, *task);

    // tSize is set by fillRows.
    size_t tSize = 0;
    bool const hasMoreRows = !transmitData->fillRows(mResult, tSize);
    transmitData->buildDataMsg(*task, multiErr);
    LOGS(_log, LOG_LVL_TRACE,
         "FileChannelShared::" << __func__ << "() hasMoreRows=" << hasMoreRows << " " << task->getIdStr()
                               << " seq=" << task->getTSeq() << dumpTransmit(tMtxLock));

    _writeToFile(tMtxLock, task, transmitData->dataMsg());

    return hasMoreRows;
}

void FileChannelShared::_writeToFile(lock_guard<mutex> const& tMtxLock, shared_ptr<Task> const& task,
                                     string const& msg) {
    string const context = "FileChannelShared::" + string(__func__) + " ";
    if (!_file.is_open()) {
        _fileName = task->fileResourceName();
        _file.open(_fileName, ios::out | ios::trunc | ios::binary);
        if (!(_file.is_open() && _file.good())) {
            string const error = context + "failed to create the file '" + _fileName + "'.";
            LOGS(_log, LOG_LVL_ERROR, error);
            throw runtime_error(error);
        }
    }
    // Write 32-bit length of the subsequent message first before writing
    // the message itself.
    uint32_t const msgSizeBytes = msg.size();
    _file.write(reinterpret_cast<char const*>(&msgSizeBytes), sizeof msgSizeBytes);
    _file.write(msg.data(), msgSizeBytes);
    if (!(_file.is_open() && _file.good())) {
        string const error = context + "failed to write " + to_string(msg.size()) + " bytes into the file '" +
                             _fileName + "'.";
        LOGS(_log, LOG_LVL_ERROR, error);
        throw runtime_error(error);
    }
}

}  // namespace lsst::qserv::wbase
