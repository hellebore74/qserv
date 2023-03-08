// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#ifndef LSST_QSERV_CCONTROL_MERGINGHANDLER_H
#define LSST_QSERV_CCONTROL_MERGINGHANDLER_H

// System headers
#include <atomic>
#include <memory>
#include <mutex>
#include <set>

// Qserv headers
#include "qdisp/ResponseHandler.h"

// Forward decl
namespace lsst::qserv {
class MsgReceiver;
namespace proto {
struct WorkerResponse;
}
namespace rproc {
class InfileMerger;
}
}  // namespace lsst::qserv

namespace lsst::qserv::ccontrol {

/// MergingHandler is an implementation of a ResponseHandler that implements
/// czar-side knowledge of the worker's response protocol. It leverages XrdSsi's
/// API by pulling the exact number of bytes needed for the next logical
/// fragment instead of performing buffer size and offset
/// management. Fully-constructed protocol messages are then passed towards an
/// InfileMerger.
/// Do to the way the code works, MerginHandler is effectively single threaded.
/// The worker can only send the data for this job back over a single channel
/// and it can only send one transmit on that channel at a time.
class MergingHandler : public qdisp::ResponseHandler {
public:
    /// Possible MergingHandler message state
    enum class MsgState { HEADER_WAIT, RESULT_WAIT, RESULT_RECV, HEADER_ERR, RESULT_ERR };
    static const char* getStateStr(MsgState const& st);

    typedef std::shared_ptr<MergingHandler> Ptr;
    virtual ~MergingHandler();

    /// @param msgReceiver Message code receiver
    /// @param merger downstream merge acceptor
    /// @param tableName target table for incoming data
    MergingHandler(std::shared_ptr<MsgReceiver> msgReceiver, std::shared_ptr<rproc::InfileMerger> merger,
                   std::string const& tableName);

    /// Flush the retrieved buffer where bLen bytes were set. If last==true,
    /// then no more buffer() and flush() calls should occur.
    /// @return true if successful (no error)
    bool flush(int bLen, BufPtr const& bufPtr, bool& last, int& nextBufSize, int& resultRows) override;

    /// Signal an unrecoverable error condition. No further calls are expected.
    void errorFlush(std::string const& msg, int code) override;

    /// @return true if the receiver has completed its duties.
    bool finished() const override;

    bool reset() override;  ///< Reset the state that a request can be retried.

    /// Print a string representation of the receiver to an ostream
    std::ostream& print(std::ostream& os) const override;

    /// @return an error code and description
    Error getError() const override {
        std::lock_guard<std::mutex> lock(_errorMutex);
        return _error;
    }

    /// Prepare to scrub the results from jobId-attempt from the result table.
    void prepScrubResults(int jobId, int attempt) override;

private:
    void _initState();  ///< Prepare for first call to flush()
    bool _merge();      ///< Call Infile::merge to add the results to the result table.
    void _setError(int code, std::string const& msg);    ///< Set error code and string
    bool _setResult(BufPtr const& bufPtr, int blen);     ///< Extract the result from the protobuffer.
    bool _verifyResult(BufPtr const& bufPtr, int blen);  ///< Check the result against hash in the header.

    std::shared_ptr<MsgReceiver> _msgReceiver;           ///< Message code receiver
    std::shared_ptr<rproc::InfileMerger> _infileMerger;  ///< Merging delegate
    std::string _tableName;                              ///< Target table name
    Error _error;                                        ///< Error description
    mutable std::mutex _errorMutex;                      ///< Protect readers from partial updates
    MsgState _state;                                     ///< Received message state
    std::shared_ptr<proto::WorkerResponse> _response;    ///< protobufs msg buf
    bool _flushed{false};                                ///< flushed to InfileMerger?
    std::string _wName{"~"};                             ///< worker name
    std::mutex _setResultMtx;  //< Allow only one call to ParseFromArray at a time from _seResult.
    /// Set of jobIds added in this request. Using std::set to prevent duplicates when the same
    /// jobId has multiple merge calls.
    std::set<int> _jobIds;
};

}  // namespace lsst::qserv::ccontrol

#endif  // LSST_QSERV_CCONTROL_MERGINGHANDLER_H
