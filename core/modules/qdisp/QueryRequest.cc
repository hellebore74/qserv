// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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

/**
  * @file
  *
  * @brief QueryRequest. XrdSsiRequest impl for czar query dispatch
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "qdisp/QdispPool.h"
#include "qdisp/QueryRequest.h"

// System headers
#include <cstddef>
#include <iostream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "czar/Czar.h"
#include "global/LogContext.h"
#include "proto/ProtoHeaderWrap.h"
#include "proto/ScanTableInfo.h"
#include "qdisp/JobStatus.h"
#include "qdisp/ResponseHandler.h"
#include "util/common.h"
#include "util/InstanceCount.h"
#include "util/Timer.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.QueryRequest");
}

namespace lsst {
namespace qserv {
namespace qdisp {


// Run action() when the system expects to have time to accept data.
class QueryRequest::AskForResponseDataCmd : public PriorityCommand {
public:
    typedef std::shared_ptr<AskForResponseDataCmd> Ptr;
    enum class State { STARTED0, DATAREADY1, DONE2 };
    AskForResponseDataCmd(QueryRequest::Ptr const& qr, JobQuery::Ptr const& jq, size_t bufferSize)
        : _qRequest(qr), _jQuery(jq), _qid(jq->getQueryId()), _jobid(jq->getIdInt()) {
        _bufPtr.reset(new std::vector<char>(bufferSize));
    }

    void action(util::CmdData *data) override {
        // If everything is ok, call GetResponseData to have XrdSsi ask the worker for the data.
        QSERV_LOGCONTEXT_QUERY_JOB(_qid, _jobid);
        util::Timer tWaiting;
        util::Timer tTotal;
        {
            tTotal.start();
            auto jq = _jQuery.lock();
            auto qr = _qRequest.lock();
            if (jq == nullptr || qr == nullptr) {
                LOGS(_log, LOG_LVL_WARN, "AskForResp null before GetResponseData");
                // No way to call _errorFinish().
                _setState(State::DONE2);
                return;
            }

            if (qr->isQueryCancelled()) {
                LOGS(_log, LOG_LVL_DEBUG, "AskForResp query was cancelled");
                qr->_errorFinish(true);
                _setState(State::DONE2);
                return;
            }
            std::vector<char>& buffer = *_bufPtr;
            LOGS(_log, LOG_LVL_TRACE, "AskForResp GetResponseData size=" << buffer.size());
            tWaiting.start();
            qr->GetResponseData(&buffer[0], buffer.size());
        }

        // Wait for XrdSsi to call ProcessResponseData with the data,
        // which will notify this wait with a call to receivedProcessResponseDataParameters.
        {
            std::unique_lock<std::mutex> uLock(_mtx);
            // TODO: make timed wait, check for wedged, if weak pointers dead, log and give up.
            _cv.wait(uLock, [this](){ return _state != State::STARTED0; });
            tWaiting.stop();
            // _mtx is locked at this point.
            LOGS(_log, LOG_LVL_TRACE, "AskForResp should be DATAREADY1 " << (int)_state);
            if (_state == State::DONE2) {
                // There was a problem. End the stream associated
                auto qr = _qRequest.lock();
                if (qr != nullptr) {
                    qr->_errorFinish();
                }
                LOGS(_log, LOG_LVL_INFO, "AskForResp returning early");
                return;
            }
        }

        // Actually process the data.
        // If more data needs to be sent, _processData will make a new AskForResponseDataCmd
        // object and queue it.
        {
            auto jq = _jQuery.lock();
            auto qr = _qRequest.lock();
            if (jq == nullptr || qr == nullptr) {
                _setState(State::DONE2);
                LOGS(_log, LOG_LVL_WARN, "AskForResp null before processData");
                return;
            }
            qr->_processData(jq, _blen, _last);
            // _processData will have created another AskForResponseDataCmd object if was needed.
            tTotal.stop();
        }
        _setState(State::DONE2);
        LOGS(_log, LOG_LVL_DEBUG, "Ask data is done wait=" << tWaiting.getElapsed() <<
                " total=" << tTotal.getElapsed());
    }

    void notifyDataSuccess(int blen, bool last) {
        {
            std::lock_guard<std::mutex> lg(_mtx);
            _blen = blen;
            _last = last;
            _state = State::DATAREADY1;
        }
        _cv.notify_all();
    }

    void notifyFailed() {
        LOGS(_log, LOG_LVL_INFO, "notifyFailed");
        _setState(State::DONE2);
        _cv.notify_all();
    }

    State getState() const {
        std::lock_guard<std::mutex> lg(_mtx);
        return _state;
    }

    ResponseHandler::BufPtr getBufPtr() { return _bufPtr; }

private:
    void _setState(State const state) {
        std::lock_guard<std::mutex> lg(_mtx);
        _state = State::DONE2;
    }


    std::weak_ptr<QueryRequest> _qRequest;
    std::weak_ptr<JobQuery> _jQuery;
    QueryId _qid;
    int _jobid;
    mutable std::mutex _mtx;
    std::condition_variable _cv;
    State _state = State::STARTED0;

    ResponseHandler::BufPtr _bufPtr;

    int _blen{-1};
    bool _last{true};
    util::InstanceCount _instCount{"AskForResponseDataCmd"};
};


////////////////////////////////////////////////////////////////////////
// QueryRequest
////////////////////////////////////////////////////////////////////////
QueryRequest::QueryRequest(JobQuery::Ptr const& jobQuery) :
  _jobQuery(jobQuery),
  _qid(jobQuery->getQueryId()),
  _jobid(jobQuery->getIdInt()),
  _jobIdStr(jobQuery->getIdStr()),
  _qdispPool(_jobQuery->getQdispPool()){
    QSERV_LOGCONTEXT_QUERY_JOB(_qid, _jobid);
    LOGS(_log, LOG_LVL_TRACE, "New QueryRequest");
}

QueryRequest::~QueryRequest() {
    QSERV_LOGCONTEXT_QUERY_JOB(_qid, _jobid);
    LOGS(_log, LOG_LVL_TRACE, "~QueryRequest");
    if (_askForResponseDataCmd != nullptr) {
        // This shouldn't really happen, but we really don't want to leave this blocking the pool.
        LOGS(_log, LOG_LVL_WARN, "~QueryRequest cleaning up _askForResponseDataCmd");
        _askForResponseDataCmd->notifyFailed();
    }
    if (!_finishedCalled) {
        LOGS(_log, LOG_LVL_WARN, "~QueryRequest cleaning up calling Finished");
        Finished(true);
    }
}

// content of request data
char* QueryRequest::GetRequest(int& requestLength) {
    QSERV_LOGCONTEXT_QUERY_JOB(_qid, _jobid);
    std::lock_guard<std::mutex> lock(_finishStatusMutex);
    auto jq = _jobQuery;
    if (_finishStatus != ACTIVE || jq == nullptr) {
        LOGS(_log, LOG_LVL_DEBUG, "QueryRequest::GetRequest called after job finished (cancelled?)");
        requestLength = 0;
        return const_cast<char*>("");
    }
    requestLength = jq->getDescription()->payload().size();
    LOGS(_log, LOG_LVL_DEBUG, "Requesting, payload size: " << requestLength);
    // Andy promises that his code won't corrupt it.
    return const_cast<char*>(jq->getDescription()->payload().data());
}

// precondition: rInfo.rType != isNone
// Must not throw exceptions: calling thread cannot trap them.
// Callback function for XrdSsiRequest.
//
bool QueryRequest::ProcessResponse(XrdSsiErrInfo  const& eInfo, XrdSsiRespInfo const& rInfo) {
    QSERV_LOGCONTEXT_QUERY_JOB(_qid, _jobid);
    LOGS(_log, LOG_LVL_DEBUG, "workerName=" << GetEndPoint() << " ProcessResponse");
    std::string errorDesc = _jobIdStr + " ";
    if (isQueryCancelled()) {
        LOGS(_log, LOG_LVL_WARN, "QueryRequest::ProcessResponse job already cancelled");
        cancel(); // calls _errorFinish()
        return true;
    }

    // Make a copy of the _jobQuery shared_ptr in case _jobQuery gets reset by a call to  cancel()
    auto jq = _jobQuery;
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE) {
            LOGS(_log, LOG_LVL_WARN,
                 "QueryRequest::GetRequest called after job finished (cancelled?)");
            return true;
        }
    }
    if (eInfo.hasError()) {
        std::ostringstream os;
        os << _jobIdStr << "ProcessResponse request failed "
           << getSsiErr(eInfo, nullptr);
        jq->getDescription()->respHandler()->errorFlush(os.str(), -1);
        jq->getStatus()->updateInfo(_jobIdStr, JobStatus::RESPONSE_ERROR);
        _errorFinish();
        return true;
    }
    switch(rInfo.rType) {
    case XrdSsiRespInfo::isNone: // All responses are non-null right now
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isNone";
        break;
    case XrdSsiRespInfo::isData: // Local-only for Mock tests!
        if (std::string(rInfo.buff, rInfo.blen) == "MockResponse") {
           jq->getStatus()->updateInfo(_jobIdStr, JobStatus::COMPLETE);
           _finish();
           return true;
        }
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isData";
        break;
    case XrdSsiRespInfo::isError:
        jq->getStatus()->updateInfo(_jobIdStr, JobStatus::RESPONSE_ERROR,
                                    rInfo.eNum, std::string(rInfo.eMsg));
        return _importError(std::string(rInfo.eMsg), rInfo.eNum);
    case XrdSsiRespInfo::isFile: // Local-only
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isFile";
        break;
    case XrdSsiRespInfo::isStream: // All remote requests
        jq->getStatus()->updateInfo(_jobIdStr, JobStatus::RESPONSE_READY);
        return _importStream(jq);
    default:
        errorDesc += "Out of range XrdSsiRespInfo.rType";
    }
    return _importError(errorDesc, -1);
}


/// Retrieve and process results in using the XrdSsi stream mechanism
/// Uses a copy of JobQuery::Ptr instead of _jobQuery as a call to cancel() would reset _jobQuery.
bool QueryRequest::_importStream(JobQuery::Ptr const& jq) {
    if (_askForResponseDataCmd != nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "_importStream There's already an _askForResponseDataCmd object!!");
        // Keep the previous object from wedging the pool.
        _askForResponseDataCmd->notifyFailed();
    }
    _askForResponseDataCmd = std::make_shared<AskForResponseDataCmd>(
                               shared_from_this(), jq, proto::ProtoHeaderWrap::PROTO_HEADER_SIZE);
    _queueAskForResponse(_askForResponseDataCmd, jq, true);
    return true;
}


void QueryRequest::_queueAskForResponse(AskForResponseDataCmd::Ptr const& cmd, JobQuery::Ptr const& jq, bool initialRequest) {
    // ScanInfo::Rating { FASTEST = 0, FAST = 10, MEDIUM = 20, SLOW = 30, SLOWEST = 100 };
    // Trying to get existing requests done before doing new ones.

    //int rating = jq->getDescription()->getScanRating(); &&&
    if (jq->getDescription()->getScanInteractive()) {
        _qdispPool->queCmd(cmd, 0);
    } else {
        if (initialRequest) {
            _qdispPool->queCmd(cmd, 3);
        } else {
            _qdispPool->queCmd(cmd, 2);
        }
    }
    /* &&&
    } else if (rating <= proto::ScanInfo::Rating::FAST) {
        if (_largeResult) {
            _qdispPool->queCmd(cmd, 5);
        } else {
            _qdispPool->queCmd(cmd, 2);
        }
    } else if (rating <= proto::ScanInfo::Rating::MEDIUM) {
        if (_largeResult) {
            _qdispPool->queCmd(cmd, 6);
        } else {
            _qdispPool->queCmd(cmd, 3);
        }
    } else if (rating <= proto::ScanInfo::Rating::SLOW) {
        if (not _largeResult) {
            _qdispPool->queCmd(cmd, 4);
        } else {
            _qdispPool->queCmd(cmd, 7);
        }
    } else {
        _qdispPool->queCmd(cmd, 7);
    }
    */

}

/// Process an incoming error.
bool QueryRequest::_importError(std::string const& msg, int code) {
    auto jq = _jobQuery;
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE || jq == nullptr) {
            LOGS(_log, LOG_LVL_WARN, "QueryRequest::_importError code=" << code
                      << " msg=" << msg << " not passed");
            return false;
        }
        jq->getDescription()->respHandler()->errorFlush(msg, code);
    }
    _errorFinish();
    return true;
}


void QueryRequest::_setHoldState(HoldState state) {
    if (state != _holdState) {
        LOGS(_log, LOG_LVL_DEBUG, "holdState changed from " << _holdState << " to " << state);
    }
    _holdState = state;
}


XrdSsiRequest::PRD_Xeq QueryRequest::ProcessResponseData(XrdSsiErrInfo const& eInfo,
                                                         char *buff, int blen, bool last) { // Step 7
    QSERV_LOGCONTEXT_QUERY_JOB(_qid, _jobid);
    // buff is ignored here. It points to jq->getDescription()->respHandler()->_mBuf, which
    // is accessed directly by the respHandler. _mBuf is a member of MergingHandler.
    LOGS(_log, LOG_LVL_DEBUG, "ProcessResponseData with buflen=" << blen
                              << " " << (last ? "(last)" : "(more)"));
    if (_askForResponseDataCmd == nullptr) {
        LOGS(_log, LOG_LVL_ERROR,
             "ProcessResponseData called with invalid _askForResponseDataCmd!!!");
        return XrdSsiRequest::PRD_Normal;
    }

    // Work with a copy of _jobQuery so it doesn't get reset underneath us by a call to cancel().
    JobQuery::Ptr jq = _jobQuery;
    bool jobFinished = false;
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        jobFinished = _finishStatus != ACTIVE || jq == nullptr;
    }
    if (jobFinished) {
        LOGS(_log, LOG_LVL_INFO, "ProcessResponseData job is inactive.");
        // This job is already dead.
        _errorFinish();
        return XrdSsiRequest::PRD_Normal;
    }
    if (jq->isQueryCancelled()) {
        LOGS(_log, LOG_LVL_INFO, "ProcessResponseData job is cancelled.");
        _errorFinish(true);
        return XrdSsiRequest::PRD_Normal;
    }

    // If there's an error, it makes sense to handle it immediately.
    if (blen < 0) { // error, check errinfo object.
        int eCode;
        auto reason = getSsiErr(eInfo, &eCode);
        jq->getStatus()->updateInfo(_jobIdStr, JobStatus::RESPONSE_DATA_NACK, eCode, reason);
        LOGS(_log, LOG_LVL_ERROR, "ProcessResponse[data] error(" << eCode
             << " " << reason << ")");
        jq->getDescription()->respHandler()->errorFlush(
            "Couldn't retrieve response data:" + reason + " " + _jobIdStr, eCode);

        // Let the AskForResponseDataCmd end.
        _askForResponseDataCmd->notifyFailed();
        _errorFinish();
        // An error occurred, let processing continue so it can be cleaned up soon.
        return XrdSsiRequest::PRD_Normal;
    }

    jq->getStatus()->updateInfo(_jobIdStr, JobStatus::RESPONSE_DATA);

    // Handle the response in a separate thread so we can give this one back to XrdSsi.
    // _askForResponseDataCmd should call QueryRequest::_processData() next.
    _askForResponseDataCmd->notifyDataSuccess(blen, last);
    return XrdSsiRequest::PRD_Normal;
}


void QueryRequest::_processData(JobQuery::Ptr const& jq, int blen, bool last) {
    // It's possible jq and _jobQuery differ, so need to use jq.
    if (jq->isQueryCancelled()) {
        LOGS(_log, LOG_LVL_WARN, "QueryRequest::_processData job was cancelled.");
        _errorFinish(true);
        return;
    }

    // Get a copy of the shared buffer pointer so askForResponseDataCmd can be deleted.
    ResponseHandler::BufPtr bufPtr = _askForResponseDataCmd->getBufPtr();
    _askForResponseDataCmd.reset(); // No longer need it, and don't want the destructor calling _errorFinish().
    bool largeResult = false;
    int nextBufSize = 0;
    bool flushOk = jq->getDescription()->respHandler()->flush(blen, bufPtr, last, largeResult, nextBufSize);
    if (largeResult) {
        if (!_largeResult) LOGS(_log, LOG_LVL_DEBUG, "holdState largeResult set to true");
        _largeResult = true; // Once the worker indicates it's a large result, it stays that way.
    }

    if (flushOk) {
        if (last) {
            if (last && nextBufSize != 0) {
                LOGS(_log, LOG_LVL_WARN,
                     "Connection closed when more information expected sz=" << nextBufSize);
            }
            jq->getStatus()->updateInfo(_jobIdStr, JobStatus::COMPLETE);
            _finish();
            // At this point all blocks for this job have been read, there's no point in
            // having XrdSsi wait for anything.
            return;
        } else {
            _askForResponseDataCmd = std::make_shared<AskForResponseDataCmd>(shared_from_this(), jq, nextBufSize);
            LOGS(_log, LOG_LVL_DEBUG, "queuing askForResponseDataCmd bufSize=" << nextBufSize);
            _queueAskForResponse(_askForResponseDataCmd, jq, false);
        }
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "ProcessResponse data flush failed");
        ResponseHandler::Error err = jq->getDescription()->respHandler()->getError();
        jq->getStatus()->updateInfo(_jobIdStr, JobStatus::MERGE_ERROR, err.getCode(), err.getMsg());
        // @todo DM-2378 Take a closer look at what causes this error and take
        // appropriate action. There could be cases where this is recoverable.
        _retried.store(true); // Do not retry
        _errorFinish(true);
    }
    return;
}


/// @return true if QueryRequest cancelled successfully.
bool QueryRequest::cancel() {
    LOGS(_log, LOG_LVL_DEBUG, "QueryRequest::cancel");
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_cancelled) {
            LOGS(_log, LOG_LVL_DEBUG, "QueryRequest::cancel already cancelled, ignoring");
            return false; // Don't do anything if already cancelled.
        }
        _cancelled = true;
        _retried.store(true); // Prevent retries.
        // Only call the following if the job is NOT already done.
        if (_finishStatus == ACTIVE) {
            auto jq = _jobQuery;
            if (jq != nullptr) jq->getStatus()->updateInfo(_jobIdStr, JobStatus::CANCEL);
        }
    }
    return _errorFinish(true); // return true if errorFinish cancelled
}


/// @return true if this object's JobQuery, or its Executive has been cancelled.
/// It takes time for the Executive to flag all jobs as being cancelled
bool QueryRequest::isQueryCancelled() {
    auto jq = _jobQuery;
    if (jq == nullptr) {
        // Need to check if _jobQuery is null due to cancellation.
        return isQueryRequestCancelled();
    }
    return jq->isQueryCancelled();
}


/// @return true if QueryRequest::cancel() has been called.
/// QueryRequest::isCancelled() is a much better indicator of user query cancellation.
bool QueryRequest::isQueryRequestCancelled() {
    std::lock_guard<std::mutex> lock(_finishStatusMutex);
    return _cancelled;
}


/// Cleanup pointers so this class can be deleted.
/// This should only be called by _finish or _errorFinish.
void QueryRequest::cleanup() {
    LOGS(_log, LOG_LVL_TRACE, "QueryRequest::cleanup()");
    {
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus == ACTIVE) {
            LOGS(_log, LOG_LVL_ERROR, "QueryRequest::cleanup called before _finish or _errorFinish");
            return;
        }
    }

    // These need to be outside the mutex lock, or you could delete
    // _finishStatusMutex before it is unlocked.
    // This should reset _jobquery and _keepAlive without risk of either being deleted
    // before being reset.
    std::shared_ptr<JobQuery> jq(std::move(_jobQuery));
    std::shared_ptr<QueryRequest> keep(std::move(_keepAlive));
}


/// Finalize under error conditions and retry or report completion
/// THIS FUNCTION WILL RESULT IN THIS OBJECT BEING DESTROYED, UNLESS there is
/// a local shared pointer for this QueryRequest and/or its owner JobQuery.
/// See QueryRequest::cleanup()
/// @return true if this QueryRequest object had the authority to make changes.
bool QueryRequest::_errorFinish(bool shouldCancel) {
    LOGS(_log, LOG_LVL_DEBUG, "_errorFinish() shouldCancel=" << shouldCancel);
    auto jq = _jobQuery;
    {
        // Running _errorFinish more than once could cause errors.
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE || jq == nullptr) {
            // Either _finish or _errorFinish has already been called.
            LOGS_DEBUG("_errorFinish() job no longer ACTIVE, ignoring "
                       << " _finishStatus=" << _finishStatus
                       << " ACTIVE=" << ACTIVE << " jq=" << jq);
            return false;
        }
        _finishStatus = ERROR;
    }

    // Make the calls outside of the mutex lock.
    LOGS(_log, LOG_LVL_DEBUG, "calling Finished(shouldCancel=" << shouldCancel << ")");
    bool ok = Finished(shouldCancel);
    _finishedCalled = true;
    if (!ok) {
        LOGS(_log, LOG_LVL_ERROR,  "QueryRequest::_errorFinish !ok ");
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "QueryRequest::_errorFinish ok");
    }

    if (!_retried.exchange(true) && !shouldCancel) {
        // There's a slight race condition here. _jobQuery::runJob() creates a
        // new QueryRequest object which will replace this one in _jobQuery.
        // The replacement could show up before this one's cleanup() is called,
        // so this will keep this alive.
        LOGS(_log, LOG_LVL_DEBUG, "QueryRequest::_errorFinish retrying");
        _keepAlive = jq->getQueryRequest(); // shared pointer to this
        if (!jq->runJob()) {
            // Retry failed, nothing left to try.
            LOGS(_log, LOG_LVL_DEBUG, "errorFinish retry failed");
            _callMarkComplete(false);
        }
    } else {
        _callMarkComplete(false);
    }
    cleanup(); // Reset smart pointers so this object can be deleted.
    return true;
}

/// Finalize under success conditions and report completion.
/// THIS FUNCTION WILL RESULT IN THIS OBJECT BEING DESTROYED, UNLESS there is
/// a local shared pointer for this QueryRequest and/or its owner JobQuery.
/// See QueryRequest::cleanup()
void QueryRequest::_finish() {
    LOGS(_log, LOG_LVL_TRACE, "QueryRequest::_finish");
    {
        // Running _finish more than once would cause errors.
        std::lock_guard<std::mutex> lock(_finishStatusMutex);
        if (_finishStatus != ACTIVE) {
            // Either _finish or _errorFinish has already been called.
            LOGS(_log, LOG_LVL_WARN, "QueryRequest::_finish called when not ACTIVE, ignoring");
            return;
        }
        _finishStatus = FINISHED;
    }

    bool ok = Finished();
    _finishedCalled = true;
    if (!ok) {
        LOGS(_log, LOG_LVL_ERROR, "QueryRequest::finish Finished() !ok ");
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "QueryRequest::finish Finished() ok.");
    }

    _callMarkComplete(true);
    cleanup();
}

/// Inform the Executive that this query completed, and
// Call MarkCompleteFunc only once, it should only be called from _finish() or _errorFinish.
void QueryRequest::_callMarkComplete(bool success) {
    if (!_calledMarkComplete.exchange(true)) {
        auto jq = _jobQuery;
        if (jq != nullptr) jq->getMarkCompleteFunc()->operator()(success);
    }
}

std::ostream& operator<<(std::ostream& os, QueryRequest const& qr) {
    os << "QueryRequest " << qr._jobIdStr;
    return os;
}


/// @return The error text and code that SSI set.
/// if eCode != nullptr, it is set to the error code set by SSI.
std::string QueryRequest::getSsiErr(XrdSsiErrInfo const& eInfo, int* eCode) {
    int errNum;
    std::string errText = eInfo.Get(errNum);
    if (eCode != nullptr) {
        *eCode = errNum;
    }
    std::ostringstream os;
    os << "SSI_Error(" << errNum << ":" << errText << ")";
    return os.str();
}

}}} // lsst::qserv::qdisp
