// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 LSST Corporation.
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
#include <xrdsvc/SsiRequest.h>
#include <cctype>
#include <cstddef>
#include <iostream>
#include <string>

// Third-party headers
#include "XrdSsi/XrdSsiRequest.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/LogContext.h"
#include "global/ResourceUnit.h"
#include "proto/FrameBuffer.h"
#include "proto/worker.pb.h"
#include "util/InstanceCount.h"
#include "util/Timer.h"
#include "wbase/MsgProcessor.h"
#include "wbase/SendChannelShared.h"
#include "wpublish/AddChunkGroupCommand.h"
#include "wpublish/ChunkListCommand.h"
#include "wpublish/GetChunkListCommand.h"
#include "wpublish/GetStatusCommand.h"
#include "wpublish/RemoveChunkGroupCommand.h"
#include "wpublish/ResourceMonitor.h"
#include "wpublish/SetChunkListCommand.h"
#include "wpublish/TestEchoCommand.h"
#include "xrdsvc/ChannelStream.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdsvc.SsiRequest");
}

namespace lsst::qserv::xrdsvc {

std::shared_ptr<wpublish::ResourceMonitor> SsiRequest::_resourceMonitor(new wpublish::ResourceMonitor());

SsiRequest::~SsiRequest() {
    LOGS(_log, LOG_LVL_DEBUG, "~SsiRequest()");
    UnBindRequest();
}

void SsiRequest::reportError(std::string const& errStr) {
    LOGS(_log, LOG_LVL_WARN, errStr);
    replyError(errStr, EINVAL);
    ReleaseRequestBuffer();
}

uint64_t countLimiter = 0;  // LockupDB

// Step 4
/// Called by XrdSsi to actually process a request.
void SsiRequest::execute(XrdSsiRequest& req) {
    util::Timer t;
    LOGS(_log, LOG_LVL_DEBUG, "Execute request, resource=" << _resourceName);

    char* reqData = nullptr;
    int reqSize;
    t.start();
    reqData = req.GetRequest(reqSize);
    t.stop();
    LOGS(_log, LOG_LVL_DEBUG, "GetRequest took " << t.getElapsed() << " seconds");

    // We bind this object to the request now. This allows us to respond at any
    // time (much simpler). Though the manual forgot to say that all pending
    // events will be reflected on a different thread the moment we bind the
    // request; the fact allows us to use a mutex to serialize the order of
    // initialization and possible early cancellation. We protect this code
    // with a mutex gaurd which will be released upon exit.
    //
    std::lock_guard<std::mutex> lock(_finMutex);
    BindRequest(req);

    ResourceUnit ru(_resourceName);

    // Make sure the requested resource belongs to this worker
    if (!(*_validator)(ru)) {
        reportError("WARNING: request to the unowned resource detected:" + _resourceName);
        return;
    }

    // Process the request
    switch (ru.unitType()) {
        case ResourceUnit::DBCHUNK: {
            // Increment the counter of the database/chunk resources in use
            _resourceMonitor->increment(_resourceName);

            // reqData has the entire request, so we can unpack it without waiting for
            // more data.
            LOGS(_log, LOG_LVL_DEBUG, "Decoding TaskMsg of size " << reqSize);
            auto taskMsg = std::make_shared<proto::TaskMsg>();
            if (!taskMsg->ParseFromArray(reqData, reqSize) || !taskMsg->IsInitialized()) {
                reportError("Failed to decode TaskMsg on resource db=" + ru.db() +
                            " chunkId=" + std::to_string(ru.chunk()));
                return;
            }

            QSERV_LOGCONTEXT_QUERY_JOB(taskMsg->queryid(), taskMsg->jobid());

            if (!taskMsg->has_db() || !taskMsg->has_chunkid() || (ru.db() != taskMsg->db()) ||
                (ru.chunk() != taskMsg->chunkid())) {
                reportError("Mismatched db/chunk in TaskMsg on resource db=" + ru.db() +
                            " chunkId=" + std::to_string(ru.chunk()));
                return;
            }

            if (not(taskMsg->has_queryid() && taskMsg->has_jobid() && taskMsg->has_scaninteractive() &&
                    taskMsg->has_attemptcount() && taskMsg->has_czarid())) {
                reportError(std::string("taskMsg missing required field ") +
                            " queryid:" + std::to_string(taskMsg->has_queryid()) +
                            " jobid:" + std::to_string(taskMsg->has_jobid()) +
                            " scaninteractive:" + std::to_string(taskMsg->has_scaninteractive()) +
                            " attemptcount:" + std::to_string(taskMsg->has_attemptcount()) +
                            " czarid:" + std::to_string(taskMsg->has_czarid()));
                return;
            }

            // Now that the request is decoded (successfully or not), release the
            // xrootd request buffer. To avoid data races, this must happen before
            // the task is handed off to another thread for processing, as there is a
            // reference to this SsiRequest inside the reply channel for the task,
            // and after the call to BindRequest.
            auto sendChannelBase = std::make_shared<wbase::SendChannel>(shared_from_this());
            auto sendChannel =
                    wbase::SendChannelShared::create(sendChannelBase, _transmitMgr, taskMsg->czarid());
            auto tasks = wbase::Task::createTasks(taskMsg, sendChannel);

            ReleaseRequestBuffer();
            t.start();
            _processor->processTasks(tasks);  // Queues tasks to be run later.
            t.stop();
            LOGS(_log, LOG_LVL_DEBUG,
                 "Enqueued TaskMsg for " << ru << " in " << t.getElapsed() << " seconds");
            break;
        }
        case ResourceUnit::WORKER: {
            LOGS(_log, LOG_LVL_DEBUG, "Parsing WorkerCommand for resource=" << _resourceName);

            wbase::WorkerCommand::Ptr const command = parseWorkerCommand(reqData, reqSize);
            if (not command) return;

            // The buffer must be released before submitting commands for
            // further processing.
            ReleaseRequestBuffer();
            _processor->processCommand(command);  // Queues the command to be run later.

            LOGS(_log, LOG_LVL_DEBUG, "Enqueued WorkerCommand for resource=" << _resourceName);
            ++countLimiter;
            if (countLimiter % 500 == 0) {
                LOGS(_log, LOG_LVL_DEBUG, "Forcing instance count to the log");
                util::InstanceCount ic("ForcingPrint_LDB");  // LockupDB
            }
            break;
        }
        default:
            reportError("Unexpected unit type '" + std::to_string(ru.unitType()) +
                        "', resource name: " + _resourceName);
            break;
    }

    // Note that upon exit the _finMutex will be unlocked allowing Finished()
    // to actually do something once everything is actually setup.
}

wbase::WorkerCommand::Ptr SsiRequest::parseWorkerCommand(char const* reqData, int reqSize) {
    wbase::SendChannel::Ptr const sendChannel = std::make_shared<wbase::SendChannel>(shared_from_this());

    wbase::WorkerCommand::Ptr command;

    try {
        // reqData has the entire request, so we can unpack it without waiting for
        // more data.
        proto::FrameBufferView view(reqData, reqSize);

        proto::WorkerCommandH header;
        view.parse(header);

        LOGS(_log, LOG_LVL_DEBUG,
             "WorkerCommandH: command=" << proto::WorkerCommandH_Command_Name(header.command())
                                        << " resource=" << _resourceName);

        switch (header.command()) {
            case proto::WorkerCommandH::TEST_ECHO: {
                proto::WorkerCommandTestEchoM echo;
                view.parse(echo);

                command = std::make_shared<wpublish::TestEchoCommand>(sendChannel, echo.value());
                break;
            }
            case proto::WorkerCommandH::ADD_CHUNK_GROUP:
            case proto::WorkerCommandH::REMOVE_CHUNK_GROUP: {
                proto::WorkerCommandChunkGroupM group;
                view.parse(group);

                std::vector<std::string> dbs;
                for (int i = 0, num = group.dbs_size(); i < num; ++i) dbs.push_back(group.dbs(i));

                int const chunk = group.chunk();
                bool const force = group.force();

                if (header.command() == proto::WorkerCommandH::ADD_CHUNK_GROUP)
                    command = std::make_shared<wpublish::AddChunkGroupCommand>(sendChannel, _chunkInventory,
                                                                               _mySqlConfig, chunk, dbs);
                else
                    command = std::make_shared<wpublish::RemoveChunkGroupCommand>(
                            sendChannel, _chunkInventory, _resourceMonitor, _mySqlConfig, chunk, dbs, force);
                break;
            }
            case proto::WorkerCommandH::UPDATE_CHUNK_LIST: {
                proto::WorkerCommandUpdateChunkListM message;
                view.parse(message);

                if (message.rebuild())
                    command = std::make_shared<wpublish::RebuildChunkListCommand>(
                            sendChannel, _chunkInventory, _mySqlConfig, message.reload());
                else
                    command = std::make_shared<wpublish::ReloadChunkListCommand>(sendChannel, _chunkInventory,
                                                                                 _mySqlConfig);
                break;
            }
            case proto::WorkerCommandH::GET_CHUNK_LIST: {
                command = std::make_shared<wpublish::GetChunkListCommand>(sendChannel, _chunkInventory,
                                                                          _resourceMonitor);
                break;
            }
            case proto::WorkerCommandH::SET_CHUNK_LIST: {
                proto::WorkerCommandSetChunkListM message;
                view.parse(message);

                std::vector<wpublish::SetChunkListCommand::Chunk> chunks;
                for (int i = 0, num = message.chunks_size(); i < num; ++i) {
                    chunks.push_back(wpublish::SetChunkListCommand::Chunk{message.chunks(i).db(),
                                                                          message.chunks(i).chunk()});
                }
                std::vector<std::string> databases;
                for (int i = 0, num = message.databases_size(); i < num; ++i) {
                    databases.push_back(message.databases(i));
                }
                bool const force = message.force();

                command = std::make_shared<wpublish::SetChunkListCommand>(sendChannel, _chunkInventory,
                                                                          _resourceMonitor, _mySqlConfig,
                                                                          chunks, databases, force);
                break;
            }
            case proto::WorkerCommandH::GET_STATUS: {
                command = std::make_shared<wpublish::GetStatusCommand>(sendChannel, _processor,
                                                                       _resourceMonitor);
                break;
            }
            default:
                reportError("Unsupported command " + proto::WorkerCommandH_Command_Name(header.command()) +
                            " found in WorkerCommandH on worker resource=" + _resourceName);
                break;
        }

    } catch (proto::FrameBufferError const& ex) {
        reportError("Failed to decode a worker management command, error: " + std::string(ex.what()));
    }
    return command;
}

/// Called by SSI to free resources.
void SsiRequest::Finished(XrdSsiRequest& req, XrdSsiRespInfo const& rinfo, bool cancel) {  // Step 8
    if (cancel) {
        // Try to cancel the task, if there is one.
        auto task = _task.lock();
        if (task != nullptr) {
            task->cancel();
        }
    }

    // This call is sync (blocking).
    // client finished retrieving response, or cancelled.
    // release response resources (e.g. buf)
    // But first we must make sure that request setup completed (i.e execute()) by
    // locking _finMutex.
    {
        std::lock_guard<std::mutex> finLock(_finMutex);
        // Clean up _stream if it exists and don't add anything new to it either.
        _reqFinished = true;
        if (_stream != nullptr) {
            _stream->clearMsgs();
        }
    }

    auto keepAlive = freeSelfKeepAlive();

    // No buffers allocated, so don't need to free.
    // We can release/unlink the file now
    const char* type = "";
    switch (rinfo.rType) {
        case XrdSsiRespInfo::isNone:
            type = "type=isNone";
            break;
        case XrdSsiRespInfo::isData:
            type = "type=isData";
            break;
        case XrdSsiRespInfo::isError:
            type = "type=isError";
            break;
        case XrdSsiRespInfo::isFile:
            type = "type=isFile";
            break;
        case XrdSsiRespInfo::isStream:
            type = "type=isStream";
            break;
        case XrdSsiRespInfo::isHandle:
            type = "type=isHandle";
            break;
    }

    // Decrement the counter of the database/chunk resources in use
    ResourceUnit ru(_resourceName);
    if (ru.unitType() == ResourceUnit::DBCHUNK) {
        _resourceMonitor->decrement(_resourceName);
    }

    // We can't do much other than close the file.
    // It should work (on linux) to unlink the file after we open it, though.
    // With the optimizer on '-Og', there was a double free for a SsiRequest.
    // The likely cause could be keepAlive being optimized out for being unused.
    // The problem has not reoccurred since adding keepAlive to the following
    // comment, but having code depend on a comment line is ugly in its own way.
    LOGS(_log, LOG_LVL_DEBUG, "RequestFinished " << type << " " << keepAlive.use_count());
}

bool SsiRequest::reply(char const* buf, int bufLen) {
    Status s = SetResponse(buf, bufLen);
    if (s != XrdSsiResponder::wasPosted) {
        LOGS(_log, LOG_LVL_ERROR, "DANGER: Couldn't post response of length=" << bufLen);
        return false;
    }
    return true;
}

bool SsiRequest::replyError(std::string const& msg, int code) {
    Status s = SetErrResponse(msg.c_str(), code);
    if (s != XrdSsiResponder::wasPosted) {
        LOGS(_log, LOG_LVL_ERROR, "DANGER: Couldn't post error response " << msg);
        return false;
    }
    return true;
}

bool SsiRequest::replyFile(int fd, long long fSize) {
    util::Timer t;
    t.start();
    Status s = SetResponse(fSize, fd);
    if (s == XrdSsiResponder::wasPosted) {
        LOGS(_log, LOG_LVL_DEBUG, "file posted ok");
    } else {
        if (s == XrdSsiResponder::notActive) {
            LOGS(_log, LOG_LVL_ERROR,
                 "DANGER: Couldn't post response file of length=" << fSize << ", responder not active.");
        } else {
            LOGS(_log, LOG_LVL_ERROR, "DANGER: Couldn't post response file of length=" << fSize);
        }
        replyError("Internal error posting response file", 1);
        t.stop();
        return false;  // call must handle everything else.
    }
    t.stop();
    LOGS(_log, LOG_LVL_DEBUG, "replyFile took " << t.getElapsed() << " seconds");
    return true;
}

bool SsiRequest::replyStream(StreamBuffer::Ptr const& sBuf, bool last, int scsSeq) {
    LOGS(_log, LOG_LVL_DEBUG,
         "replyStream, checking stream size=" << sBuf->getSize() << " last=" << last << " scsseq=" << scsSeq);

    // Normally, XrdSsi would call Recycle() when it is done with sBuf, but if this function
    // returns false, then it must call Recycle(). Otherwise, the scheduler will likely
    // wedge waiting for the buffer to be released.
    std::lock_guard<std::mutex> finLock(_finMutex);
    if (_reqFinished) {
        // Finished() was called, give up.
        LOGS(_log, LOG_LVL_ERROR, "replyStream called after reqFinished.");
        sBuf->Recycle();
        return false;
    }
    // Create a stream if needed.
    if (!_stream) {
        _stream = std::make_shared<ChannelStream>();
        if (SetResponse(_stream.get()) != XrdSsiResponder::Status::wasPosted) {
            LOGS(_log, LOG_LVL_WARN, "SetResponse stream failed, calling Recycle for sBuf");
            // SetResponse return value indicates XrdSsi wont call Recycle().
            sBuf->Recycle();
            return false;
        }
    } else if (_stream->closed()) {
        // XrdSsi isn't going to call Recycle if we wind up here.
        LOGS(_log, LOG_LVL_ERROR, "Logic error SsiRequest::replyStream called with stream closed.");
        sBuf->Recycle();
        return false;
    }
    // XrdSsi or Finished() will call Recycle().
    LOGS(_log, LOG_LVL_INFO, "SsiRequest::replyStream seq=" << getSeq() << " scsseq=" << scsSeq);
    _stream->append(sBuf, last, scsSeq);
    return true;
}

bool SsiRequest::sendMetadata(const char* buf, int blen) {
    Status stat = SetMetadata(buf, blen);
    switch (stat) {
        case XrdSsiResponder::wasPosted:
            return true;
        case XrdSsiResponder::notActive:
            LOGS(_log, LOG_LVL_ERROR, "failed to setMetadata notActive");
            break;
        case XrdSsiResponder::notPosted:
            LOGS(_log, LOG_LVL_ERROR, "failed to setMetadata notPosted blen=" << blen);
            break;
        default:
            LOGS(_log, LOG_LVL_ERROR, "failed to setMetadata unkown state blen=" << blen);
    }
    return false;
}

SsiRequest::Ptr SsiRequest::freeSelfKeepAlive() {
    Ptr keepAlive = std::move(_selfKeepAlive);
    return keepAlive;
}

uint64_t SsiRequest::getSeq() const {
    if (_stream == nullptr) return 0;
    return _stream->getSeq();
}

}  // namespace lsst::qserv::xrdsvc
