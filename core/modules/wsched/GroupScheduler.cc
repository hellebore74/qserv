/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
  * @file GroupScheduler.cc
  *
  * @brief A scheduler implementation that limits disk scans to one at
  * a time, but allows multiple queries to share I/O.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "wsched/GroupScheduler.h"
#include <iostream>
#include <sstream>
#include <boost/thread.hpp>
#include "proto/worker.pb.h"
#include "wlog/WLogger.h"

namespace lsst {
namespace qserv {
namespace wsched {
////////////////////////////////////////////////////////////////////////
// class GroupScheduler
////////////////////////////////////////////////////////////////////////
GroupScheduler::GroupScheduler(wlog::WLogger::Ptr logger)
    : _maxRunning(4), // FIXME: set to some multiple of system proc count.
      _logger(logger) {
}

struct matchHash {
    explicit matchHash(std::string const& hash_) : hash(hash_) {}
    bool operator()(wcontrol::Task::Ptr const& t) {
        return t && (t->hash == hash);
    }
    std::string const hash;
};

bool
GroupScheduler::removeByHash(std::string const& hash) {
    int removed = _queue.removeIf(matchHash(hash));
    return removed > 0;
}

void
GroupScheduler::queueTaskAct(wcontrol::Task::Ptr incoming) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    _enqueueTask(incoming);
}

wcontrol::TaskQueuePtr
GroupScheduler::nopAct(wcontrol::TaskQueuePtr running) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    assert(_integrityHelper());
    return _getNextIfAvail(running->size());
}

/// @return a queue of all tasks ready to run.
///
wcontrol::TaskQueuePtr
GroupScheduler::newTaskAct(wcontrol::Task::Ptr incoming,
                           wcontrol::TaskQueuePtr running) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    assert(_integrityHelper());
    assert(running.get());
    _enqueueTask(incoming);
    return _getNextIfAvail(running->size());
}

wcontrol::TaskQueuePtr
GroupScheduler::taskFinishAct(wcontrol::Task::Ptr finished,
                              wcontrol::TaskQueuePtr running) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    assert(_integrityHelper());

    std::ostringstream os;
    os << "Completed: " << "(" << finished->msg->chunkid()
       << ")" << finished->msg->fragment(0).query(0);
    _logger->debug(os.str());
    return _getNextIfAvail(running->size());
}

/// @return true if data is okay.
bool
GroupScheduler::checkIntegrity() {
    boost::lock_guard<boost::mutex> guard(_mutex);
    return _integrityHelper();
}

/// @return true if data is okay
/// precondition: _mutex is locked.
bool
GroupScheduler::_integrityHelper() {
    // FIXME
    return true;
}

/// Precondition: _mutex is already locked.
/// @return new tasks to run
/// TODO: preferential treatment for chunkId just run?
/// or chunkId that are currently running?
wcontrol::TaskQueuePtr
GroupScheduler::_getNextIfAvail(int runCount) {
    int available = _maxRunning - runCount;
    if(available <= 0) {
        return wcontrol::TaskQueuePtr();
    }
    return _getNextTasks(available);
}

/// Precondition: _mutex is already locked.
/// @return new tasks to run
wcontrol::TaskQueuePtr
GroupScheduler::_getNextTasks(int max) {
    // FIXME: Select disk based on chunk location.
    if(max < 1) { throw std::invalid_argument("max < 1)"); }
    std::ostringstream os;
    os << "_getNextTasks(" << max << ")>->->";
    _logger->debug(os.str());
    os.str("");
    wcontrol::TaskQueuePtr tq;
    if(_queue.size() > 0) {
        tq.reset(new wcontrol::TaskQueue());
        for(int i=max; i >= 0; --i) {
            if(_queue.empty()) { break; }
            wcontrol::Task::Ptr t = _queue.front();
            tq->push_back(t);
            _queue.pop_front();
        }
    }
    if(tq) {
        os << "Returning " << tq->size() << " to launch";
        _logger->debug(os.str());
    }
    assert(_integrityHelper());
    _logger->debug("_getNextTasks <<<<<");
    return tq;
}

/// Precondition: _mutex is locked.
void
GroupScheduler::_enqueueTask(wcontrol::Task::Ptr incoming) {
    if(!incoming) {
        throw std::invalid_argument("null task");
    }
    _queue.insert(incoming);
    std::ostringstream os;
    proto::TaskMsg const& msg = *(incoming->msg);
    os << "Adding new task: " << msg.chunkid()
       << " : " << msg.fragment(0).query(0);
    _logger->debug(os.str());
}

}}} // namespace lsst::qserv::wsched