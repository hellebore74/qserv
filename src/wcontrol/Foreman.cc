// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2016 AURA/LSST.
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
#include "wcontrol/Foreman.h"

// System headers
#include <cassert>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "wbase/WorkerCommand.h"
#include "wcontrol/SqlConnMgr.h"
#include "wcontrol/WorkerStats.h"
#include "wdb/ChunkResource.h"
#include "wdb/SQLBackend.h"
#include "wpublish/QueriesAndChunks.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wcontrol.Foreman");
}

using namespace std;

namespace lsst::qserv::wcontrol {

Foreman::Foreman(Scheduler::Ptr const& scheduler, unsigned int poolSize, unsigned int maxPoolThreads,
                 mysql::MySqlConfig const& mySqlConfig, wpublish::QueriesAndChunks::Ptr const& queries,
                 std::shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr,
                 std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr, std::string const& resultsDirname)
        : _scheduler(scheduler),
          _mySqlConfig(mySqlConfig),
          _queries(queries),
          _sqlConnMgr(sqlConnMgr),
          _transmitMgr(transmitMgr),
          _resultsDirname(resultsDirname) {
    // Make the chunk resource mgr
    // Creating backend makes a connection to the database for making temporary tables.
    // It will delete temporary tables that it can identify as being created by a worker.
    // Previous instances of the worker will terminate when they try to use or create temporary tables.
    // Previous instances of the worker should be terminated before a new worker is started.
    _chunkResourceMgr = wdb::ChunkResourceMgr::newMgr(make_shared<wdb::SQLBackend>(_mySqlConfig));

    assert(_scheduler);  // Cannot operate without scheduler.

    LOGS(_log, LOG_LVL_DEBUG, "poolSize=" << poolSize << " maxPoolThreads=" << maxPoolThreads);
    _pool = util::ThreadPool::newThreadPool(poolSize, maxPoolThreads, _scheduler);

    _workerCommandQueue = make_shared<util::CommandQueue>();
    _workerCommandPool = util::ThreadPool::newThreadPool(poolSize, _workerCommandQueue);

    WorkerStats::setup();  // FUTURE: maybe add links to scheduler, _backend, etc?
}

Foreman::~Foreman() {
    LOGS(_log, LOG_LVL_DEBUG, "Foreman::~Foreman()");
    // It will take significant effort to have xrootd shutdown cleanly and this will never get called
    // until that happens.
    _pool->shutdownPool();
}

void Foreman::processTasks(vector<wbase::Task::Ptr> const& tasks) {
    std::vector<util::Command::Ptr> cmds;
    for (auto const& task : tasks) {
        _queries->addTask(task);
        cmds.push_back(task);
    }
    _scheduler->queCmd(cmds);
}

void Foreman::processCommand(shared_ptr<wbase::WorkerCommand> const& command) {
    _workerCommandQueue->queCmd(command);
}

nlohmann::json Foreman::statusToJson() {
    nlohmann::json status;
    status["queries"] = _queries->statusToJson();
    status["sql_conn_mgr"] = _sqlConnMgr->statusToJson();
    return status;
}

}  // namespace lsst::qserv::wcontrol
