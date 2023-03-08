// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 AURA/LSST.
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
#include "wconfig/WorkerConfig.h"

// System headers
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/ConfigStoreError.h"
#include "wsched/BlendScheduler.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wconfig.WorkerConfig");

}

namespace lsst::qserv::wconfig {

WorkerConfig::WorkerConfig(const util::ConfigStore& configStore)
        : _memManClass(configStore.get("memman.class", "MemManReal")),
          _memManSizeMb(configStore.getInt("memman.memory", 1000)),
          _memManLocation(configStore.getRequired("memman.location")),
          _threadPoolSize(
                  configStore.getInt("scheduler.thread_pool_size", wsched::BlendScheduler::getMinPoolSize())),
          _maxPoolThreads(configStore.getInt("scheduler.max_pool_threads", 5000)),
          _maxGroupSize(configStore.getInt("scheduler.group_size", 1)),
          _requiredTasksCompleted(configStore.getInt("scheduler.required_tasks_completed", 25)),
          _prioritySlow(configStore.getInt("scheduler.priority_slow", 2)),
          _prioritySnail(configStore.getInt("scheduler.priority_snail", 1)),
          _priorityMed(configStore.getInt("scheduler.priority_med", 3)),
          _priorityFast(configStore.getInt("scheduler.priority_fast", 4)),
          _maxReserveSlow(configStore.getInt("scheduler.reserve_slow", 2)),
          _maxReserveSnail(configStore.getInt("scheduler.reserve_snail", 2)),
          _maxReserveMed(configStore.getInt("scheduler.reserve_med", 2)),
          _maxReserveFast(configStore.getInt("scheduler.reserve_fast", 2)),
          _maxActiveChunksSlow(configStore.getInt("scheduler.maxactivechunks_slow", 2)),
          _maxActiveChunksSnail(configStore.getInt("scheduler.maxactivechunks_snail", 1)),
          _maxActiveChunksMed(configStore.getInt("scheduler.maxactivechunks_med", 4)),
          _maxActiveChunksFast(configStore.getInt("scheduler.maxactivechunks_fast", 4)),
          _scanMaxMinutesFast(configStore.getInt("scheduler.scanmaxminutes_fast", 60)),
          _scanMaxMinutesMed(configStore.getInt("scheduler.scanmaxminutes_med", 60 * 8)),
          _scanMaxMinutesSlow(configStore.getInt("scheduler.scanmaxminutes_slow", 60 * 12)),
          _scanMaxMinutesSnail(configStore.getInt("scheduler.scanmaxminutes_snail", 60 * 24)),
          _maxTasksBootedPerUserQuery(configStore.getInt("scheduler.maxtasksbootedperuserquery", 5)),
          _maxSqlConnections(configStore.getInt("sqlconnections.maxsqlconn", 800)),
          _ReservedInteractiveSqlConnections(
                  configStore.getInt("sqlconnections.reservedinteractivesqlconn", 50)),
          _bufferMaxTotalGB(configStore.getInt("transmit.buffermaxtotalgb", 41)),
          _maxTransmits(configStore.getInt("transmit.maxtransmits", 40)),
          _maxPerQid(configStore.getInt("transmit.maxperqid", 3)),
          _resultsDirname(configStore.get("results.dirname", "/qserv/data/results")) {
    int mysqlPort = configStore.getInt("mysql.port");
    std::string mysqlSocket = configStore.get("mysql.socket");
    if (mysqlPort == 0 && mysqlSocket.empty()) {
        throw std::runtime_error(
                "At least one of mysql.port or mysql.socket is required in the configuration file.");
    }
    _mySqlConfig =
            mysql::MySqlConfig(configStore.getRequired("mysql.username"), configStore.get("mysql.password"),
                               configStore.getRequired("mysql.hostname"), mysqlPort, mysqlSocket,
                               "");  // dbname
}

std::ostream& operator<<(std::ostream& out, WorkerConfig const& workerConfig) {
    out << "MemManClass=" << workerConfig._memManClass;
    if (workerConfig._memManClass == "MemManReal") {
        out << "MemManSizeMb=" << workerConfig._memManSizeMb;
    }
    out << " poolSize=" << workerConfig._threadPoolSize << ", maxGroupSize=" << workerConfig._maxGroupSize;
    out << " requiredTasksCompleted=" << workerConfig._requiredTasksCompleted;

    out << " priority fast=" << workerConfig._priorityFast << " med=" << workerConfig._priorityMed
        << " slow=" << workerConfig._prioritySlow;

    out << " Reserved threads fast=" << workerConfig._maxReserveFast << " med=" << workerConfig._maxReserveMed
        << " slow=" << workerConfig._maxReserveSlow;

    return out;
}

}  // namespace lsst::qserv::wconfig
