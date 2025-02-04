// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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
#ifndef LSST_QSERV_WPUBLISH_QUERIESANDCHUNKS_H
#define LSST_QSERV_WPUBLISH_QUERIESANDCHUNKS_H

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "wbase/Task.h"

// Forward declarations
namespace lsst::qserv {
namespace wsched {
class SchedulerBase;
class BlendScheduler;
class ScanScheduler;
}  // namespace wsched
namespace wpublish {
class QueriesAndChunks;
}
}  // namespace lsst::qserv

// This header declarations
namespace lsst::qserv::wpublish {

/// Statistics for a single user query.
/// This class stores some statistics for each Task in the user query on this worker.
class QueryStatistics {
public:
    using Ptr = std::shared_ptr<QueryStatistics>;

    using TaskId = std::pair<QueryId, int>;  ///< Unique identifier for task is jobId, fragment number.
    /// For maps of Tasks in a query. It would be nice to use an unordered_map, but a hash need to be defined.
    using TaskMap = std::map<TaskId, wbase::Task::Ptr>;

    void addTask(wbase::Task::Ptr const& task);

    bool isDead(std::chrono::seconds deadTime, TIMEPOINT now);

    int getTasksBooted();
    bool getQueryBooted() { return _queryBooted; }

    /// Add statistics related to the running of the query in the task.
    /// If there are subchunks in the user query, several Tasks may be needed for one chunk.
    /// @param runTimeSeconds - How long it took to run the query.
    /// @param subchunkRunTimeSeconds - How long the query spent waiting for the
    ///                         subchunk temporary tables to be made. It's important to
    ///                         remember that it's very common for several tasks to be waiting
    ///                         on the same subchunk tables at the same time.
    void addTaskRunQuery(double runTimeSeconds, double subchunkRunTimeSeconds);

    /// Add statistics related to transmitting results back to the czar.
    /// If there are subchunks in the user query, several Tasks may be needed for one chunk.
    /// @param timeSeconds - time to transmit data back to the czar for one Task
    /// @param bytesTransmitted - number of bytes transmitted to the czar for one Task.
    /// @param rowsTransmitted - number of rows transmitted to the czar for one Task.
    /// @param bufferFillSecs - time spent filling the buffer from the sql result.
    void addTaskTransmit(double timeSeconds, int64_t bytesTransmitted, int64_t rowsTransmitted,
                         double bufferFillSecs);

    TIMEPOINT const creationTime;
    QueryId const queryId;

    /// Return a json object containing high level data, such as histograms.
    nlohmann::json getJsonHist() const;

    /// Return a json object containing information about all tasks.
    nlohmann::json getJsonTasks() const;

    friend class QueriesAndChunks;
    friend std::ostream& operator<<(std::ostream& os, QueryStatistics const& q);

private:
    explicit QueryStatistics(QueryId const& queryId);
    bool _isMostlyDead() const;

    mutable std::mutex _qStatsMtx;

    std::chrono::system_clock::time_point _touched = std::chrono::system_clock::now();

    int _size = 0;
    int _tasksCompleted = 0;
    int _tasksRunning = 0;
    int _tasksBooted = 0;                   ///< Number of Tasks booted for being too slow.
    std::atomic<bool> _queryBooted{false};  ///< True when the entire query booted.

    double _totalTimeMinutes = 0.0;

    TaskMap _taskMap;  ///< Map of all Tasks for this user query keyed by job id and fragment number.

    util::Histogram::Ptr _histTimeRunningPerTask;  ///< Histogram of SQL query run times.
    util::Histogram::Ptr
            _histTimeSubchunkPerTask;  ///< Histogram of time waiting for temporary table generation.
    util::Histogram::Ptr _histTimeTransmittingPerTask;  ///< Histogram of time spent transmitting.
    util::Histogram::Ptr _histTimeBufferFillPerTask;    ///< Histogram of time filling buffers.
    util::Histogram::Ptr _histSizePerTask;              ///< Histogram of bytes per Task.
    util::Histogram::Ptr _histRowsPerTask;              ///< Histogram of rows per Task.
};

/// Statistics for a table in a chunk. Statistics are based on the slowest table in a query,
/// so this most likely includes values for queries on _scanTableName and queries that join
/// against _scanTableName. Source is slower than Object, so joins with Source and Object will
/// have their statistics logged with Source.
class ChunkTableStats {
public:
    using Ptr = std::shared_ptr<ChunkTableStats>;

    /// Contains statistics data for this table in this chunk.
    struct Data {
        std::uint64_t tasksCompleted = 0;  ///< Number of Tasks that have completed on this chunk/table.
        std::uint64_t tasksBooted = 0;     ///< Number of Tasks that have been booted for taking too long.
        double avgCompletionTime = 0.0;    ///< weighted average of completion time in minutes.
    };

    static std::string makeTableName(std::string const& db, std::string const& table) {
        return db + ":" + table;
    }

    ChunkTableStats(int chunkId, std::string const& name) : _chunkId{chunkId}, _scanTableName{name} {}

    void addTaskFinished(double minutes);

    /// @return a copy of the statics data.
    Data getData() {
        std::lock_guard<std::mutex> g(_dataMtx);
        return _data;
    }

    friend std::ostream& operator<<(std::ostream& os, ChunkTableStats const& cts);

private:
    mutable std::mutex _dataMtx;  ///< Protects _data.
    int const _chunkId;
    std::string const _scanTableName;

    Data _data;                                   ///< Statistics for this table in this chunk.
    double _weightAvg = 49.0;                     ///< weight of previous average
    double _weightNew = 1.0;                      ///< weight of new measurement
    double _weightSum = _weightAvg + _weightNew;  ///< denominator
};

/// Statistics for one chunk, including scan table statistics.
class ChunkStatistics {
public:
    using Ptr = std::shared_ptr<ChunkStatistics>;

    ChunkStatistics(int chunkId) : _chunkId{chunkId} {}

    ChunkTableStats::Ptr add(std::string const& scanTableName, double duration);
    ChunkTableStats::Ptr getStats(std::string const& scanTableName) const;

    friend QueriesAndChunks;
    friend std::ostream& operator<<(std::ostream& os, ChunkStatistics const& cs);

private:
    int const _chunkId;
    mutable std::mutex _tStatsMtx;  ///< protects _tableStats;
    /// Map of chunk scan table statistics indexed by slowest scan table name in query.
    std::map<std::string, ChunkTableStats::Ptr> _tableStats;
};

class QueriesAndChunks {
public:
    using Ptr = std::shared_ptr<QueriesAndChunks>;

    /// Setup the global instance and return a pointer to it.
    /// @param deadAfter - consider a user query to be dead after this number of seconds.
    /// @param examineAfter - examine all know tasks after this much time has passed since the last
    ///                 examineAll() call
    /// @param maxTasksBooted - after this many tasks have been booted, the query should be
    ///                 moved to the snail scheduler.
    /// @param resetForTesting - set this to true ONLY if the class needs to be reset for unit testing.
    static Ptr setupGlobal(std::chrono::seconds deadAfter, std::chrono::seconds examineAfter,
                           int maxTasksBooted, bool resetForTesting = false);

    /// Return the pointer to the global object.
    /// @param noThrow - if true, this will not throw an exception when called and
    ///                  the function can return nullptr. This should only be true in unit testing.
    /// @throws - throws util::Bug if setupGlobal() has not been called before this.
    static Ptr get(bool noThrow = false);

    virtual ~QueriesAndChunks();

    void setBlendScheduler(std::shared_ptr<wsched::BlendScheduler> const& blendsched);
    void setRequiredTasksCompleted(unsigned int value);

    std::vector<wbase::Task::Ptr> removeQueryFrom(QueryId const& qId,
                                                  std::shared_ptr<wsched::SchedulerBase> const& sched);
    void removeDead();
    void removeDead(QueryStatistics::Ptr const& queryStats);

    QueryStatistics::Ptr getStats(QueryId const& qId) const;

    void addTask(wbase::Task::Ptr const& task);
    void queuedTask(wbase::Task::Ptr const& task);
    void startedTask(wbase::Task::Ptr const& task);
    void finishedTask(wbase::Task::Ptr const& task);

    void examineAll();

    /// @return a JSON representation of the object's status for the monitoring
    nlohmann::json statusToJson();

    // Figure out each chunkTable's percentage of time.
    // Store average time for a task to run on this table for this chunk.
    struct ChunkTimePercent {
        double shardTime = 0.0;
        double percent = 0.0;
        bool valid = false;
    };
    // Store the time to scan entire table with time for each chunk within that table.
    struct ScanTableSums {
        double totalTime = 0.0;
        std::map<int, ChunkTimePercent> chunkPercentages;
    };
    using ScanTableSumsMap = std::map<std::string, ScanTableSums>;

    friend std::ostream& operator<<(std::ostream& os, QueriesAndChunks const& qc);

private:
    static Ptr _globalQueriesAndChunks;
    QueriesAndChunks(std::chrono::seconds deadAfter, std::chrono::seconds examineAfter, int maxTasksBooted);

    void _bootTask(QueryStatistics::Ptr const& uq, wbase::Task::Ptr const& task,
                   std::shared_ptr<wsched::SchedulerBase> const& sched);
    ScanTableSumsMap _calcScanTableSums();
    void _finishedTaskForChunk(wbase::Task::Ptr const& task, double minutes);

    mutable std::mutex _queryStatsMtx;                    ///< protects _queryStats;
    std::map<QueryId, QueryStatistics::Ptr> _queryStats;  ///< Map of Query stats indexed by QueryId.

    mutable std::mutex _chunkMtx;
    std::map<int, ChunkStatistics::Ptr> _chunkStats;  ///< Map of Chunk stats indexed by chunk id.

    std::weak_ptr<wsched::BlendScheduler> _blendSched;  ///< Pointer to the BlendScheduler.

    // Query removal thread members. A user query is dead if all its tasks are complete and it hasn't
    // been touched for a period of time.
    std::thread _removalThread;
    std::atomic<bool> _loopRemoval{true};  ///< While true, check to see if any Queries can be removed.
    /// A user query must be complete and inactive this long before it can be considered dead.
    std::chrono::seconds _deadAfter = std::chrono::minutes(5);

    std::mutex _deadMtx;       ///< Protects _deadQueries.
    std::mutex _newlyDeadMtx;  ///< Protects _newlyDeadQueries.
    using DeadQueriesType = std::map<QueryId, QueryStatistics::Ptr>;
    DeadQueriesType _deadQueries;  ///< Map of user queries that might be dead.
    std::shared_ptr<DeadQueriesType> _newlyDeadQueries{new DeadQueriesType()};

    // Members for running a separate thread to examine all the running Tasks on the scan schedulers
    // and remove those that are taking too long (boot them). If too many Tasks in a single user query
    // take too long, move all remaining task to the snail scan.
    // Booted Tasks are removed from he scheduler they were on but the Tasks should complete. Booting
    // them allows the scheduler to move onto other queries.
    std::thread _examineThread;
    std::atomic<bool> _loopExamine{true};
    std::chrono::seconds _examineAfter = std::chrono::minutes(5);

    /// Maximum number of tasks that can be booted until entire UserQuery is put on snailScan.
    int _maxTasksBooted = 25;

    /// Number of completed Tasks needed before ChunkTableStats::_avgCompletionTime can be
    /// considered valid enough to boot a Task.
    unsigned int _requiredTasksCompleted = 50;
};

}  // namespace lsst::qserv::wpublish

#endif  // LSST_QSERV_WPUBLISH_QUERIESANDCHUNKS_H
