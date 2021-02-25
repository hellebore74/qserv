// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 LSST Corporation.
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
#ifndef LSST_QSERV_WSCHED_CHUNKDISK_H
#define LSST_QSERV_WSCHED_CHUNKDISK_H
 /**
  * @file
  *
  * @brief ChunkDisk is a resource that queues tasks for chunks on a disk.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <algorithm>
#include <memory>
#include <mutex>
#include <vector>

// Qserv headers
#include "memman/MemMan.h"
#include "proto/worker.pb.h"
#include "wbase/Task.h"
#include "wsched/ChunkTaskCollection.h"

namespace lsst {
namespace qserv {
namespace wsched {

/// Limits Tasks to running when resources are available.
class ChunkDisk : public ChunkTaskCollection {
public:

    ChunkDisk(memman::MemMan::Ptr const& memMan) : _memMan{memMan} {}
    ChunkDisk(ChunkDisk const&) = delete;
    ChunkDisk& operator=(ChunkDisk const&) = delete;
    virtual ~ChunkDisk() {};

    // Queue management
    void queueTask(wbase::Task::Ptr const& a) override;
    void queueTask(std::vector<wbase::Task::Ptr> const& tasks) override;
    wbase::Task::Ptr getTask(bool useFlexibleLock) override;
    bool empty() const override;
    bool ready(bool useFlexibleLock) override;
    std::size_t getSize() const override;
    void taskComplete(wbase::Task::Ptr const& task) override {};

    bool setResourceStarved(bool starved) override;
    bool nextTaskDifferentChunkId() override;

    /// ChunkDisk version does nothing for now.
    /// TODO: Make a legitimate removeTask function or delete the ChunkDisk class.
    wbase::Task::Ptr removeTask(wbase::Task::Ptr const& task) override {return nullptr;}

    /// Class that keeps the minimum chunkId at the front of the heap
    /// and within that chunkId, start with the slowest tables to scan.
    class MinHeap {
    public:
        // Using a greater than comparison function results in a minimum value heap.
        static bool compareFunc(wbase::Task::Ptr const& x, wbase::Task::Ptr const& y) {
                if(!x || !y) { return false; }
                if (x->getChunkId() > y->getChunkId()) return true;
                if (x->getChunkId() < y->getChunkId()) return false;
                // chunkId's must be equal, compare scanInfo (slower scans first)
                int siComp = x->getScanInfo().compareTables(y->getScanInfo());
                return siComp < 0;
        };
        void push(wbase::Task::Ptr const& task);
        wbase::Task::Ptr pop();
        wbase::Task::Ptr top() {
            if (_tasks.empty()) return nullptr;
            return _tasks.front();
        }
        bool empty() const { return _tasks.empty(); }
        void heapify() {
            std::make_heap(_tasks.begin(), _tasks.end(), compareFunc);
        }

        std::vector<wbase::Task::Ptr> _tasks;
    };

private:
    bool _empty() const;
    bool _ready(bool useFlexibleLock);

    mutable std::mutex _queueMutex;
    MinHeap _activeTasks;
    MinHeap _pendingTasks;
    int _lastChunk{-100}; // initialize to impossibly small value;
    memman::MemMan::Ptr _memMan;
    mutable std::mutex _inflightMutex;
    bool _resourceStarved{false};
};

}}} // namespace

#endif // LSST_QSERV_WSCHED_CHUNKDISK_H
