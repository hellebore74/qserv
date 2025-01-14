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
#ifndef LSST_QSERV_REPLICA_CHUNKLOCKER_H
#define LSST_QSERV_REPLICA_CHUNKLOCKER_H

// System headers
#include <list>
#include <map>
#include <ostream>
#include <string>

// Qserv headers
#include "replica/Mutex.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Structure Chunk is an abstraction grouping together database families and
 * chunk numbers. This is needed to support chunk replication operations which
 * require chunk collocation.
 */
struct Chunk {
    std::string databaseFamily;
    unsigned int number;

    /**
     * The overloaded operator for comparing objects of structure Chunk.
     * The operator is used where the equality of chunks is needed.
     *
     * @return
     *   'true' if the chunk is 'equal' to the other one.
     */
    bool operator==(Chunk const& rhs) const;

    /**
     * The overloaded operator for comparing objects of structure Chunk.
     *
     * This operator is needed for using objects of the structure
     * as a key in ordered (map, set, ) or unordered (unordered_map,unordered_set,)
     * associative containers.
     *
     * @return
     *   'true' if the chunk is 'less' than the other one.
     */
    bool operator<(Chunk const& rhs) const;
};

/// The overloaded operator for dumping objects of type Chunk
std::ostream& operator<<(std::ostream& os, Chunk const& chunk);

/**
 * Class ChunkLocker provides a thread-safe mechanism allowing
 * owners (represented by unique string-based identifiers) to claim
 * exclusive 'locks' (ownership claims) on chunks.
 */
class ChunkLocker {
public:
    /// The type for a collection of locked chunks grouped by owners
    typedef std::map<std::string, std::list<Chunk>> OwnerToChunks;

    /// A map of chunks to their owners
    typedef std::map<Chunk, std::string> ChunkToOwner;

    /// The default constructor
    ChunkLocker() = default;

    // The copy semantics is prohibited

    ChunkLocker(ChunkLocker const&) = delete;
    ChunkLocker& operator=(ChunkLocker const&) = delete;

    ~ChunkLocker() = default;

    /**
     * @param chunk
     *   a chunk to be tested
     *
     * @return
     *   'true' if a chunk is locked
     */
    bool isLocked(Chunk const& chunk) const;

    /**
     * @param chunk
     *   a chunk to be tested
     *
     * @param owner
     *   a reference to a string which will be initialized with
     *   an identifier of an owner of the chunk if the chunk is found
     *   locked
     *
     * @return
     *   'true' if the chunk is locked and set an identifier of
     *   an owner which locked the chunk.
     */
    bool isLocked(Chunk const& chunk, std::string& ownerId) const;

    /**
     * Find chunks which are locked by a particular owner (if provided),
     * or by all owners.
     *
     * @param owner
     *   an optional owner. If the owner is not provided then
     *   all chunks will be returned
     *
     * @return
     *   a collection of chunks grouped by owners
     */
    OwnerToChunks locked(std::string const& owner = std::string()) const;

    /**
     * Lock a chunk to a specific owner
     *
     * @param chunk
     *   a chunk to be locked
     *
     * @param owner
     *   an identifier of an owner claiming the chunk
     *
     * @return
     *   'true' of the operation was successful or if the specified
     *   owner already owns it, or 'false' if there is outstanding lock on
     *   a chunk made earlier by another owner.
     *
     * @throw std::invalid_argument
     *   if the owner 'id' is an empty string
     */
    bool lock(Chunk const& chunk, std::string const& owner);

    /**
     * Release a chunk regardless of its owner
     *
     * @param chunk
     *   a chunk to be released
     *
     * @return
     *   'true' if the operation was successful
     */
    bool release(Chunk const& chunk);

    /**
     * Release a chunk and, if successful, set an identifier of an owner which
     * previously 'claimed' the chunk.
     *
     * @param chunk
     *   chunk to be released
     *
     * @param owner
     *   reference to a string which will be initialized with
     *   an identifier of an owner which had a claim on the chunk
     *   at a time of the method call
     *
     * @return
     *   'true' if the operation was successful
     */
    bool release(Chunk const& chunk, std::string& owner);

    /**
     * Release all chunks which were found claimed by the specified owner
     * and return a collection of those chunks
     *
     * @throw std::invalid_argument - if the owner is an empty string
     */
    std::list<Chunk> release(std::string const& owner);

private:
    /**
     * Find chunks which are locked by a particular owner (if provided),
     * or by all owners.
     *
     * @param mLock
     *   a lock on ChunkLocker::_mtx must be made before calling this method
     *
     * @param owner
     *   an optional owner. If the owner is not provided then
     *   all chunks will be returned.
     *
     * @owner2chunks
     *   collection of chunks to be initialized
     *
     * @return
     *   a collection of chunks grouped by owners
     */
    void _lockedImpl(replica::Lock const& mLock, std::string const& owner, OwnerToChunks& owner2chunks) const;

    /**
     * Actual implementation of the chunk release operation, which will attempt
     * to release a chunk and, if successful, set an identifier of an owner which
     * previously 'claimed' the chunk.
     *
     * @note
     *   This method is not thread-safe. It's up to its callers to ensure
     *   proper synchronization context before invoking the method.
     *
     * @param mLock
     *   a lock on ChunkLocker::_mtx must be made before calling this method
     *
     * @param chunk
     *   a chunk to be released
     *
     * @param owner
     *   a reference to a string which will be initialized with an identifier
     *   of an owner which had a claim on the chunk (if if the chunk was locked at
     *   at a time the method was called) after a completion of the method.
     *   if the chunk wasn't locked then the value of the parameter won't change.
     *
     * @return
     *   'true' if the operation was successful
     */
    bool _releaseImpl(replica::Lock const& mLock, Chunk const& chunk, std::string& owner);

    /// Mapping a chunk to its "owner" (the one which holds the lock)
    ChunkToOwner _chunk2owner;

    /// For thread safety where it's required
    mutable replica::Mutex _mtx;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CHUNKLOCKER_H
