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
#ifndef LSST_QSERV_REPLICA_DATABASETESTAPP_H
#define LSST_QSERV_REPLICA_DATABASETESTAPP_H

// Qserv headers
#include "replica/Application.h"
#include "replica/Common.h"

// Forward declaratons
namespace lsst::qserv::replica {
class TableRowStats;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class DatabaseTestApp implements a tool for testing the DatabaseServices API
 * used by the Replication system implementation.
 */
class DatabaseTestApp : public Application {
public:
    typedef std::shared_ptr<DatabaseTestApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc the number of command-line arguments
     * @param argv the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    DatabaseTestApp() = delete;
    DatabaseTestApp(DatabaseTestApp const&) = delete;
    DatabaseTestApp& operator=(DatabaseTestApp const&) = delete;

    ~DatabaseTestApp() override = default;

protected:
    int runImpl() final;

private:
    DatabaseTestApp(int argc, char* argv[]);

    void _dump(TableRowStats const& stats);

    /// The name of a test
    std::string _operation;

    /// The maximum number of replicas to be returned
    size_t _maxReplicas = 1;

    /// Limit a scope of an operation to workers which are presently enabled in
    /// the Replication system.
    bool _enabledWorkersOnly = false;

    unsigned int _chunk = 0;
    unsigned int _chunk1 = 0;
    unsigned int _chunk2 = 0;

    std::string _workerName;
    std::string _databaseName;
    std::string _databaseFamilyName;
    std::string _tableName;

    TransactionId _transactionId = 0;

    /// Report all databases regardless if they're PUBLISHED or not
    bool _allDatabases = false;

    /// Report a subset of PUBLISHED databases only
    bool _isPublished = false;

    /// The number of rows in the tables (0 means no pages)
    size_t _pageSize = 0;

    // Display vertical separator.
    bool _verticalSeparator = false;
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_DATABASETESTAPP_H */
