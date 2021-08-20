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
#ifndef LSST_QSERV_QCACHE_MYSQLCONNECTION_H
#define LSST_QSERV_QCACHE_MYSQLCONNECTION_H

// System headers
#include <memory>
#include <string>

// This header declarations
namespace lsst {
namespace qserv {
namespace qcache {

/**
  * Class MySQLConnection is a mock-up implementation of the MySQL connection
  * manager.
  */
class MySQLConnection: public std::enable_shared_from_this<MySQLConnection> {
public:
    MySQLConnection() = delete;
    MySQLConnection(MySQLConnection const&) = delete;
    MySQLConnection& operator=(MySQLConnection const&) = delete;

    virtual ~MySQLConnection() = default;

    /// @return A new connection object.
    static std::shared_ptr<MySQLConnection> create();

    /// Execute a query
    void execute(std::string const& query);

private:
    MySQLConnection();
};

}}} // namespace lsst::qserv::qcache

#endif // LSST_QSERV_QCACHE_MYSQLCONNECTION_H
