
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

// Class header
#include "replica/DatabaseMySQLUtils.h"

// Qserv headers
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseMySQLExceptions.h"

// System headers
#include <stdexcept>

using namespace std;

namespace lsst::qserv::replica::database::mysql::detail {

bool selectSingleValueImpl(shared_ptr<Connection> const& conn, string const& query,
                           function<bool(Row&)> const& onEachRow, bool noMoreThanOne) {
    string const context = "DatabaseMySQLUtils::" + string(__func__) + " ";
    conn->execute(query);
    if (!conn->hasResult()) {
        throw logic_error(context + "wrong query type - the query doesn't have any result set.");
    }
    bool isNotNull = false;
    size_t numRows = 0;
    Row row;
    while (conn->next(row)) {
        // Only the very first row matters
        if (numRows == 0) isNotNull = onEachRow(row);
        // Have to read the rest of the result set to avoid problems with
        // the MySQL protocol
        ++numRows;
    }
    switch (numRows) {
        case 0:
            throw EmptyResultSetError(context + "result set is empty.");
        case 1:
            return isNotNull;
        default:
            if (!noMoreThanOne) return isNotNull;
    }
    throw logic_error(context + "result set has more than 1 row");
}

}  // namespace lsst::qserv::replica::database::mysql::detail
