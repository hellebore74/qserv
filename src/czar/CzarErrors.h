/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_CZAR_CZARERRORS_H
#define LSST_QSERV_CZAR_CZARERRORS_H

// System headers

// Third-party headers

// Qserv headers
#include "sql/SqlErrorObject.h"
#include "util/Issue.h"

namespace lsst::qserv::czar {

/// Base class for exceptions generated by czar module.
class CzarError : public util::Issue {
public:
    CzarError(util::Issue::Context const& ctx, std::string const& ecxName, std::string const& message)
            : util::Issue(ctx, ecxName + ": " + message) {}
};

/// Exception class for errors in SQL queries.
class SqlError : public CzarError {
public:
    SqlError(util::Issue::Context const& ctx, std::string const& message, sql::SqlErrorObject const& sqlErr)
            : CzarError(ctx, "SqlError", message + ": " + sqlErr.printErrMsg()) {}
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_CZARERRORS_H
