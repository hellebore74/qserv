// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

#ifndef LSST_QSERV_UTIL_STRINGHASH_H
#define LSST_QSERV_UTIL_STRINGHASH_H

// System headers
#include <string>

namespace lsst::qserv::util {

/// Small wrappers for computing hashes
class StringHash {
public:
    static std::string getMd5Hex(char const* buffer, int bufferSize);
    static std::string getSha1Hex(char const* buffer, int bufferSize);
    static std::string getSha256Hex(char const* buffer, int bufferSize);
    static std::string getMd5(char const* buffer, int bufferSize);
    static std::string getSha1(char const* buffer, int bufferSize);
    static std::string getSha256(char const* buffer, int bufferSize);
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_STRINGHASH_H
