// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#ifndef LSST_QSERV_CONSTANTS_H
#define LSST_QSERV_CONSTANTS_H
 /**
  * @file testCppParser.cc
  *
  * @brief  Constants. Consider moving to qserv/common in a future ticket.
  *
  * @author Daniel L. Wang, SLAC
  */ 

namespace lsst {
namespace qserv {
// FIXME: should be constant somewhere else. See #2405
const char CHUNK_COLUMN[] = "chunkId";
const char SUB_CHUNK_COLUMN[] = "subChunkId";

}}  
#endif // LSST_QSERV_CONSTANTS_H