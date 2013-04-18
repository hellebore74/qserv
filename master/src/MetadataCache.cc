// -*- LSST-C++ -*-

/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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

/**
  * @file MetadataCache.cc
  *
  * @brief Transient metadata structure for qserv. 
  *
  * @Author Jacek Becla, SLAC
  */

#include "lsst/qserv/master/MetadataCache.h"

namespace qMaster = lsst::qserv::master;

/** Constructs object representing a non-partitioned database.
  */
qMaster::MetadataCache::DbInfo::DbInfo() :
    _isPartitioned(false),
    _nStripes(-1),
    _nSubStripes(-1),
    _defOverlapF(-1),
    _defOverlapNN(-1) {
}

/** Constructs object representing a partitioned database
  * which use spherical partitioning mode.
  *
  * @param nStripes number of stripes
  * @param nSubStripes number of sub-stripes
  * @param defOverlapF default overlap for 'fuzziness'
  * @param defOverlapNN default overlap for 'near-neighbor'-type queries
  */
qMaster::MetadataCache::DbInfo::DbInfo(int nStripes, int nSubStripes,
                                       float defOverlapF, float defOverlapNN) :
    _isPartitioned(true),
    _nStripes(nStripes),
    _nSubStripes(nSubStripes),
    _defOverlapF(defOverlapF),
    _defOverlapNN(defOverlapNN) {
}

/** Adds information about a non-partitioned table.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns status (0 on success)
  */
int
qMaster::MetadataCache::DbInfo::addTable(std::string const& tbName, const TableInfo& tbInfo) {
    std::map<std::string, TableInfo>::const_iterator itr = _tables.find(tbName);
    if (itr != _tables.end()) {
        return MetadataCache::STATUS_ERR_TABLE_EXISTS;
    }
    _tables.insert(std::pair<std::string, TableInfo> (tbName, tbInfo));
    return MetadataCache::STATUS_OK;
}

/** Checks if a given table is registered in the qserv metadata.
  *
  * @param tableName table name
  *
  * @return returns true or false
  */
bool
qMaster::MetadataCache::DbInfo::checkIfContainsTable(std::string const& tableName) const {
    std::map<std::string, TableInfo>::const_iterator itr = _tables.find(tableName);
    return itr != _tables.end();
}

/** Constructs object representing a non-partitioned table.
  */
qMaster::MetadataCache::TableInfo::TableInfo() :
    _isPartitioned(false),
    _overlap(-1),
    _phiCol("invalid"),
    _thetaCol("invalid"),
    _phiColNo(-1),
    _thetaColNo(-1),
    _logicalPart(-1),
    _physChunking(-1) {
}

/** Constructs object representing a partitioned table
  * which use spherical partitioning mode.
  *
  * @param overlap used for this table (overwrites overlaps from dbInfo)
  * @param phiCol name of the phi col (right ascention)
  * @param thetaCol name of the theta col (declination)
  * @param phiColNo position of the phi col in the table, counting from zero
  * @param thetaColNo position of the theta col in the table, counting from zero
  * @param logicalPart definition how the table is partitioned logically
  * @param physChunking definition how the table is chunked physically
  */
qMaster::MetadataCache::TableInfo::TableInfo(float overlap, 
                                             std::string const& phiCol,
                                             std::string const& thetaCol,
                                             int phiColNo,
                                             int thetaColNo,
                                             int logicalPart,
                                             int physChunking) :
    _isPartitioned(true),
    _overlap(overlap),
    _phiCol(phiCol),
    _thetaCol(thetaCol),
    _phiColNo(phiColNo),
    _thetaColNo(thetaColNo),
    _logicalPart(logicalPart),
    _physChunking(physChunking) {
}

/** Adds database information for a non-partitioned database.
  *
  * @param dbName database name
  *
  * @return returns status (0 on success)
  */
int
qMaster::MetadataCache::addDbInfoNonPartitioned(std::string const& dbName) {
    if (checkIfContainsDb(dbName)) {
        return MetadataCache::STATUS_ERR_DB_EXISTS;
    }
    boost::lock_guard<boost::mutex> m(_mutex);
    _dbs.insert(std::pair<std::string, DbInfo> (dbName, DbInfo()));
    return MetadataCache::STATUS_OK;
}

/** Adds database information for a partitioned database,
  * which use spherical partitioning mode.
  *
  * @param dbName database name
  * @param nStripes number of stripes
  * @param nSubStripes number of sub-stripes
  * @param defOverlapF default overlap for 'fuzziness'
  * @param defOverlapNN default overlap for 'near-neighbor'-type queries
  *
  * @return returns status (0 on success)
  */
int
qMaster::MetadataCache::addDbInfoPartitionedSphBox(std::string const& dbName,
                                                   int nStripes,
                                                   int nSubStripes,
                                                   float defOverlapF,
                                                   float defOverlapNN) {
    if (checkIfContainsDb(dbName)) {
        return MetadataCache::STATUS_ERR_DB_EXISTS;
    }
    DbInfo dbInfo(nStripes, nSubStripes, defOverlapF, defOverlapNN);
    boost::lock_guard<boost::mutex> m(_mutex);
    _dbs.insert(std::pair<std::string, DbInfo> (dbName, dbInfo));
    return MetadataCache::STATUS_OK;
}

/** Adds table information for a non-partitioned table.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns status (0 on success)
  */
int
qMaster::MetadataCache::addTbInfoNonPartitioned(std::string const& dbName,
                                                std::string const& tbName) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        return MetadataCache::STATUS_ERR_DB_DOES_NOT_EXIST;
    }
    const qMaster::MetadataCache::TableInfo tInfo;
    return itr->second.addTable(tbName, tInfo);
}

/** Adds database information for a partitioned table,
  * which use spherical partitioning mode.
  *
  * @param dbName database name
  * @param tableName table name
  * @param overlap used for this table (overwrites overlaps from dbInfo)
  * @param phiCol name of the phi col (right ascention)
  * @param thetaCol name of the theta col (declination)
  * @param phiColNo position of the phi col in the table, counting from zero
  * @param thetaColNo position of the theta col in the table, counting from zero
  * @param logicalPart definition how the table is partitioned logically
  * @param physChunking definition how the table is chunked physically
  *
  * @return returns status (0 on success)
  */
int
qMaster::MetadataCache::addTbInfoPartitionedSphBox(std::string const& dbName, 
                                                   std::string const& tbName,
                                                   float overlap, 
                                                   std::string const& phiCol, 
                                                   std::string const& thetaCol, 
                                                   int phiColNo, 
                                                   int thetaColNo, 
                                                   int logicalPart, 
                                                   int physChunking) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        return MetadataCache::STATUS_ERR_DB_DOES_NOT_EXIST;
    }
    const qMaster::MetadataCache::TableInfo tInfo(
                          overlap, phiCol, thetaCol, phiColNo, 
                          thetaColNo, logicalPart, physChunking);
    return itr->second.addTable(tbName, tInfo);
}

/** Checks if a given database is registered in the qserv metadata.
  *
  * @param dbName database name
  *
  * @return returns true or false
  */
bool
qMaster::MetadataCache::checkIfContainsDb(std::string const& dbName) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::const_iterator itr = _dbs.find(dbName);
    return itr != _dbs.end();
}

/** Checks if a given table is registered in the qserv metadata.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns true or false
  */
bool
qMaster::MetadataCache::checkIfContainsTable(std::string const& dbName,
                                             std::string const& tableName) {
    boost::lock_guard<boost::mutex> m(_mutex);
    std::map<std::string, DbInfo>::const_iterator itr = _dbs.find(dbName);
    if (itr == _dbs.end()) {
        return false;
    }
    return itr->second.checkIfContainsTable(tableName);
}

/** Prints the contents of the qserv metadata cache. This is
  * handy for debugging.
  */
void
qMaster::MetadataCache::printSelf() {
    std::cout << "\n\nMetadata Cache in C++:" << std::endl;
    std::map<std::string, DbInfo>::const_iterator itr;
    boost::lock_guard<boost::mutex> m(_mutex);
    for (itr=_dbs.begin() ; itr!= _dbs.end() ; ++itr) {
        std::cout << "db: " << itr->first << ": " << itr->second << "\n";
    }
    std::cout << std::endl;
}

/** Operator<< for printing DbInfo object
  *
  * @param s the output stream
  * @param dbInfo DbInfo object
  *
  * @return returns the output stream
  */
std::ostream &
qMaster::operator<<(std::ostream &s, const qMaster::MetadataCache::DbInfo &dbInfo) {
    if (dbInfo.getIsPartitioned()) {
        s << "is partitioned (nStripes=" << dbInfo.getNStripes()
          << ", nSubStripes=" << dbInfo.getNSubStripes()
          << ", defOvF=" << dbInfo.getDefOverlapF()
          << ", defOvNN=" << dbInfo.getDefOverlapNN() << ").\n";
    } else {
        s << "is not partitioned.\n";
    }
    s << "  Tables:";
    std::map<std::string, qMaster::MetadataCache::TableInfo>::const_iterator itr;
    for (itr=dbInfo._tables.begin() ; itr!= dbInfo._tables.end(); ++itr) {
        s << "   " << itr->first << ": " << itr->second << "\n";
    }
    return s;
}

/** Operator<< for printing TableInfo object
  *
  * @param s the output stream
  * @param tableInfo TableInfo object
  *
  * @return returns the output stream
  */
std::ostream &
qMaster::operator<<(std::ostream &s, const qMaster::MetadataCache::TableInfo &tableInfo) {
    if (tableInfo.getIsPartitioned()) {
        s << "is partitioned (overlap=" << tableInfo.getOverlap()
          << ", phiCol=" << tableInfo.getPhiCol()
          << ", thetaCol=" << tableInfo.getThetaCol()
          << ", phiColNo=" << tableInfo.getPhiColNo()
          << ", thetaColNo=" << tableInfo.getThetaColNo()
          << ", logPart=" << tableInfo.getLogicalPart()
          << ", physChunking=" << tableInfo.getPhysChunking() << ").\n";
    } else {
        s << "is not partitioned.\n";
    }
    return s;
}