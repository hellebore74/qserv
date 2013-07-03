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
/**
  * @file QueryMapping.cc
  *
  * @brief Implementation of QueryMapping. Local implementations of:
  * class MapTuple
  * class Mapping : public QueryTemplate::EntryMapping
  *
  * @author Daniel L. Wang, SLAC
  */
#include "lsst/qserv/master/QueryMapping.h"

#include <deque>
#include <sstream>

#include <boost/regex.hpp>
#include <boost/lexical_cast.hpp>

#include "lsst/qserv/master/ChunkSpec.h"
#include "lsst/qserv/master/QueryTemplate.h"

namespace lsst { 
namespace qserv { 
namespace master { 

class MapTuple {
public:
    MapTuple(std::string const& pat, 
             std::string const target,
             QueryMapping::Parameter p) 
        : reg(pat), tgt(target), param(p) {}
    boost::regex reg;
    std::string tgt;
    QueryMapping::Parameter param;
};

class Mapping : public QueryTemplate::EntryMapping {
public:
    typedef std::deque<int> IntDeque;
    typedef std::deque<MapTuple> Map;

    Mapping(QueryMapping::ParameterMap const& m, ChunkSpec const& s) 
        : _subChunks(s.subChunks.begin(), s.subChunks.end()) {
        _chunkString = boost::lexical_cast<std::string>(s.chunkId);
        if(!_subChunks.empty()) {
            _subChunkString = boost::lexical_cast<std::string>(_subChunks.front());
        }
        _initMap(m);
    }
    Mapping(QueryMapping::ParameterMap const& m, ChunkSpecSingle const& s) {
        _chunkString = boost::lexical_cast<std::string>(s.chunkId);
        _subChunkString = boost::lexical_cast<std::string>(s.subChunkId);
        _subChunks.push_back(s.subChunkId);
        _initMap(m);
    }
    virtual ~Mapping() {}
    virtual boost::shared_ptr<QueryTemplate::Entry> mapEntry(QueryTemplate::Entry const& e) const {
        typedef QueryTemplate::StringEntry StringEntry;
        boost::shared_ptr<StringEntry> newE(new StringEntry(e.getValue()));
        Map::const_iterator i;

        // FIXME see if this works
        //if(!e.isDynamic()) {return newE; }

        for(i=_map.begin(); i != _map.end(); ++i) {
            newE->s = boost::regex_replace(newE->s, i->reg, i->tgt);
            if(i->param == QueryMapping::SUBCHUNK) {
                // Remember that we mapped a subchunk,
                    // so we know to iterate over subchunks.
                    // Or... the plugins could signal that subchunks
                    // are needed somehow. FIXME.
            }            
        }
        return newE;
    } 
    bool valid() const {
        return _subChunkString.empty() 
            || (!_subChunkString.empty() && !_subChunks.empty());
    }
private:
    inline void _initMap(QueryMapping::ParameterMap const& m) {
        QueryMapping::ParameterMap::const_iterator i;
        for(i = m.begin(); i != m.end(); ++i) {
            _map.push_back(MapTuple(i->first, lookup(i->second), i->second));
        }
    }

    inline std::string lookup(QueryMapping::Parameter const& p) const {
        switch(p) {
        case QueryMapping::INVALID:
            return "INVALID";
        case QueryMapping::CHUNK:
            return _chunkString;
        case QueryMapping::SUBCHUNK:
            return _subChunkString;
        case QueryMapping::HTM1:
            throw std::range_error("HTM unimplemented");
        default:
            throw std::range_error("Unknown mapping parameter");
        }
    }
    void _nextSubChunk() {
        _subChunks.pop_front();
        if(_subChunks.empty()) return;
        _subChunkString = boost::lexical_cast<std::string>(_subChunks.front());
    }
        
    std::string _chunkString;
    std::string _subChunkString;
    IntDeque _subChunks;
    Map _map;

};

////////////////////////////////////////////////////////////////////////
// class QueryMapping implementation
////////////////////////////////////////////////////////////////////////
QueryMapping::QueryMapping() {}

std::string 
QueryMapping::apply(ChunkSpec const& s, QueryTemplate const& t) const {
    Mapping m(_subs, s);
    return t.generate(m);
}
std::string 
QueryMapping::apply(ChunkSpecSingle const& s, QueryTemplate const& t) const {
    Mapping m(_subs, s);
    return t.generate(m);
}

void 
QueryMapping::update(QueryMapping const& m) {
    // Update this mapping to reflect the union of the two mappings.
    // We manually merge so that we have a chance to detect conflicts.
    ParameterMap::const_iterator i;
    for(i=m._subs.begin(); i != m._subs.end(); ++i) {
        ParameterMap::const_iterator f = _subs.find(i->first);
        if(f != _subs.end()) {
            if(f->second != i->second) {
                throw std::logic_error("Conflict  during update in QueryMapping");
                // Not sure what to do.
                // This is a big parse error, or a flaw in parsing logic.
            }
        } else {
            _subs.insert(*i);
        }
    }
    _subChunkTables.insert(m._subChunkTables.begin(), m._subChunkTables.end());
}

bool 
QueryMapping::hasParameter(Parameter p) const {
    ParameterMap::const_iterator i;
    for(i=_subs.begin(); i != _subs.end(); ++i) {
        if(i->second == p) return true;
    }
    return false;
}

}}} // namespace lsst::qserv::master