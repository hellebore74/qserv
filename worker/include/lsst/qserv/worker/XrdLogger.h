/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
#ifndef LSST_QSERV_WORKER_XRDLOGGER_H
#define LSST_QSERV_WORKER_XRDLOGGER_H
#include "lsst/qserv/worker/Base.h"
#include "XrdSys/XrdSysError.hh"

namespace lsst {
namespace qserv {
namespace worker {

class XrdLogger : public Logger {
public:
    explicit XrdLogger(XrdSysError& e) : _e(e) {}
    virtual void operator()(std::string const& s) {
        _e.Say(s.c_str());
    }
    virtual void operator()(char const* s) {
        _e.Say(s);
    }

private:
    XrdSysError& _e;
};

}}} // lsst::qserv:worker
#endif // LSST_QSERV_WORKER_XRDLOGGER_H
