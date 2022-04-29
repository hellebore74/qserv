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
#include "replica/ConfigWorker.h"

// System headers
#include <iostream>
#include <stdexcept>
#include <tuple>

using namespace std;
using json = nlohmann::json;

// Template functions for filling worker attributes from JSON.

namespace {
template <typename T>
void parseRequired(T& dest, json const& obj, string const& attr) {
    dest = obj.at(attr).get<T>();
}

template <>
void parseRequired<bool>(bool& dest, json const& obj, string const& attr) {
    dest = obj.at(attr).get<int>() != 0;
}

template <typename T>
void parseOptional(T& dest, json const& obj, string const& attr) {
    if (auto const itr = obj.find(attr); itr != obj.end()) dest = itr->get<T>();
}
}  // namespace

namespace lsst { namespace qserv { namespace replica {

WorkerInfo::WorkerInfo(json const& obj) {
    string const context = "WorkerInfo::WorkerInfo(json): ";
    if (obj.empty()) return;
    if (!obj.is_object()) {
        throw invalid_argument(context + "a JSON object is required.");
    }
    try {
        parseRequired<string>(name, obj, "name");
        parseRequired<bool>(isEnabled, obj, "is-enabled");
        parseRequired<bool>(isReadOnly, obj, "is-read-only");
        parseOptional<string>(svcHost, obj, "svc-host");
        parseOptional<uint16_t>(svcPort, obj, "svc-port");
        parseOptional<string>(fsHost, obj, "fs-host");
        parseOptional<uint16_t>(fsPort, obj, "fs-port");
        parseOptional<string>(dataDir, obj, "data-dir");
        parseOptional<string>(loaderHost, obj, "loader-host");
        parseOptional<uint16_t>(loaderPort, obj, "loader-port");
        parseOptional<string>(loaderTmpDir, obj, "loader-tmp-dir");
        parseOptional<string>(exporterHost, obj, "exporter-host");
        parseOptional<uint16_t>(exporterPort, obj, "exporter-port");
        parseOptional<string>(exporterTmpDir, obj, "exporter-tmp-dir");
        parseOptional<string>(httpLoaderHost, obj, "http-loader-host");
        parseOptional<uint16_t>(httpLoaderPort, obj, "http-loader-port");
        parseOptional<string>(httpLoaderTmpDir, obj, "http-loader-tmp-dir");
    } catch (exception const& ex) {
        throw invalid_argument(context + "the JSON object is not valid, ex: " + string(ex.what()));
    }
}

json WorkerInfo::toJson() const {
    json infoJson;
    infoJson["name"] = name;
    infoJson["is-enabled"] = isEnabled ? 1 : 0;
    infoJson["is-read-only"] = isReadOnly ? 1 : 0;
    infoJson["svc-host"] = svcHost;
    infoJson["svc-port"] = svcPort;
    infoJson["fs-host"] = fsHost;
    infoJson["fs-port"] = fsPort;
    infoJson["data-dir"] = dataDir;
    infoJson["loader-host"] = loaderHost;
    infoJson["loader-port"] = loaderPort;
    infoJson["loader-tmp-dir"] = loaderTmpDir;
    infoJson["exporter-host"] = exporterHost;
    infoJson["exporter-port"] = exporterPort;
    infoJson["exporter-tmp-dir"] = exporterTmpDir;
    infoJson["http-loader-host"] = httpLoaderHost;
    infoJson["http-loader-port"] = httpLoaderPort;
    infoJson["http-loader-tmp-dir"] = httpLoaderTmpDir;
    return infoJson;
}

bool WorkerInfo::operator==(WorkerInfo const& other) const {
    return tie(name, isEnabled, isReadOnly, svcHost, svcPort, fsHost, fsPort, dataDir, loaderHost, loaderPort,
               loaderTmpDir, exporterHost, exporterPort, exporterTmpDir, httpLoaderHost, httpLoaderPort,
               httpLoaderTmpDir) == tie(other.name, other.isEnabled, other.isReadOnly, other.svcHost,
                                        other.svcPort, other.fsHost, other.fsPort, other.dataDir,
                                        other.loaderHost, other.loaderPort, other.loaderTmpDir,
                                        other.exporterHost, other.exporterPort, other.exporterTmpDir,
                                        other.httpLoaderHost, other.httpLoaderPort, other.httpLoaderTmpDir);
}

ostream& operator<<(ostream& os, WorkerInfo const& info) {
    os << "WorkerInfo: " << info.toJson().dump();
    return os;
}

}}}  // namespace lsst::qserv::replica
