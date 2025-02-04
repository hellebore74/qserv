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
#include "replica/HttpRequestsModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/DatabaseServices.h"
#include "replica/HttpExceptions.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

void HttpRequestsModule::process(Controller::Ptr const& controller, string const& taskName,
                                 HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                                 qhttp::Response::Ptr const& resp, string const& subModuleName,
                                 HttpAuthType const authType) {
    HttpRequestsModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpRequestsModule::HttpRequestsModule(Controller::Ptr const& controller, string const& taskName,
                                       HttpProcessorConfig const& processorConfig,
                                       qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpRequestsModule::executeImpl(string const& subModuleName) {
    if (subModuleName.empty())
        return _requests();
    else if (subModuleName == "SELECT-ONE-BY-ID")
        return _oneRequest();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpRequestsModule::_requests() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    string const jobId = query().optionalString("job_id");
    uint64_t const fromTimeStamp = query().optionalUInt64("from");
    uint64_t const toTimeStamp = query().optionalUInt64("to", numeric_limits<uint64_t>::max());
    size_t const maxEntries = query().optionalUInt64("max_entries");

    debug(__func__, "job_id=" + jobId);
    debug(__func__, "from=" + to_string(fromTimeStamp));
    debug(__func__, "to=" + to_string(toTimeStamp));
    debug(__func__, "max_entries=" + to_string(maxEntries));

    auto const requests = controller()->serviceProvider()->databaseServices()->requests(
            jobId, fromTimeStamp, toTimeStamp, maxEntries);

    json requestsJson;
    for (auto&& info : requests) {
        requestsJson.push_back(info.toJson());
    }
    json result;
    result["requests"] = requestsJson;
    return result;
}

json HttpRequestsModule::_oneRequest() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const id = params().at("id");
    try {
        json result;
        result["request"] = controller()->serviceProvider()->databaseServices()->request(id).toJson();
        return result;
    } catch (DatabaseServicesNotFound const& ex) {
        throw HttpError(__func__, "no such request found");
    }
}

}  // namespace lsst::qserv::replica
