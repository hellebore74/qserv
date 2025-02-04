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
#include "replica/HttpJobsModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/DatabaseServices.h"
#include "replica/HttpExceptions.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

void HttpJobsModule::process(Controller::Ptr const& controller, string const& taskName,
                             HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                             qhttp::Response::Ptr const& resp, string const& subModuleName,
                             HttpAuthType const authType) {
    HttpJobsModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpJobsModule::HttpJobsModule(Controller::Ptr const& controller, string const& taskName,
                               HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                               qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpJobsModule::executeImpl(string const& subModuleName) {
    if (subModuleName.empty())
        return _jobs();
    else if (subModuleName == "SELECT-ONE-BY-ID")
        return _oneJob();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpJobsModule::_jobs() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    string const controllerId = query().optionalString("controller_id");
    string const parentJobId = query().optionalString("parent_job_id");
    uint64_t const fromTimeStamp = query().optionalUInt64("from");
    uint64_t const toTimeStamp = query().optionalUInt64("to", numeric_limits<uint64_t>::max());
    size_t const maxEntries = query().optionalUInt64("max_entries");

    debug(string(__func__) + " controller_id=" + controllerId);
    debug(string(__func__) + " parent_job_id=" + parentJobId);
    debug(string(__func__) + " from=" + to_string(fromTimeStamp));
    debug(string(__func__) + " to=" + to_string(toTimeStamp));
    debug(string(__func__) + " max_entries=" + to_string(maxEntries));

    // Pull descriptions of the Jobs

    auto const jobs = controller()->serviceProvider()->databaseServices()->jobs(
            controllerId, parentJobId, fromTimeStamp, toTimeStamp, maxEntries);

    json jobsJson;
    for (auto&& info : jobs) {
        jobsJson.push_back(info.toJson());
    }
    json result;
    result["jobs"] = jobsJson;
    return result;
}

json HttpJobsModule::_oneJob() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const id = params().at("id");
    try {
        json result;
        result["job"] = controller()->serviceProvider()->databaseServices()->job(id).toJson();
        return result;

    } catch (DatabaseServicesNotFound const& ex) {
        throw HttpError(__func__, "no such job found");
    }
}

}  // namespace lsst::qserv::replica
