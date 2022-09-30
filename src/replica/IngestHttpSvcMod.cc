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
#include "replica/IngestHttpSvcMod.h"

// Qserv header
#include "replica/Csv.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

void IngestHttpSvcMod::process(ServiceProvider::Ptr const& serviceProvider,
                               IngestRequestMgr::Ptr const& ingestRequestMgr, string const& workerName,
                               qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                               string const& subModuleName, HttpAuthType const authType) {
    IngestHttpSvcMod module(serviceProvider, ingestRequestMgr, workerName, req, resp);
    module.execute(subModuleName, authType);
}

IngestHttpSvcMod::IngestHttpSvcMod(ServiceProvider::Ptr const& serviceProvider,
                                   IngestRequestMgr::Ptr const& ingestRequestMgr, string const& workerName,
                                   qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : HttpModuleBase(serviceProvider->authKey(), serviceProvider->adminAuthKey(), req, resp),
          _serviceProvider(serviceProvider),
          _ingestRequestMgr(ingestRequestMgr),
          _workerName(workerName) {}

string IngestHttpSvcMod::context() const { return "INGEST-HTTP-SVC "; }

json IngestHttpSvcMod::executeImpl(string const& subModuleName) {
    debug(__func__, "subModuleName: '" + subModuleName + "'");
    if (subModuleName == "SYNC-PROCESS")
        return _syncProcessRequest();
    else if (subModuleName == "ASYNC-SUBMIT")
        return _asyncSubmitRequest();
    else if (subModuleName == "ASYNC-STATUS-BY-ID")
        return _asyncRequest();
    else if (subModuleName == "ASYNC-CANCEL-BY-ID")
        return _asyncCancelRequest();
    else if (subModuleName == "ASYNC-STATUS-BY-TRANS-ID")
        return _asyncTransRequests();
    else if (subModuleName == "ASYNC-CANCEL-BY-TRANS-ID")
        return _asyncTransCancelRequests();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json IngestHttpSvcMod::_syncProcessRequest() const {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const request = _createRequest();
    request->process();
    return json::object({{"contrib", request->transactionContribInfo().toJson()}});
}

json IngestHttpSvcMod::_asyncSubmitRequest() const {
    debug(__func__);
    checkApiVersion(__func__, 12);

    bool const async = true;
    auto const request = _createRequest(async);
    _ingestRequestMgr->submit(request);
    return json::object({{"contrib", request->transactionContribInfo().toJson()}});
}

json IngestHttpSvcMod::_asyncRequest() const {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const id = stoul(params().at("id"));
    auto const contrib = _ingestRequestMgr->find(id);
    return json::object({{"contrib", contrib.toJson()}});
}

json IngestHttpSvcMod::_asyncCancelRequest() const {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const id = stoul(params().at("id"));
    auto const contrib = _ingestRequestMgr->cancel(id);
    return json::object({{"contrib", contrib.toJson()}});
}

json IngestHttpSvcMod::_asyncTransRequests() const {
    debug(__func__);
    checkApiVersion(__func__, 12);

    TransactionId const transactionId = stoul(params().at("id"));
    string const anyTable;
    auto const contribs = _serviceProvider->databaseServices()->transactionContribs(
            transactionId, anyTable, _workerName, TransactionContribInfo::TypeSelector::ASYNC);
    json contribsJson = json::array();
    for (auto& contrib : contribs) {
        contribsJson.push_back(contrib.toJson());
    }
    return json::object({{"contribs", contribsJson}});
}

json IngestHttpSvcMod::_asyncTransCancelRequests() const {
    debug(__func__);
    checkApiVersion(__func__, 12);

    TransactionId const transactionId = stoul(params().at("id"));
    string const anyTable;
    auto const contribs = _serviceProvider->databaseServices()->transactionContribs(
            transactionId, anyTable, _workerName, TransactionContribInfo::TypeSelector::ASYNC);
    json contribsJson = json::array();
    for (auto& contrib : contribs) {
        try {
            contribsJson.push_back(_ingestRequestMgr->cancel(contrib.id).toJson());
        } catch (IngestRequestNotFound const& ex) {
            // Ignore the false-positive error condition for the inactive requests that don't
            // have in-memory representation. These requests only exist in the persistent state
            // of the system. They still need to be included into the service's response.
            contribsJson.push_back(contrib.toJson());
        }
    }
    return json::object({{"contribs", contribsJson}});
}

IngestRequest::Ptr IngestHttpSvcMod::_createRequest(bool async) const {
    TransactionId const transactionId = body().required<TransactionId>("transaction_id");
    string const table = body().required<string>("table");
    unsigned int const chunk = body().required<unsigned int>("chunk");
    bool const isOverlap = body().required<int>("overlap") != 0;
    string const url = body().required<string>("url");

    csv::DialectInput dialectInput;
    // Allow an empty string in the input. Simply replace the one (if present) with
    // the corresponding default value of the parameter.
    auto const getDialectParam = [&](string const& param, string const& defaultValue) -> string {
        string val = body().optional<string>(param, defaultValue);
        if (val.empty()) val = defaultValue;
        return val;
    };
    dialectInput.fieldsTerminatedBy =
            getDialectParam("fields_terminated_by", csv::Dialect::defaultFieldsTerminatedBy);
    dialectInput.fieldsEnclosedBy =
            getDialectParam("fields_enclosed_by", csv::Dialect::defaultFieldsEnclosedBy);
    dialectInput.fieldsEscapedBy = getDialectParam("fields_escaped_by", csv::Dialect::defaultFieldsEscapedBy);
    dialectInput.linesTerminatedBy =
            getDialectParam("lines_terminated_by", csv::Dialect::defaultLinesTerminatedBy);

    string const httpMethod = body().optional<string>("http_method", "GET");
    string const httpData = body().optional<string>("http_data", string());
    vector<string> const httpHeaders = body().optionalColl<string>("http_headers", vector<string>());

    debug(__func__, "transactionId: " + to_string(transactionId));
    debug(__func__, "table: '" + table + "'");
    debug(__func__, "fields_terminated_by: '" + dialectInput.fieldsTerminatedBy + "'");
    debug(__func__, "fields_enclosed_by: '" + dialectInput.fieldsEnclosedBy + "'");
    debug(__func__, "fields_escaped_by: '" + dialectInput.fieldsEscapedBy + "'");
    debug(__func__, "lines_terminated_by: '" + dialectInput.linesTerminatedBy + "'");
    debug(__func__, "chunk: " + to_string(chunk));
    debug(__func__, "isOverlap: " + string(isOverlap ? "1" : "0"));
    debug(__func__, "url: '" + url + "'");
    debug(__func__, "http_method: '" + httpMethod + "'");
    debug(__func__, "http_data: '" + httpData + "'");
    debug(__func__, "http_headers.size(): " + to_string(httpHeaders.size()));

    IngestRequest::Ptr const request =
            IngestRequest::create(_serviceProvider, _workerName, transactionId, table, chunk, isOverlap, url,
                                  async, dialectInput, httpMethod, httpData, httpHeaders);
    return request;
}

}  // namespace lsst::qserv::replica
