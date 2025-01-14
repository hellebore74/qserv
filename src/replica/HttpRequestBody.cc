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
#include "replica/HttpRequestBody.h"

// System headers
#include <iterator>

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

HttpRequestBody::HttpRequestBody(qhttp::Request::Ptr const& req) : objJson(json::object()) {
    // This way of parsing the optional body allows requests which have no body.

    string const contentType = req->header["Content-Type"];
    string const requiredContentType = "application/json";

    if (contentType == requiredContentType) {
        string content(istreambuf_iterator<char>(req->content), {});
        if (not content.empty()) {
            try {
                objJson = json::parse(content);
                if (objJson.is_null() or objJson.is_object()) return;
            } catch (...) {
                // Not really interested in knowing specific details of the exception.
                // All what matters here is that the string can't be parsed into
                // a valid JSON object. This will be reported via another exception
                // after this block ends.
                ;
            }
            throw invalid_argument("invalid format of the request body. A simple JSON object was expected");
        }
    }
}

bool HttpRequestBody::has(json const& obj, string const& name) const {
    if (not obj.is_object()) {
        throw invalid_argument("HttpRequestBody::" + string(__func__) +
                               " parameter 'obj' is not a valid JSON object");
    }
    return obj.find(name) != obj.end();
}

bool HttpRequestBody::has(string const& name) const { return has(objJson, name); }

}  // namespace lsst::qserv::replica