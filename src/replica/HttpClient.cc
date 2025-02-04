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
#include "replica/HttpClient.h"

// Qserv headers
#include "replica/HttpExceptions.h"

// Standard headers
#include <algorithm>
#include <cassert>
#include <iostream>
#include <vector>

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

string const HttpClientConfig::category = "worker-http-file-reader";

string const HttpClientConfig::sslVerifyHostKey = "SSL_VERIFYHOST";
string const HttpClientConfig::sslVerifyPeerKey = "SSL_VERIFYPEER";
string const HttpClientConfig::caPathKey = "CAPATH";
string const HttpClientConfig::caInfoKey = "CAINFO";
string const HttpClientConfig::caInfoValKey = "CAINFO_VAL";

string const HttpClientConfig::proxySslVerifyHostKey = "PROXY_SSL_VERIFYHOST";
string const HttpClientConfig::proxySslVerifyPeerKey = "PROXY_SSL_VERIFYPEER";
string const HttpClientConfig::proxyCaPathKey = "PROXY_CAPATH";
string const HttpClientConfig::proxyCaInfoKey = "PROXY_CAINFO";
string const HttpClientConfig::proxyCaInfoValKey = "PROXY_CAINFO_VAL";

string const HttpClientConfig::proxyKey = "CURLOPT_PROXY";
string const HttpClientConfig::noProxyKey = "CURLOPT_NOPROXY";
string const HttpClientConfig::httpProxyTunnelKey = "CURLOPT_HTTPPROXYTUNNEL";

string const HttpClientConfig::connectTimeoutKey = "CONNECTTIMEOUT";
string const HttpClientConfig::timeoutKey = "TIMEOUT";
string const HttpClientConfig::lowSpeedLimitKey = "LOW_SPEED_LIMIT";
string const HttpClientConfig::lowSpeedTimeKey = "LOW_SPEED_TIME";

string const HttpClientConfig::asyncProcLimitKey = "ASYNC_PROC_LIMIT";

size_t forwardToHttpClient(char* ptr, size_t size, size_t nmemb, void* userdata) {
    size_t const nchars = size * nmemb;
    HttpClient* reader = reinterpret_cast<HttpClient*>(userdata);
    reader->_store(ptr, nchars);
    return nchars;
}

HttpClient::HttpClient(string const& method, string const& url, string const& data,
                       vector<string> const& headers, HttpClientConfig const& clientConfig)
        : _method(method), _url(url), _data(data), _headers(headers), _clientConfig(clientConfig) {
    _hcurl = curl_easy_init();
    assert(_hcurl != nullptr);  // curl_easy_init() failed to allocate memory, etc.
}

HttpClient::~HttpClient() {
    curl_slist_free_all(_hlist);
    curl_easy_cleanup(_hcurl);
}

void HttpClient::read(CallbackType const& onDataRead) {
    assert(onDataRead != nullptr);  // no callback function provided
    string const context = "HttpClient::" + string(__func__) + " ";
    _onDataRead = onDataRead;
    _errorChecked("curl_easy_setopt(CURLOPT_URL)", curl_easy_setopt(_hcurl, CURLOPT_URL, _url.c_str()));
    _errorChecked("curl_easy_setopt(CURLOPT_CUSTOMREQUEST)",
                  curl_easy_setopt(_hcurl, CURLOPT_CUSTOMREQUEST, nullptr));
    if (_method == "GET") {
        _errorChecked("curl_easy_setopt(CURLOPT_HTTPGET)", curl_easy_setopt(_hcurl, CURLOPT_HTTPGET, 1L));
    } else if (_method == "POST") {
        _errorChecked("curl_easy_setopt(CURLOPT_POST)", curl_easy_setopt(_hcurl, CURLOPT_POST, 1L));
    } else {
        _errorChecked("curl_easy_setopt(CURLOPT_CUSTOMREQUEST)",
                      curl_easy_setopt(_hcurl, CURLOPT_CUSTOMREQUEST, _method.c_str()));
    }
    if (!_data.empty()) {
        _errorChecked("curl_easy_setopt(CURLOPT_POSTFIELDS)",
                      curl_easy_setopt(_hcurl, CURLOPT_POSTFIELDS, _data.c_str()));
        _errorChecked("curl_easy_setopt(CURLOPT_POSTFIELDSIZE)",
                      curl_easy_setopt(_hcurl, CURLOPT_POSTFIELDSIZE, _data.size()));
    }
    curl_slist_free_all(_hlist);
    _hlist = nullptr;
    for (auto& header : _headers) {
        _hlist = curl_slist_append(_hlist, header.c_str());
    }
    _errorChecked("curl_easy_setopt(CURLOPT_HTTPHEADER)",
                  curl_easy_setopt(_hcurl, CURLOPT_HTTPHEADER, _hlist));

    // Optional settings for the peer's cert
    if (!_clientConfig.sslVerifyHost) {
        _errorChecked("curl_easy_setopt(CURLOPT_SSL_VERIFYHOST)",
                      curl_easy_setopt(_hcurl, CURLOPT_SSL_VERIFYHOST, 0L));
    }
    if (_clientConfig.sslVerifyPeer) {
        if (!_clientConfig.caPath.empty()) {
            _errorChecked("curl_easy_setopt(CURLOPT_CAPATH)",
                          curl_easy_setopt(_hcurl, CURLOPT_CAPATH, _clientConfig.caPath.c_str()));
        }
        if (!_clientConfig.caInfo.empty()) {
            _errorChecked("curl_easy_setopt(CURLOPT_CAINFO)",
                          curl_easy_setopt(_hcurl, CURLOPT_CAINFO, _clientConfig.caInfo.c_str()));
        }
    } else {
        _errorChecked("curl_easy_setopt(CURLOPT_SSL_VERIFYPEER)",
                      curl_easy_setopt(_hcurl, CURLOPT_SSL_VERIFYPEER, 0L));
    }

    // Optional settings for the proxy's cert
    if (!_clientConfig.proxySslVerifyHost) {
        _errorChecked("curl_easy_setopt(CURLOPT_PROXY_SSL_VERIFYHOST)",
                      curl_easy_setopt(_hcurl, CURLOPT_PROXY_SSL_VERIFYHOST, 0L));
    }
    if (_clientConfig.proxySslVerifyPeer) {
        if (!_clientConfig.proxyCaPath.empty()) {
            _errorChecked("curl_easy_setopt(CURLOPT_PROXY_CAPATH)",
                          curl_easy_setopt(_hcurl, CURLOPT_PROXY_CAPATH, _clientConfig.proxyCaPath.c_str()));
        }
        if (!_clientConfig.proxyCaInfo.empty()) {
            _errorChecked("curl_easy_setopt(CURLOPT_PROXY_CAINFO)",
                          curl_easy_setopt(_hcurl, CURLOPT_PROXY_CAINFO, _clientConfig.proxyCaInfo.c_str()));
        }
    } else {
        _errorChecked("curl_easy_setopt(CURLOPT_PROXY_SSL_VERIFYPEER)",
                      curl_easy_setopt(_hcurl, CURLOPT_PROXY_SSL_VERIFYPEER, 0L));
    }

    // Optional settings for proxies
    if (!_clientConfig.proxy.empty()) {
        _errorChecked("curl_easy_setopt(CURLOPT_PROXY)",
                      curl_easy_setopt(_hcurl, CURLOPT_PROXY, _clientConfig.proxy.c_str()));
        if (_clientConfig.httpProxyTunnel != 0) {
            _errorChecked("curl_easy_setopt(CURLOPT_HTTPPROXYTUNNEL)",
                          curl_easy_setopt(_hcurl, CURLOPT_HTTPPROXYTUNNEL, 1L));
        }
    }
    if (!_clientConfig.noProxy.empty()) {
        _errorChecked("curl_easy_setopt(CURLOPT_NOPROXY)",
                      curl_easy_setopt(_hcurl, CURLOPT_NOPROXY, _clientConfig.noProxy.c_str()));
    }

    // Optional settings for timing and performance of the transfer
    _errorChecked("curl_easy_setopt(CURLOPT_CONNECTTIMEOUT)",
                  curl_easy_setopt(_hcurl, CURLOPT_CONNECTTIMEOUT, _clientConfig.connectTimeout));
    _errorChecked("curl_easy_setopt(CURLOPT_TIMEOUT)",
                  curl_easy_setopt(_hcurl, CURLOPT_TIMEOUT, _clientConfig.timeout));
    _errorChecked("curl_easy_setopt(CURLOPT_LOW_SPEED_LIMIT)",
                  curl_easy_setopt(_hcurl, CURLOPT_LOW_SPEED_LIMIT, _clientConfig.lowSpeedLimit));
    _errorChecked("curl_easy_setopt(CURLOPT_LOW_SPEED_TIME)",
                  curl_easy_setopt(_hcurl, CURLOPT_LOW_SPEED_TIME, _clientConfig.lowSpeedTime));

    _errorChecked("curl_easy_setopt(CURLOPT_FAILONERROR)", curl_easy_setopt(_hcurl, CURLOPT_FAILONERROR, 1L));
    _errorChecked("curl_easy_setopt(CURLOPT_WRITEFUNCTION)",
                  curl_easy_setopt(_hcurl, CURLOPT_WRITEFUNCTION, forwardToHttpClient));
    _errorChecked("curl_easy_setopt(CURLOPT_WRITEDATA)", curl_easy_setopt(_hcurl, CURLOPT_WRITEDATA, this));
    _errorChecked("curl_easy_perform()", curl_easy_perform(_hcurl));
}

json HttpClient::readAsJson() {
    vector<char> data;
    this->read([&data](char const* buf, size_t size) { data.insert(data.cend(), buf, buf + size); });
    return json::parse(data);
}

void HttpClient::_errorChecked(string const& scope, CURLcode errnum) {
    if (errnum != CURLE_OK) {
        string errorStr = curl_easy_strerror(errnum);
        long httpResponseCode = 0;
        if (errnum == CURLE_HTTP_RETURNED_ERROR) {
            errorStr += " (on HTTP error codes 400 or greater)";
            curl_easy_getinfo(_hcurl, CURLINFO_RESPONSE_CODE, &httpResponseCode);
        }
        raiseRetryAllowedError(scope, " error: '" + errorStr + "', errnum: " + to_string(errnum),
                               httpResponseCode);
    }
}

void HttpClient::_store(char const* ptr, size_t nchars) { _onDataRead(ptr, nchars); }

}  // namespace lsst::qserv::replica
