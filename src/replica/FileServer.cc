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
#include "replica/FileServer.h"

// System headers
#include <functional>
#include <thread>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FileServer");

}  // namespace

namespace lsst::qserv::replica {

FileServer::Ptr FileServer::create(ServiceProvider::Ptr const& serviceProvider, string const& workerName) {
    return FileServer::Ptr(new FileServer(serviceProvider, workerName));
}

FileServer::FileServer(ServiceProvider::Ptr const& serviceProvider, string const& workerName)
        : _serviceProvider(serviceProvider),
          _workerName(workerName),
          _io_service(),
          _acceptor(_io_service, boost::asio::ip::tcp::endpoint(
                                         boost::asio::ip::tcp::v4(),
                                         serviceProvider->config()->get<uint16_t>("worker", "fs-port"))) {
    // Set the socket reuse option to allow recycling ports after catastrophic
    // failures.
    _acceptor.set_option(boost::asio::socket_base::reuse_address(true));
}

void FileServer::run() {
    // We shall do so before running the io_service. Otherwise it will
    // immediately finish as soon as it will discover that there are
    // outstanding operations.
    _beginAccept();

    // Launch all threads in the pool
    vector<shared_ptr<thread>> threads(
            _serviceProvider->config()->get<size_t>("worker", "num-fs-processing-threads"));
    for (auto&& ptr : threads) {
        ptr = shared_ptr<thread>(new thread([&]() { _io_service.run(); }));
    }

    // Wait for all threads in the pool to exit.
    for (auto&& ptr : threads) {
        ptr->join();
    }
}

void FileServer::_beginAccept() {
    FileServerConnection::Ptr const connection =
            FileServerConnection::create(_serviceProvider, _workerName, _io_service);

    _acceptor.async_accept(connection->socket(),
                           bind(&FileServer::_handleAccept, shared_from_this(), connection, _1));
}

void FileServer::_handleAccept(FileServerConnection::Ptr const& connection,
                               boost::system::error_code const& ec) {
    if (ec.value() == 0) {
        connection->beginProtocol();
    } else {
        LOGS(_log, LOG_LVL_DEBUG, _context() << __func__ << "  ec:" << ec);
    }
    _beginAccept();
}

}  // namespace lsst::qserv::replica
