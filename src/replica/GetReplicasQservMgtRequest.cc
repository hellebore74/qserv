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
#include "replica/GetReplicasQservMgtRequest.h"

// System headers
#include <set>
#include <stdexcept>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.GetReplicasQservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

GetReplicasQservMgtRequest::Ptr GetReplicasQservMgtRequest::create(
        ServiceProvider::Ptr const& serviceProvider, string const& worker, string const& databaseFamily,
        bool inUseOnly, GetReplicasQservMgtRequest::CallbackType const& onFinish) {
    return GetReplicasQservMgtRequest::Ptr(
            new GetReplicasQservMgtRequest(serviceProvider, worker, databaseFamily, inUseOnly, onFinish));
}

GetReplicasQservMgtRequest::GetReplicasQservMgtRequest(
        ServiceProvider::Ptr const& serviceProvider, string const& worker, string const& databaseFamily,
        bool inUseOnly, GetReplicasQservMgtRequest::CallbackType const& onFinish)
        : QservMgtRequest(serviceProvider, "QSERV_GET_REPLICAS", worker),
          _databaseFamily(databaseFamily),
          _inUseOnly(inUseOnly),
          _onFinish(onFinish),
          _qservRequest(nullptr) {}

QservReplicaCollection const& GetReplicasQservMgtRequest::replicas() const {
    if (not((state() == State::FINISHED) and (extendedState() == ExtendedState::SUCCESS))) {
        throw logic_error("GetReplicasQservMgtRequest::" + string(__func__) +
                          "  replicas aren't available in state: " + state2string(state(), extendedState()));
    }
    return _replicas;
}

list<pair<string, string>> GetReplicasQservMgtRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database_family", databaseFamily());
    result.emplace_back("in_use_only", bool2str(inUseOnly()));
    return result;
}

void GetReplicasQservMgtRequest::_setReplicas(
        replica::Lock const& lock, wpublish::GetChunkListQservRequest::ChunkCollection const& collection) {
    // Filter results by databases participating in the family

    set<string> databases;
    for (auto&& database : serviceProvider()->config()->databases(databaseFamily())) {
        databases.insert(database);
    }
    _replicas.clear();
    for (auto&& replica : collection) {
        if (databases.count(replica.database)) {
            _replicas.emplace_back(QservReplica{replica.chunk, replica.database, replica.use_count});
        }
    }
}

void GetReplicasQservMgtRequest::startImpl(replica::Lock const& lock) {
    // Check if configuration parameters are valid

    if (not serviceProvider()->config()->isKnownDatabaseFamily(databaseFamily())) {
        LOGS(_log, LOG_LVL_ERROR,
             context() << __func__ << "  ** MISCONFIGURED ** "
                       << " database family: '" << databaseFamily() << "'");

        finish(lock, ExtendedState::CONFIG_ERROR);
        return;
    }

    // Submit the actual request

    auto const request = shared_from_base<GetReplicasQservMgtRequest>();

    _qservRequest = wpublish::GetChunkListQservRequest::create(
            inUseOnly(), [request](wpublish::GetChunkListQservRequest::Status status, string const& error,
                                   wpublish::GetChunkListQservRequest::ChunkCollection const& collection) {
                if (request->state() == State::FINISHED) return;

                replica::Lock lock(request->_mtx, request->context() + string(__func__) + "[callback]");

                if (request->state() == State::FINISHED) return;

                switch (status) {
                    case wpublish::GetChunkListQservRequest::Status::SUCCESS:

                        request->_setReplicas(lock, collection);
                        request->finish(lock, QservMgtRequest::ExtendedState::SUCCESS);
                        break;

                    case wpublish::GetChunkListQservRequest::Status::ERROR:

                        request->finish(lock, QservMgtRequest::ExtendedState::SERVER_ERROR, error);
                        break;

                    default:
                        throw logic_error("GetReplicasQservMgtRequest::" + string(__func__) +
                                          "  unhandled server status: " +
                                          wpublish::GetChunkListQservRequest::status2str(status));
                }
            });
    XrdSsiResource resource(ResourceUnit::makeWorkerPath(worker()));
    service()->ProcessRequest(*_qservRequest, resource);
}

void GetReplicasQservMgtRequest::finishImpl(replica::Lock const& lock) {
    switch (extendedState()) {
        case ExtendedState::CANCELLED:
        case ExtendedState::TIMEOUT_EXPIRED:

            // And if the SSI request is still around then tell it to stop

            if (_qservRequest) {
                bool const cancel = true;
                _qservRequest->Finished(cancel);
            }
            break;

        default:
            break;
    }
}

void GetReplicasQservMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<GetReplicasQservMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
