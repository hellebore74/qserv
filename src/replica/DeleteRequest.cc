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
#include "replica/DeleteRequest.h"

// System headers
#include <functional>
#include <stdexcept>

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/DatabaseServices.h"
#include "replica/Messenger.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DeleteRequest");

}  // namespace

namespace lsst::qserv::replica {

DeleteRequest::Ptr DeleteRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                         boost::asio::io_service& io_service, string const& worker,
                                         string const& database, unsigned int chunk, bool allowDuplicate,
                                         CallbackType const& onFinish, int priority, bool keepTracking,
                                         shared_ptr<Messenger> const& messenger) {
    return DeleteRequest::Ptr(new DeleteRequest(serviceProvider, io_service, worker, database, chunk,
                                                allowDuplicate, onFinish, priority, keepTracking, messenger));
}

DeleteRequest::DeleteRequest(ServiceProvider::Ptr const& serviceProvider, boost::asio::io_service& io_service,
                             string const& worker, string const& database, unsigned int chunk,
                             bool allowDuplicate, CallbackType const& onFinish, int priority,
                             bool keepTracking, shared_ptr<Messenger> const& messenger)
        : RequestMessenger(serviceProvider, io_service, "REPLICA_DELETE", worker, priority, keepTracking,
                           allowDuplicate,
                           true,  // disposeRequired
                           messenger),
          _database(database),
          _chunk(chunk),
          _onFinish(onFinish) {
    Request::serviceProvider()->config()->assertDatabaseIsValid(database);
}

void DeleteRequest::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Serialize the Request message header and the request itself into
    // the network buffer.

    buffer()->resize();

    ProtocolRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(ProtocolRequestHeader::QUEUED);
    hdr.set_queued_type(ProtocolQueuedRequestType::REPLICA_DELETE);
    hdr.set_timeout(requestExpirationIvalSec());
    hdr.set_priority(priority());
    hdr.set_instance_id(serviceProvider()->instanceId());

    buffer()->serialize(hdr);

    ProtocolRequestDelete message;
    message.set_database(database());
    message.set_chunk(chunk());

    buffer()->serialize(message);

    _send(lock);
}

void DeleteRequest::awaken(boost::system::error_code const& ec) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (isAborted(ec)) return;

    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    // Serialize the Status message header and the request itself into
    // the network buffer.

    buffer()->resize();

    ProtocolRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(ProtocolRequestHeader::REQUEST);
    hdr.set_management_type(ProtocolManagementRequestType::REQUEST_STATUS);
    hdr.set_instance_id(serviceProvider()->instanceId());

    buffer()->serialize(hdr);

    ProtocolRequestStatus message;
    message.set_id(remoteId());
    message.set_queued_type(ProtocolQueuedRequestType::REPLICA_DELETE);

    buffer()->serialize(message);

    _send(lock);
}

void DeleteRequest::_send(replica::Lock const& lock) {
    auto self = shared_from_base<DeleteRequest>();
    messenger()->send<ProtocolResponseDelete>(
            worker(), id(), priority(), buffer(),
            [self](string const& id, bool success, ProtocolResponseDelete const& response) {
                self->_analyze(success, response);
            });
}

void DeleteRequest::_analyze(bool success, ProtocolResponseDelete const& message) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  success=" << (success ? "true" : "false"));

    // This method is called on behalf of an asynchronous callback fired
    // upon a completion of the request within method _send() - the only
    // client of _analyze(). So, we should take care of proper locking and watch
    // for possible state transition which might occur while the async I/O was
    // still in a progress.
    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    if (not success) {
        finish(lock, CLIENT_ERROR);
        return;
    }

    // Always get the latest status reported by the remote server
    setExtendedServerStatus(lock, message.status_ext());

    // Performance counters are updated from either of two sources,
    // depending on the availability of the 'target' performance counters
    // filled in by the 'STATUS' queries. If the later is not available
    // then fallback to the one of the current request.
    if (message.has_target_performance()) {
        mutablePerformance().update(message.target_performance());
    } else {
        mutablePerformance().update(message.performance());
    }

    // Always extract extended data regardless of the completion status
    // reported by the worker service.
    _replicaInfo = ReplicaInfo(&(message.replica_info()));

    // Extract target request type-specific parameters from the response
    if (message.has_request()) {
        _targetRequestParams = DeleteRequestParams(message.request());
    }
    switch (message.status()) {
        case ProtocolStatus::SUCCESS:
            serviceProvider()->databaseServices()->saveReplicaInfo(_replicaInfo);
            finish(lock, SUCCESS);
            break;

        case ProtocolStatus::CREATED:
            keepTrackingOrFinish(lock, SERVER_CREATED);
            break;

        case ProtocolStatus::QUEUED:
            keepTrackingOrFinish(lock, SERVER_QUEUED);
            break;

        case ProtocolStatus::IN_PROGRESS:
            keepTrackingOrFinish(lock, SERVER_IN_PROGRESS);
            break;

        case ProtocolStatus::IS_CANCELLING:
            keepTrackingOrFinish(lock, SERVER_IS_CANCELLING);
            break;

        case ProtocolStatus::BAD:
            // Special treatment of the duplicate requests if allowed
            if (extendedServerStatus() == ProtocolStatusExt::DUPLICATE) {
                setDuplicateRequestId(lock, message.duplicate_request_id());
                if (allowDuplicate() && keepTracking()) {
                    timer().expires_from_now(boost::posix_time::milliseconds(nextTimeIvalMsec()));
                    timer().async_wait(bind(&DeleteRequest::awaken, shared_from_base<DeleteRequest>(), _1));
                    return;
                }
            }
            finish(lock, SERVER_BAD);
            break;

        case ProtocolStatus::FAILED:
            finish(lock, SERVER_ERROR);
            break;

        case ProtocolStatus::CANCELLED:
            finish(lock, SERVER_CANCELLED);
            break;

        default:
            throw logic_error("DeleteRequest::" + string(__func__) + "  unknown status '" +
                              ProtocolStatus_Name(message.status()) + "' received from server");
    }
}

void DeleteRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<DeleteRequest>(lock, _onFinish);
}

void DeleteRequest::savePersistentState(replica::Lock const& lock) {
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}

list<pair<string, string>> DeleteRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("chunk", to_string(chunk()));
    return result;
}

}  // namespace lsst::qserv::replica
