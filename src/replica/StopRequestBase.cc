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
#include "replica/StopRequestBase.h"

// System headers
#include <functional>
#include <sstream>
#include <stdexcept>

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

// Qserv headers
#include "replica/Controller.h"
#include "replica/DatabaseServices.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.StopRequest");

}  // namespace

namespace lsst::qserv::replica {

StopRequestBase::StopRequestBase(ServiceProvider::Ptr const& serviceProvider,
                                 boost::asio::io_service& io_service, char const* requestTypeName,
                                 string const& worker, string const& targetRequestId,
                                 ProtocolQueuedRequestType targetRequestType, int priority, bool keepTracking,
                                 shared_ptr<Messenger> const& messenger)
        : RequestMessenger(serviceProvider, io_service, requestTypeName, worker, priority, keepTracking,
                           false,  // allowDuplicate
                           false,  // disposeRequired
                           messenger),
          _targetRequestId(targetRequestId),
          _targetRequestType(targetRequestType) {}

string StopRequestBase::toString(bool extended) const {
    ostringstream oss(Request::toString(extended));
    oss << "  targetRequestId: " << targetRequestId() << "\n"
        << "  targetPerformance: " << targetPerformance() << "\n";
    return oss.str();
}

void StopRequestBase::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    _sendImpl(lock);
}

void StopRequestBase::awaken(boost::system::error_code const& ec) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (isAborted(ec)) return;

    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    _sendImpl(lock);
}

void StopRequestBase::_sendImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Serialize the Stop message header and the request itself into
    // the network buffer.

    buffer()->resize();

    ProtocolRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(ProtocolRequestHeader::REQUEST);
    hdr.set_management_type(ProtocolManagementRequestType::REQUEST_STOP);
    hdr.set_instance_id(serviceProvider()->instanceId());

    buffer()->serialize(hdr);

    ProtocolRequestStop message;
    message.set_id(_targetRequestId);
    message.set_queued_type(_targetRequestType);

    buffer()->serialize(message);

    send(lock);
}

void StopRequestBase::analyze(bool success, ProtocolStatus status) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  success=" << (success ? "true" : "false"));

    // This method is called on behalf of an asynchronous callback fired
    // upon a completion of the request within method send() - the only
    // client of analyze(). So, we should take care of proper locking and watch
    // for possible state transition which might occur while the async I/O was
    // still in a progress.
    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    if (not success) {
        finish(lock, CLIENT_ERROR);
        return;
    }

    switch (status) {
        case ProtocolStatus::SUCCESS:
            saveReplicaInfo();
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
            finish(lock, SERVER_BAD);
            break;

        case ProtocolStatus::FAILED:
            finish(lock, SERVER_ERROR);
            break;

        case ProtocolStatus::CANCELLED:
            finish(lock, SERVER_CANCELLED);
            break;

        default:
            throw logic_error("StopRequestBase::" + string(__func__) + "  unknown status '" +
                              ProtocolStatus_Name(status) + "' received from server");
    }
}

void StopRequestBase::savePersistentState(replica::Lock const& lock) {
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}

list<pair<string, string>> StopRequestBase::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("target_request_id", targetRequestId());
    return result;
}

}  // namespace lsst::qserv::replica
