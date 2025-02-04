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
#ifndef LSST_QSERV_REPLICA_CONTROLLER_H
#define LSST_QSERV_REPLICA_CONTROLLER_H

/**
 * This header defines the Replication Controller service for creating and
 * managing requests sent to the remote worker services.
 */

// System headers
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <thread>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "replica/Request.h"
#include "replica/ServiceProvider.h"
#include "replica/Mutex.h"

// Forward declarations
namespace lsst::qserv::replica {

class ReplicationRequest;
class DeleteRequest;
class FindRequest;
class FindAllRequest;
class EchoRequest;
class DirectorIndexRequest;
class SqlAlterTablesRequest;
class SqlQueryRequest;
class SqlCreateDbRequest;
class SqlDeleteDbRequest;
class SqlEnableDbRequest;
class SqlDisableDbRequest;
class SqlGrantAccessRequest;
class SqlCreateIndexesRequest;
class SqlCreateTableRequest;
class SqlCreateTablesRequest;
class SqlDeleteTableRequest;
class SqlDropIndexesRequest;
class SqlGetIndexesRequest;
class SqlRemoveTablePartitionsRequest;
class SqlDeleteTablePartitionRequest;
class SqlRowStatsRequest;
class DisposeRequest;

class StopReplicationRequestPolicy;
class StopDeleteRequestPolicy;
class StopFindRequestPolicy;
class StopFindAllRequestPolicy;
class StopEchoRequestPolicy;
class StopDirectorIndexRequestPolicy;
class StopSqlRequestPolicy;

template <typename POLICY>
class StopRequest;

using StopReplicationRequest = StopRequest<StopReplicationRequestPolicy>;
using StopDeleteRequest = StopRequest<StopDeleteRequestPolicy>;
using StopFindRequest = StopRequest<StopFindRequestPolicy>;
using StopFindAllRequest = StopRequest<StopFindAllRequestPolicy>;
using StopEchoRequest = StopRequest<StopEchoRequestPolicy>;
using StopDirectorIndexRequest = StopRequest<StopDirectorIndexRequestPolicy>;
using StopSqlAlterTablesRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlQueryRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlCreateDbRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlDeleteDbRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlEnableDbRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlDisableDbRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlGrantAccessRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlCreateIndexesRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlCreateTableRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlCreateTablesRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlDeleteTableRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlDropIndexesRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlGetIndexesRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlRemoveTablePartitionsRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlDeleteTablePartitionRequest = StopRequest<StopSqlRequestPolicy>;
using StopSqlRowStatsRequest = StopRequest<StopSqlRequestPolicy>;

class StatusReplicationRequestPolicy;
class StatusDeleteRequestPolicy;
class StatusFindRequestPolicy;
class StatusFindAllRequestPolicy;
class StatusEchoRequestPolicy;
class StatusDirectorIndexRequestPolicy;
class StatusSqlRequestPolicy;

template <typename POLICY>
class StatusRequest;

using StatusReplicationRequest = StatusRequest<StatusReplicationRequestPolicy>;
using StatusDeleteRequest = StatusRequest<StatusDeleteRequestPolicy>;
using StatusFindRequest = StatusRequest<StatusFindRequestPolicy>;
using StatusFindAllRequest = StatusRequest<StatusFindAllRequestPolicy>;
using StatusEchoRequest = StatusRequest<StatusEchoRequestPolicy>;
using StatusDirectorIndexRequest = StatusRequest<StatusDirectorIndexRequestPolicy>;
using StatusSqlAlterTablesRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlQueryRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlCreateDbRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlDeleteDbRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlEnableDbRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlDisableDbRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlGrantAccessRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlCreateIndexesRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlCreateTableRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlCreateTablesRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlDeleteTableRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlDropIndexesRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlGetIndexesRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlRemoveTablePartitionsRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlDeleteTablePartitionRequest = StatusRequest<StatusSqlRequestPolicy>;
using StatusSqlRowStatsRequest = StatusRequest<StatusSqlRequestPolicy>;

class ServiceSuspendRequestPolicy;
class ServiceResumeRequestPolicy;
class ServiceStatusRequestPolicy;
class ServiceRequestsRequestPolicy;
class ServiceDrainRequestPolicy;
class ServiceReconfigRequestPolicy;

template <typename POLICY>
class ServiceManagementRequest;

using ServiceSuspendRequest = ServiceManagementRequest<ServiceSuspendRequestPolicy>;
using ServiceResumeRequest = ServiceManagementRequest<ServiceResumeRequestPolicy>;
using ServiceStatusRequest = ServiceManagementRequest<ServiceStatusRequestPolicy>;
using ServiceRequestsRequest = ServiceManagementRequest<ServiceRequestsRequestPolicy>;
using ServiceDrainRequest = ServiceManagementRequest<ServiceDrainRequestPolicy>;
using ServiceReconfigRequest = ServiceManagementRequest<ServiceReconfigRequestPolicy>;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ControllerIdentity encapsulates various attributes which identify
 * each instance of the Controller class. This information is meant to
 * be used in the multi-Controller setups to coordinate operations
 * between multiple instances and to avoid/resolve conflicts.
 */
class ControllerIdentity {
public:
    std::string id;    ///< A unique identifier of the Controller
    std::string host;  ///< The name of a host where it runs
    pid_t pid;         ///< An identifier of a process
};

std::ostream& operator<<(std::ostream& os, ControllerIdentity const& identity);

/**
 * Class Controller is used for pushing replication (etc.) requests
 * to the worker replication services. Only one instance of this class is
 * allowed per a thread. Request-specific methods of the class will
 * instantiate and start the requests.
 *
 * All methods launching, stopping or checking status of requests
 * require that the server to be running. Otherwise it will throw
 * std::runtime_error. The current implementation of the server
 * doesn't support (yet?) an operation queuing mechanism.
 *
 * Methods which take worker names as parameters will throw exception
 * std::invalid_argument if the specified worker names are not found
 * in the configuration.
 */
class Controller : public std::enable_shared_from_this<Controller> {
public:
    friend class ControllerImpl;
    typedef std::shared_ptr<Controller> Ptr;

    static Ptr create(ServiceProvider::Ptr const& serviceProvider);

    Controller() = delete;
    Controller(Controller const&) = delete;
    Controller& operator=(Controller const&) = delete;
    ~Controller() = default;

    ControllerIdentity const& identity() const { return _identity; }

    uint64_t startTime() const { return _startTime; }

    ServiceProvider::Ptr const& serviceProvider() const { return _serviceProvider; }

    boost::asio::io_service& io_service() { return serviceProvider()->io_service(); }

    /**
     * Check if required folders exist and they're write-enabled for an effective user
     * of the current process. Create missing folders if needed.
     * @param createMissingFolders The optional flag telling the method to create missing folders.
     * @throw std::runtime_error If any folder can't be created, or if any folder is not
     *   write-enabled for the current user.
     */
    void verifyFolders(bool createMissingFolders = false) const;

    std::shared_ptr<ReplicationRequest> replicate(
            std::string const& workerName, std::string const& sourceWorkerName, std::string const& database,
            unsigned int chunk,
            std::function<void(std::shared_ptr<ReplicationRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, bool allowDuplicate = true,
            std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<DeleteRequest> deleteReplica(
            std::string const& workerName, std::string const& database, unsigned int chunk,
            std::function<void(std::shared_ptr<DeleteRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, bool allowDuplicate = true,
            std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<FindRequest> findReplica(
            std::string const& workerName, std::string const& database, unsigned int chunk,
            std::function<void(std::shared_ptr<FindRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool computeCheckSum = false, bool keepTracking = true,
            std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<FindAllRequest> findAllReplicas(
            std::string const& workerName, std::string const& database, bool saveReplicaInfo = true,
            std::function<void(std::shared_ptr<FindAllRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<EchoRequest> echo(
            std::string const& workerName, std::string const& data, uint64_t delay,
            std::function<void(std::shared_ptr<EchoRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<DirectorIndexRequest> directorIndex(
            std::string const& workerName, std::string const& database, std::string const& directorTable,
            unsigned int chunk, bool hasTransactions, TransactionId transactionId,
            std::function<void(std::shared_ptr<DirectorIndexRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlAlterTablesRequest> sqlAlterTables(
            std::string const& workerName, std::string const& database,
            std::vector<std::string> const& tables, std::string const& alterSpec,
            std::function<void(std::shared_ptr<SqlAlterTablesRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlQueryRequest> sqlQuery(
            std::string const& workerName, std::string const& query, std::string const& user,
            std::string const& password, uint64_t maxRows,
            std::function<void(std::shared_ptr<SqlQueryRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlCreateDbRequest> sqlCreateDb(
            std::string const& workerName, std::string const& database,
            std::function<void(std::shared_ptr<SqlCreateDbRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlDeleteDbRequest> sqlDeleteDb(
            std::string const& workerName, std::string const& database,
            std::function<void(std::shared_ptr<SqlDeleteDbRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlEnableDbRequest> sqlEnableDb(
            std::string const& workerName, std::string const& database,
            std::function<void(std::shared_ptr<SqlEnableDbRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlDisableDbRequest> sqlDisableDb(
            std::string const& workerName, std::string const& database,
            std::function<void(std::shared_ptr<SqlDisableDbRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlGrantAccessRequest> sqlGrantAccess(
            std::string const& workerName, std::string const& database, std::string const& user,
            std::function<void(std::shared_ptr<SqlGrantAccessRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlCreateIndexesRequest> sqlCreateTableIndexes(
            std::string const& workerName, std::string const& database,
            std::vector<std::string> const& tables, SqlRequestParams::IndexSpec const& indexSpec,
            std::string const& indexName, std::string const& indexComment,
            std::vector<SqlIndexColumn> const& indexColumns,
            std::function<void(std::shared_ptr<SqlCreateIndexesRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlCreateTableRequest> sqlCreateTable(
            std::string const& workerName, std::string const& database, std::string const& table,
            std::string const& engine, std::string const& partitionByColumn,
            std::list<SqlColDef> const& columns,
            std::function<void(std::shared_ptr<SqlCreateTableRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlCreateTablesRequest> sqlCreateTables(
            std::string const& workerName, std::string const& database,
            std::vector<std::string> const& tables, std::string const& engine,
            std::string const& partitionByColumn, std::list<SqlColDef> const& columns,
            std::function<void(std::shared_ptr<SqlCreateTablesRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlDeleteTableRequest> sqlDeleteTable(
            std::string const& workerName, std::string const& database,
            std::vector<std::string> const& tables,
            std::function<void(std::shared_ptr<SqlDeleteTableRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlRemoveTablePartitionsRequest> sqlRemoveTablePartitions(
            std::string const& workerName, std::string const& database,
            std::vector<std::string> const& tables,
            std::function<void(std::shared_ptr<SqlRemoveTablePartitionsRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlDeleteTablePartitionRequest> sqlDeleteTablePartition(
            std::string const& workerName, std::string const& database,
            std::vector<std::string> const& tables, TransactionId transactionId,
            std::function<void(std::shared_ptr<SqlDeleteTablePartitionRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlDropIndexesRequest> sqlDropTableIndexes(
            std::string const& workerName, std::string const& database,
            std::vector<std::string> const& tables, std::string const& indexName,
            std::function<void(std::shared_ptr<SqlDropIndexesRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlGetIndexesRequest> sqlGetTableIndexes(
            std::string const& workerName, std::string const& database,
            std::vector<std::string> const& tables,
            std::function<void(std::shared_ptr<SqlGetIndexesRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<SqlRowStatsRequest> sqlRowStats(
            std::string const& workerName, std::string const& database,
            std::vector<std::string> const& tables,
            std::function<void(std::shared_ptr<SqlRowStatsRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<DisposeRequest> dispose(
            std::string const& workerName, std::vector<std::string> const& targetIds,
            std::function<void(std::shared_ptr<DisposeRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_NORMAL, bool keepTracking = true, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    template <class REQUEST>
    typename REQUEST::Ptr stopById(std::string const& workerName, std::string const& targetRequestId,
                                   typename REQUEST::CallbackType const& onFinish = nullptr,
                                   int priority = PRIORITY_NORMAL, bool keepTracking = true,
                                   std::string const& jobId = "", unsigned int requestExpirationIvalSec = 0) {
        _debug(__func__, "targetRequestId: " + targetRequestId);
        return _submit<REQUEST, decltype(targetRequestId)>(workerName, targetRequestId, onFinish, priority,
                                                           keepTracking, jobId, requestExpirationIvalSec);
    }

    template <class REQUEST>
    typename REQUEST::Ptr statusById(std::string const& workerName, std::string const& targetRequestId,
                                     typename REQUEST::CallbackType const& onFinish = nullptr,
                                     int priority = PRIORITY_NORMAL, bool keepTracking = true,
                                     std::string const& jobId = "",
                                     unsigned int requestExpirationIvalSec = 0) {
        _debug(__func__, "targetRequestId: " + targetRequestId);
        return _submit<REQUEST, decltype(targetRequestId)>(workerName, targetRequestId, onFinish, priority,
                                                           keepTracking, jobId, requestExpirationIvalSec);
    }

    std::shared_ptr<ServiceSuspendRequest> suspendWorkerService(
            std::string const& workerName,
            std::function<void(std::shared_ptr<ServiceSuspendRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_VERY_HIGH, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<ServiceResumeRequest> resumeWorkerService(
            std::string const& workerName,
            std::function<void(std::shared_ptr<ServiceResumeRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_VERY_HIGH, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<ServiceStatusRequest> statusOfWorkerService(
            std::string const& workerName,
            std::function<void(std::shared_ptr<ServiceStatusRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_VERY_HIGH, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<ServiceRequestsRequest> requestsOfWorkerService(
            std::string const& workerName,
            std::function<void(std::shared_ptr<ServiceRequestsRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_VERY_HIGH, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<ServiceDrainRequest> drainWorkerService(
            std::string const& workerName,
            std::function<void(std::shared_ptr<ServiceDrainRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_VERY_HIGH, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    std::shared_ptr<ServiceReconfigRequest> reconfigWorkerService(
            std::string const& workerName,
            std::function<void(std::shared_ptr<ServiceReconfigRequest>)> const& onFinish = nullptr,
            int priority = PRIORITY_VERY_HIGH, std::string const& jobId = "",
            unsigned int requestExpirationIvalSec = 0);

    /**
     * Specialized version of the requests launcher for the worker service
     * management requests. This is an alternative request launching method
     * for the above defined operations. It allows upstream template specialization
     * in those cases when a generic code is desired.
     * @see Controller::suspendWorkerService()
     * @see Controller::resumeWorkerService()
     * @see Controller::statusOfWorkerService()
     * @see Controller::requestsOfWorkerService()
     * @see Controller::drainWorkerService()
     * @see Controller::reconfigWorkerService()
     * @see Controller::_submit()
     */
    template <class REQUEST>
    typename REQUEST::Ptr workerServiceRequest(std::string const& workerName,
                                               typename REQUEST::CallbackType const& onFinish = nullptr,
                                               int priority = PRIORITY_VERY_HIGH,
                                               std::string const& jobId = "",
                                               unsigned int requestExpirationIvalSec = 0) {
        _logManagementRequest(REQUEST::Policy::requestName(), workerName);
        return _submit<REQUEST>(workerName, onFinish, priority, jobId, requestExpirationIvalSec);
    }

    template <class REQUEST>
    void requestsOfType(std::vector<typename REQUEST::Ptr>& requests) const {
        replica::Lock lock(_mtx, _context(__func__));
        requests.clear();
        for (auto&& itr : _registry)
            if (typename REQUEST::Ptr ptr = std::dynamic_pointer_cast<REQUEST>(itr.second->request())) {
                requests.push_back(ptr);
            }
    }

    template <class REQUEST>
    size_t numRequestsOfType() const {
        replica::Lock lock(_mtx, _context(__func__));
        size_t result(0);
        for (auto&& itr : _registry) {
            if (typename REQUEST::Ptr request = std::dynamic_pointer_cast<REQUEST>(itr.second->request())) {
                ++result;
            }
        }
        return result;
    }

    size_t numActiveRequests() const;

    /**
     * Class RequestWrapper is the base class for implementing requests
     * registry as a polymorphic collection to store active requests. Pure virtual
     * methods of the class will be overridden by request-type-specific implementations.
     * @see class RequestWrapperImpl
     */
    class RequestWrapper {
    public:
        typedef std::shared_ptr<RequestWrapper> Ptr;

        virtual ~RequestWrapper() = default;

        /// to be called on completion of a request
        virtual void notify() = 0;

        /// @return stored request object by a pointer to its base class
        virtual std::shared_ptr<Request> request() const = 0;
    };

    /**
     * Class RequestWrapperImpl extends its base class to implement request-specific
     * pointer extraction and call-back notification.
     * @see class RequestWrapper
     */
    template <class T>
    class RequestWrapperImpl : public RequestWrapper {
    public:
        void notify() override {
            if (nullptr != _onFinish) {
                // Clearing the stored callback after finishing the up-stream notification
                // has two purposes:
                // 1. it guaranties (exactly) one time notification
                // 2. it breaks the up-stream dependency on a caller object if a shared
                //    pointer to the object was mentioned as the lambda-function's closure
                auto onFinish = move(_onFinish);
                _onFinish = nullptr;
                onFinish(_request);
            }
        }

        RequestWrapperImpl(typename T::Ptr const& request, typename T::CallbackType const& onFinish)
                : RequestWrapper(), _request(request), _onFinish(onFinish) {}

        ~RequestWrapperImpl() override = default;

        std::shared_ptr<Request> request() const override { return _request; }

    private:
        typename T::Ptr _request;
        typename T::CallbackType _onFinish;
    };

private:
    explicit Controller(ServiceProvider::Ptr const& serviceProvider);

    std::string _context(std::string const& func = std::string()) const;
    void _debug(std::string const& func, std::string const& msg = std::string()) const;
    void _finish(std::string const& id);
    void _assertIsRunning() const;

    /// Logger for the management requests.
    void _logManagementRequest(std::string const& requestName, std::string const& workerName);

    /**
     * A generic version of the request creation and submission
     * for a variable collection of the request-specific parameters.
     */
    template <class REQUEST, typename... Targs>
    typename REQUEST::Ptr _submit(std::string const& workerName, Targs... Fargs,
                                  typename REQUEST::CallbackType const& onFinish, int priority,
                                  bool keepTracking, std::string const& jobId,
                                  unsigned int requestExpirationIvalSec) {
        _assertIsRunning();
        replica::Lock lock(_mtx, _context(__func__));
        auto const controller = shared_from_this();
        auto const request = REQUEST::create(
                serviceProvider(), serviceProvider()->io_service(), workerName, Fargs...,
                [controller](typename REQUEST::Ptr const& request) { controller->_finish(request->id()); },
                priority, keepTracking, serviceProvider()->messenger());

        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.
        _registry[request->id()] = std::make_shared<RequestWrapperImpl<REQUEST>>(request, onFinish);

        // Initiate the request
        request->start(controller, jobId, requestExpirationIvalSec);
        return request;
    }

    /**
     * Specialized version of the requests launcher for the worker service
     * management requests.
     */
    template <class REQUEST>
    typename REQUEST::Ptr _submit(std::string const& workerName,
                                  typename REQUEST::CallbackType const& onFinish, int priority,
                                  std::string const& jobId, unsigned int requestExpirationIvalSec) {
        _assertIsRunning();
        replica::Lock lock(_mtx, _context(__func__));
        auto const controller = shared_from_this();
        auto const request = REQUEST::create(
                serviceProvider(), serviceProvider()->io_service(), workerName,
                [controller](typename REQUEST::Ptr const& request) { controller->_finish(request->id()); },
                priority, serviceProvider()->messenger());

        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.
        _registry[request->id()] = std::make_shared<RequestWrapperImpl<REQUEST>>(request, onFinish);

        // Initiate the request
        request->start(controller, jobId, requestExpirationIvalSec);
        return request;
    }

    /// The unique identity of the instance/
    ControllerIdentity const _identity;

    /// The number of milliseconds since UNIX Epoch when an instance of
    /// the Controller was created.
    uint64_t const _startTime;

    ServiceProvider::Ptr const _serviceProvider;

    /// For enforcing thread safety of the class's public API
    /// and internal operations.
    mutable replica::Mutex _mtx;

    std::map<std::string, std::shared_ptr<RequestWrapper>> _registry;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CONTROLLER_H
