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
#include "replica/HttpConfigurationModule.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ConfigurationTypes.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpExceptions.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

/**
 * Inspect parameters of the request's query to see if the specified parameter
 * is one of those. And if so then extract its value, convert it into an appropriate
 * type and save in the Configuration.
 * 
 * @param struct_  helper type specific to the Configuration parameter
 * @param query    parameters of the HTTP request
 * @param config   pointer to the Configuration service
 * @param logger   the logger function to report  to the Configuration service
 * @return          'true' f the parameter was found and saved
 */
template<typename T>
bool saveConfigParameter(T& struct_,
                         unordered_map<string,string> const& query,
                         Configuration::Ptr const& config,
                         function<void(string const&)> const& logger) {
    auto const itr = query.find(struct_.key);
    if (itr != query.end()) {
        struct_.value = boost::lexical_cast<decltype(struct_.value)>(itr->second);
        struct_.save(config);
        logger("updated " + struct_.key + "=" + itr->second);
        return true;
    }
    return false;
}
}

namespace lsst {
namespace qserv {
namespace replica {

void HttpConfigurationModule::process(Controller::Ptr const& controller,
                                      string const& taskName,
                                      HttpProcessorConfig const& processorConfig,
                                      qhttp::Request::Ptr const& req,
                                      qhttp::Response::Ptr const& resp,
                                      string const& subModuleName,
                                      HttpModule::AuthType const authType) {
    HttpConfigurationModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}


HttpConfigurationModule::HttpConfigurationModule(Controller::Ptr const& controller,
                                                 string const& taskName,
                                                 HttpProcessorConfig const& processorConfig,
                                                 qhttp::Request::Ptr const& req,
                                                 qhttp::Response::Ptr const& resp)
    :   HttpModule(controller, taskName, processorConfig, req, resp) {
}


json HttpConfigurationModule::executeImpl(string const& subModuleName) {
    if (subModuleName.empty()) return _get();
    else if (subModuleName == "UPDATE-GENERAL") return _updateGeneral();
    else if (subModuleName == "UPDATE-WORKER") return _updateWorker();
    else if (subModuleName == "DELETE-WORKER") return _deleteWorker();
    else if (subModuleName == "ADD-WORKER") return _addWorker();
    else if (subModuleName == "DELETE-DATABASE-FAMILY") return _deleteFamily();
    else if (subModuleName == "ADD-DATABASE-FAMILY") return _addFamily();
    else if (subModuleName == "DELETE-DATABASE") return _deleteDatabase();
    else if (subModuleName == "ADD-DATABASE") return _addDatabase();
    else if (subModuleName == "DELETE-TABLE") return _deleteTable();
    else if (subModuleName == "ADD-TABLE") return _addTable();
    throw invalid_argument(
            context() + "::" + string(__func__) +
            "  unsupported sub-module: '" + subModuleName + "'");
}


json HttpConfigurationModule::_get() {
    debug(__func__);
    json result;
    result["config"] = Configuration::toJson(controller()->serviceProvider()->config());
    return result;
}


json HttpConfigurationModule::_updateGeneral() {
    debug(__func__);

    json result;
    try {
        ConfigurationGeneralParams general;

        auto   const config  = controller()->serviceProvider()->config();
        string const context = __func__;
        auto   const logger  = [this, context](string const& msg) {
            this->debug(context, msg);
        };
        ::saveConfigParameter(general.requestBufferSizeBytes, req()->query, config, logger);
        ::saveConfigParameter(general.retryTimeoutSec, req()->query, config, logger);
        ::saveConfigParameter(general.controllerThreads, req()->query, config, logger);
        ::saveConfigParameter(general.controllerHttpPort, req()->query, config, logger);
        ::saveConfigParameter(general.controllerHttpThreads, req()->query, config, logger);
        ::saveConfigParameter(general.controllerRequestTimeoutSec, req()->query, config, logger);
        ::saveConfigParameter(general.jobTimeoutSec, req()->query, config, logger);
        ::saveConfigParameter(general.jobHeartbeatTimeoutSec, req()->query, config, logger);
        ::saveConfigParameter(general.xrootdAutoNotify, req()->query, config, logger);
        ::saveConfigParameter(general.xrootdHost, req()->query, config, logger);
        ::saveConfigParameter(general.xrootdPort, req()->query, config, logger);
        ::saveConfigParameter(general.xrootdTimeoutSec, req()->query, config, logger);
        ::saveConfigParameter(general.databaseServicesPoolSize, req()->query, config, logger);
        ::saveConfigParameter(general.workerTechnology, req()->query, config, logger);
        ::saveConfigParameter(general.workerNumProcessingThreads, req()->query, config, logger);
        ::saveConfigParameter(general.fsNumProcessingThreads, req()->query, config, logger);
        ::saveConfigParameter(general.workerFsBufferSizeBytes, req()->query, config, logger);
        ::saveConfigParameter(general.loaderNumProcessingThreads, req()->query, config, logger);
        ::saveConfigParameter(general.exporterNumProcessingThreads, req()->query, config, logger);

        result["config"] = Configuration::toJson(config);

    } catch (boost::bad_lexical_cast const& ex) {
        throw HttpError(__func__, "invalid value of a configuration parameter: " + string(ex.what()));
    }
    return result;
}


json HttpConfigurationModule::_updateWorker() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const worker = params().at("worker");

    // Get optional parameters of the query. Note the default values which
    // are expected to be replaced by actual values provided by a client in
    // parameters found in the query.

    string   const svcHost    = query().optionalString("svc_host");
    uint16_t const svcPort    = query().optionalUInt16("svc_port");
    string   const fsHost     = query().optionalString("fs_host");
    uint16_t const fsPort     = query().optionalUInt16("fs_port");
    string   const dataDir    = query().optionalString("data_dir");
    int      const isEnabled  = query().optionalInt(   "is_enabled");
    int      const isReadOnly = query().optionalInt(   "is_read_only");

    debug(__func__, "svc_host="     +           svcHost);
    debug(__func__, "svc_port="     + to_string(svcPort));
    debug(__func__, "fs_host="      +           fsHost);
    debug(__func__, "fs_port="      + to_string(fsPort));
    debug(__func__, "data_dir="     +           dataDir);
    debug(__func__, "is_enabled="   + to_string(isEnabled));
    debug(__func__, "is_read_only=" + to_string(isReadOnly));


    if (not  svcHost.empty()) config->setWorkerSvcHost(worker, svcHost);
    if (0 != svcPort)         config->setWorkerSvcPort(worker, svcPort);
    if (not  fsHost.empty())  config->setWorkerFsHost( worker, fsHost);
    if (0 != fsPort)          config->setWorkerFsPort( worker, fsPort);
    if (not  dataDir.empty()) config->setWorkerDataDir(worker, dataDir);

    if (isEnabled >= 0) {
        if (isEnabled != 0) config->disableWorker(worker, true);
        if (isEnabled == 0) config->disableWorker(worker, false);
    }
    if (isReadOnly >= 0) {
        if (isReadOnly != 0) config->setWorkerReadOnly(worker, true);
        if (isReadOnly == 0) config->setWorkerReadOnly(worker, false);
    }
    json result;
    result["config"] = Configuration::toJson(config);
    return result;
}


json HttpConfigurationModule::_deleteWorker() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const worker = params().at("worker");

    config->deleteWorker(worker);

    json result;
    result["config"] = Configuration::toJson(config);
    return result;
}


json HttpConfigurationModule::_addWorker() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    WorkerInfo info;
    info.name       = query().requiredString("name");
    info.svcHost    = query().requiredString("svc_host");
    info.svcPort    = query().requiredUInt16("svc_port");
    info.fsHost     = query().requiredString("fs_host");
    info.fsPort     = query().requiredUInt16("fs_port");
    info.dataDir    = query().requiredString("data_dir");
    info.isEnabled  = query().requiredBool(  "is_enabled");
    info.isReadOnly = query().requiredBool(  "is_read_only");

    debug(__func__, "name="         +           info.name);
    debug(__func__, "svc_host="     +           info.svcHost);
    debug(__func__, "svc_port="     + to_string(info.svcPort));
    debug(__func__, "fs_host="      +           info.fsHost);
    debug(__func__, "fs_port="      + to_string(info.fsPort));
    debug(__func__, "data_dir="     +           info.dataDir);        
    debug(__func__, "is_enabled="   + to_string(info.isEnabled  ? 1 : 0));
    debug(__func__, "is_read_only=" + to_string(info.isReadOnly ? 1 : 0));

    config->addWorker(info);

    json result;
    result["config"] = Configuration::toJson(config);
    return result;
}


json HttpConfigurationModule::_deleteFamily() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const family = params().at("family");

    config->deleteDatabaseFamily(family);

    json result;
    result["config"] = Configuration::toJson(config);
    return result;
}


json HttpConfigurationModule::_addFamily() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    DatabaseFamilyInfo info;
    info.name             = query().requiredString("name");
    info.replicationLevel = query().requiredUInt64("replication_level");
    info.numStripes       = query().requiredUInt(  "num_stripes");
    info.numSubStripes    = query().requiredUInt(  "num_sub_stripes");
    info.overlap          = query().requiredDouble("overlap");

    debug(__func__, "name="              +           info.name);
    debug(__func__, "replication_level=" + to_string(info.replicationLevel));
    debug(__func__, "num_stripes="       + to_string(info.numStripes));
    debug(__func__, "num_sub_stripes="   + to_string(info.numSubStripes));
    debug(__func__, "overlap="           + to_string(info.overlap));

    if (0 == info.replicationLevel) {
        throw HttpError(__func__, "'replication_level' can't be equal to 0");
    }
    if (0 == info.numStripes) {
        throw HttpError(__func__, "'num_stripes' can't be equal to 0");
    }
    if (0 == info.numSubStripes) {
        throw HttpError(__func__, "'num_sub_stripes' can't be equal to 0");
    }
    if (info.overlap <= 0) {
        throw HttpError(__func__, "'overlap' can't be less or equal to 0");
    }
    config->addDatabaseFamily(info);

    json result;
    result["config"] = Configuration::toJson(config);
    return result;
}


json HttpConfigurationModule::_deleteDatabase() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const database = params().at("database");

    config->deleteDatabase(database);

    json result;
    result["config"] = Configuration::toJson(config);
    return result;
}


json HttpConfigurationModule::_addDatabase() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    DatabaseInfo info;
    info.name   = query().requiredString("name");
    info.family = query().requiredString("family");

    debug(__func__, "name="   + info.name);
    debug(__func__, "family=" + info.family);

    config->addDatabase(info);

    json result;
    result["config"] = Configuration::toJson(config);
    return result;
}


json HttpConfigurationModule::_deleteTable() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const table = params().at("table");
    auto const database = query().requiredString("database");

    config->deleteTable(database, table);

    json result;
    result["config"] = Configuration::toJson(config);
    return result;
}


json HttpConfigurationModule::_addTable() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    auto const table         = query().requiredString("name");
    auto const database      = query().requiredString("database");
    auto const isPartitioned = query().requiredBool(  "is_partitioned");

    debug(__func__, "name="           + table);
    debug(__func__, "database="       + database);
    debug(__func__, "is_partitioned=" + to_string(isPartitioned ? 1 : 0));

    config->addTable(database, table, isPartitioned);

    json result;
    result["config"] = Configuration::toJson(config);
    return result;
}

}}}  // namespace lsst::qserv::replica
