// System header
#include <iostream>
#include <iomanip>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "proto/worker.pb.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"
#include "util/File.h"
#include "wpublish/TestEchoQservRequest.h"

/// This C++ symbol is provided by the SSI shared library
extern XrdSsiProvider* XrdSsiProviderClient;

namespace global = lsst::qserv;
namespace util = lsst::qserv::util;
namespace wpublish = lsst::qserv::wpublish;

using namespace std;

namespace {

// Command line parameters

string fileName;
unsigned int numRequests;
string value;
string serviceProviderLocation;
unsigned int numWorkers;
bool workerFirst;
unsigned int cancelAfterMs;

int test() {
    vector<string> const workers = util::File::getLines(fileName, true);
    if (not numWorkers or (workers.size() < numWorkers)) {
        cerr << "error: specified number of workers not in the valid range: 1.." << numWorkers << endl;
        return 1;
    }

    // Connect to a service provider
    XrdSsiErrInfo errInfo;
    auto serviceProvider = XrdSsiProviderClient->GetService(errInfo, serviceProviderLocation);
    if (nullptr == serviceProvider) {
        cerr << "failed to contact service provider at: " << serviceProviderLocation
             << ", error: " << errInfo.Get() << endl;
        return 1;
    }
    cout << "connected to service provider at: " << serviceProviderLocation << endl;

    // Store request pointers here to prevent them deleted too early
    vector<wpublish::TestEchoQservRequest::Ptr> requests;

    atomic<unsigned int> finished(0);

    if (workerFirst) {
        for (unsigned int j = 0; j < numWorkers; ++j) {
            string const& worker = workers[j];

            for (unsigned int i = 0; i < numRequests; ++i) {
                auto request = wpublish::TestEchoQservRequest::create(
                        value, [&finished](wpublish::TestEchoQservRequest::Status status, string const& error,
                                           string const& sent, string const& received) {
                            if (status != wpublish::TestEchoQservRequest::Status::SUCCESS) {
                                cout << "status: " << wpublish::TestEchoQservRequest::status2str(status)
                                     << "\n"
                                     << "error:  " << error << endl;
                            } else {
                                cout << "value sent:     " << sent << "\n"
                                     << "value received: " << received << endl;
                            }
                            finished--;
                        });
                requests.push_back(request);

                // Submit the request
                finished++;
                XrdSsiResource resource(global::ResourceUnit::makeWorkerPath(worker));
                serviceProvider->ProcessRequest(*request, resource);
            }
        }

    } else {
        for (unsigned int i = 0; i < numRequests; ++i) {
            for (unsigned int j = 0; j < numWorkers; ++j) {
                string const& worker = workers[j];
                auto request = wpublish::TestEchoQservRequest::create(
                        value, [&finished](wpublish::TestEchoQservRequest::Status status, string const& error,
                                           string const& sent, string const& received) {
                            if (status != wpublish::TestEchoQservRequest::Status::SUCCESS) {
                                cout << "status: " << wpublish::TestEchoQservRequest::status2str(status)
                                     << "\n"
                                     << "error:  " << error << endl;
                            } else {
                                cout << "value sent:     " << sent << "\n"
                                     << "value received: " << received << endl;
                            }
                            finished--;
                        });
                requests.push_back(request);

                // Submit the request
                finished++;
                XrdSsiResource resource(global::ResourceUnit::makeWorkerPath(worker));
                serviceProvider->ProcessRequest(*request, resource);
            }
        }
    }
    if (cancelAfterMs == 0) {
        // Block while at least one request is in progress
        util::BlockPost blockPost(1000, 2000);
        while (finished) {
            blockPost.wait(200);
        }
    } else {
        // Request cancellation timeout is used to test the correctness of
        // the XRootD/SSI implementation under heavy loads.
        util::BlockPost blockPost(cancelAfterMs, cancelAfterMs + 1);
        blockPost.wait();
        for (auto&& request : requests) {
            bool const cancel = true;
            request->Finished(cancel);
        }
    }
    return 0;
}
}  // namespace

int main(int argc, const char* const argv[]) {
    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        util::CmdLineParser parser(
                argc, argv,
                "\n"
                "Usage:\n"
                "  <workers-file-name> <num-requests> <value>\n"
                "  [--service=<provider>]\n"
                "  [--num-workers=<value>]\n"
                "  [--worker-first]\b"
                "  [--cancel-after=<milliseconds>]\n"
                "\n"
                "Flags an options:\n"
                "  --service=<provider>  - location of a service provider (default: 'localhost:1094')\n"
                "  --num-workers=<value> - the number of workers (default: 1, range: 1..10)\n"
                "  --worker-first        - iterate over workers, then over requests\n"
                "  --cancel-after=<milliseconds> \n"
                "                        - the number of milliseconds to wait before cancelling\n"
                "                          all requests (default 0 means no cancellation)\n"
                "\n"
                "Parameters:\n"
                "  <workers-file-name>  - a file with worker identifiers (one worker per line)\n"
                "  <num-requests>       - the number of requests per worker\n"
                "  <value>              - arbitrary string\n");

        ::fileName = parser.parameter<string>(1);
        ::numRequests = parser.parameter<unsigned int>(2);
        ::value = parser.parameter<string>(3);

        ::serviceProviderLocation = parser.option<string>("service", "localhost:1094");
        ::numWorkers = parser.option<unsigned int>("num-workers", 1);
        ::workerFirst = parser.flag("worker-first");
        ::cancelAfterMs = parser.option<unsigned int>("cancel-after", 0);

    } catch (exception const& ex) {
        return 1;
    }
    return ::test();
}
