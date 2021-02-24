// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2018 LSST Corporation.
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

#ifndef LSST_QSERV_WBASE_SENDCHANNEL_H
#define LSST_QSERV_WBASE_SENDCHANNEL_H

// System headers
#include <functional>
#include <memory>
#include <string>
#include <stdexcept>

// Qserv headers
#include "global/Bug.h"
#include "xrdsvc/StreamBuffer.h"

namespace lsst {
namespace qserv {
namespace xrdsvc {
    class SsiRequest;    // Forward declaration
}
namespace wbase {

/// SendChannel objects abstract an byte-output mechanism. Provides a layer of
/// abstraction to reduce coupling to the XrdSsi API. SendChannel generally
/// accepts only one call to send bytes, unless the sendStream call is used.
class SendChannel {
public:
    using Ptr = std::shared_ptr<SendChannel>;
    using Size = long long;

    SendChannel(std::shared_ptr<xrdsvc::SsiRequest> const& s) : _ssiRequest(s) {}
    SendChannel() {} // Strictly for non-Request versions of this object.

    virtual ~SendChannel() {}

    /// ******************************************************************
    /// The following methods are used to send responses back to a request.
    /// The "send" calls may vector the response via the tightly bound
    /// SsiRequest object (the constructor default) or use some other
    /// mechanism (see newNopChannel and newStringChannel).
    ///
    virtual bool send(char const* buf, int bufLen);
    virtual bool sendError(std::string const& msg, int code);

    /// Send the bytes from a POSIX file handle
    virtual bool sendFile(int fd, Size fSize);

    /// Send a bucket of bytes.
    /// @param last true if no more sendStream calls will be invoked.
    virtual bool sendStream(xrdsvc::StreamBuffer::Ptr const& sBuf, bool last);

    ///
    /// ******************************************************************

    /// Set a function to be called when a resources from a deferred send*
    /// operation may be released. This allows a sendFile() caller to be
    /// notified when the file descriptor may be closed and perhaps reclaimed.
    void setReleaseFunc(std::function<void(void)> const& r) { _release = r; }
    void release() {
        _release();
    }

    /// Set the number of Tasks that will be sent using this SendChannel.
    void setTaskCount(int taskCount);

    /// Return a reference to the 'streamMutex'.
    std::mutex& getStreamMutexRef() {
        return _streamMutex;
    }

    ///
    /// @return true if inLast is true and this is the last task to call this
    ///              with inLast == true.
    /// The calling Thread must hold 'streamMutex' before calling this.
    bool transmitTaskLast(bool inLast);

    /// Construct a new NopChannel that ignores everything it is asked to send
    static SendChannel::Ptr newNopChannel();

    /// Construct a StringChannel, which appends all it receives into a string
    /// provided by reference at construction.
    static SendChannel::Ptr newStringChannel(std::string& dest);

protected:
    std::function<void(void)> _release = [](){;}; ///< Function to release resources.

private:
    std::shared_ptr<xrdsvc::SsiRequest> _ssiRequest;

    /// _streamMutex is used to protect _lastCount and messages that are sent
    /// using more than one buffer and order is important.
    std::mutex _streamMutex;
    int _taskCount = 0; ///< The number of tasks to be sent over this SendChannel.
    int _lastCount = 0; ///< Then number of 'last' buffers received.
};

}}} // lsst::qserv::wbase
#endif // LSST_QSERV_WBASE_SENDCHANNEL_H
