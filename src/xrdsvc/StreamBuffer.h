// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2018 LSST Corporation.
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
#ifndef LSST_QSERV_XRDSVC_STREAMBUFFER_H
#define LSST_QSERV_XRDSVC_STREAMBUFFER_H

// System headers
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>

// qserv headers
#include "util/InstanceCount.h"

// Third-party headers
#include "XrdSsi/XrdSsiErrInfo.hh"  // required by XrdSsiStream
#include "XrdSsi/XrdSsiStream.hh"

namespace lsst::qserv::xrdsvc {

/// StreamBuffer is a single use buffer for transferring data packets
/// to XrdSsi.
/// Its notable feature is the Recycle() function, which XrdSsi will
/// promptly call when it no longer needs the buffer.
class StreamBuffer : public XrdSsiStream::Buffer {
public:
    using Ptr = std::shared_ptr<StreamBuffer>;

    // Copying this would be very confusing for something waiting for Recycle().
    StreamBuffer() = delete;
    StreamBuffer(StreamBuffer const &) = delete;
    StreamBuffer &operator=(StreamBuffer const &) = delete;

    /// Factory function, because this should be able to delete itself when Recycle() is called.
    /// The constructor uses move to avoid copying the string.
    static StreamBuffer::Ptr createWithMove(std::string &input);

    /// Set the maximum number of bytes that can be used by all instances of this class.
    static void setMaxTotalBytes(int64_t maxBytes);

    /// @return the percent of totalBytes used out of _maxTotalByes.
    static double percentOfMaxTotalBytesUsed();

    size_t getSize() const { return _dataStr.size(); }

    /// @Return total number of bytes used by ALL StreamBuffer objects.
    static size_t getTotalBytes() { return _totalBytes; }

    /// Call to recycle the buffer when finished (normally called by XrdSsi).
    void Recycle() override;

    /// Wait until Recycle() is called.
    /// @return true if there is data in the buffer.
    bool waitForDoneWithThis();

    /// Unblock the condition variable on cancel.
    void cancel();

    ~StreamBuffer() override;

private:
    /// This constructor will invalidate 'input'.
    explicit StreamBuffer(std::string &input);

    std::string _dataStr;
    std::mutex _mtx;
    std::condition_variable _cv;
    bool _doneWithThis = false;
    bool _cancelled = false;
    Ptr _selfKeepAlive;  ///< keep this object alive until after Recycle() is called.
    // util::InstanceCount _ic{"StreamBuffer"}; ///< Useful as it indicates amount of waiting for czar.

    // Members associated with limiting memory use.
    static std::atomic<int64_t> _totalBytes;  ///< Total bytes currently in use by all StreamBuffer instances.
    static std::atomic<int64_t> _maxTotalBytes;
    static std::mutex _createMtx;
    static std::condition_variable _createCv;
};

}  // namespace lsst::qserv::xrdsvc

#endif  // LSST_QSERV_XRDSVC_STREAMBUFFER_H
