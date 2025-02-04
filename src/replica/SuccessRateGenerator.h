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
#ifndef LSST_QSERV_REPLICA_SUCCESSRATEGENERATOR_H
#define LSST_QSERV_REPLICA_SUCCESSRATEGENERATOR_H

// System headers
#include <random>

// Qserv headers
#include "replica/Mutex.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * The SuccessRateGenerator provides a facility for generating a sequence
 * of random boolean values which can be used for simulating Success/Failure
 * scenarios. An implementation of the class is based on the uniform distribution.
 * True values returned by the generator are interpreted as 'success'. The probability
 * density ("success rate") is specified through the constructor of
 * the class.
 *
 * THREAD SAFETY: the generator is thread-safe.
 */
class SuccessRateGenerator {
public:
    // Default construction and copy semantics are prohibited

    SuccessRateGenerator() = delete;
    SuccessRateGenerator(SuccessRateGenerator const&) = delete;
    SuccessRateGenerator& operator=(SuccessRateGenerator const&) = delete;

    /**
     * Normal constructor
     *
     * Allowed range for the 'successRate' parameter is [0.0,1.0].
     * Note that both ends of the range are inclusive. Choosing a value
     * equal to (within the small epsilon) 0.0 would result in the 100% failure rate.
     * The opposite scenario will be seen when choosing  1.0.
     *
     * @param successRate
     *   probability density for 'success'
     */
    explicit SuccessRateGenerator(double successRate = 0.5);

    ~SuccessRateGenerator() = default;

    /**
     * Generate the next random value.
     *
     * @return 'true' for 'success'
     */
    bool success();

private:
    /// The random device is needed to obtain a seed for the random
    /// number engine.
    std::random_device _rd;

    /// Standard mersenne_twister_engine seeded with rd()
    std::mt19937 _gen;

    std::bernoulli_distribution _distr;

    /// The mutex is for synchronized update of the object's state
    /// within a multi-threaded environment.
    mutable replica::Mutex _generatorMtx;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SUCCESSRATEGENERATOR_H
