/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

/// \file
/// \brief A tool for estimating the chunk and sub-chunk record
///        counts for the data-sets generated by the duplicator.

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "boost/filesystem.hpp"
#include "boost/program_options.hpp"
#include "boost/shared_ptr.hpp"

#include "partition/Chunker.h"
#include "partition/ChunkIndex.h"
#include "partition/CmdLineUtils.h"
#include "partition/ConfigStore.h"
#include "partition/Geometry.h"
#include "partition/HtmIndex.h"

namespace fs = boost::filesystem;
namespace po = boost::program_options;

namespace lsst::partition {

void defineOptions(po::options_description& opts) {
    po::options_description dup("\\________________ Duplication", 80);
    dup.add_options()("sample.fraction", po::value<double>()->default_value(1.0),
                      "The fraction of input positions to include in the output.");
    dup.add_options()("index", po::value<std::string>(),
                      "HTM index file name for the data set to duplicate. May be "
                      "omitted, in which case --part.index is used as the HTM index "
                      "for both the input data set and for partitioning positions.");
    dup.add_options()("lon-min", po::value<double>()->default_value(0.0),
                      "Minimum longitude angle bound (deg) for the duplication region.");
    dup.add_options()("lon-max", po::value<double>()->default_value(360.0),
                      "Maximum longitude angle bound (deg) for the duplication region.");
    dup.add_options()("lat-min", po::value<double>()->default_value(-90.0),
                      "Minimum latitude angle bound (deg) for the duplication region.");
    dup.add_options()("lat-max", po::value<double>()->default_value(90.0),
                      "Maximum latitude angle bound (deg) for the duplication region.");
    dup.add_options()("chunk-id", po::value<std::vector<int32_t> >(),
                      "Optionally limit duplication to one or more chunks. If specified, "
                      "data will be duplicated for the given chunk(s) regardless of the "
                      "the duplication region and node.");
    dup.add_options()("out.node", po::value<uint32_t>(),
                      "Optionally limit duplication to chunks for the given output node. "
                      "A chunk is assigned to a node when the hash of the chunk ID modulo "
                      "the number of nodes is equal to the node number. If this option is "
                      "specified, its value must be less than --out.num-nodes. It is "
                      "ignored if --chunk-id is specified.");
    po::options_description part("\\_______________ Partitioning", 80);
    part.add_options()("part.index", po::value<std::string>(),
                       "HTM index of partitioning positions. For example, if duplicating "
                       "a source table partitioned on associated object RA and Dec, this "
                       "would be the name of the HTM index file for the object table. If "
                       "this option is omitted, then --index is used as the HTM index for "
                       "both the input and partitioning position data sets.");
    part.add_options()("part.prefix", po::value<std::string>()->default_value("chunk"),
                       "Chunk file name prefix.");
    Chunker::defineOptions(part);
    opts.add(dup).add(part);
    defineOutputOptions(opts);
}

boost::shared_ptr<ChunkIndex> const estimateStats(std::vector<int32_t> const& chunks, Chunker const& chunker,
                                                  HtmIndex const& index, HtmIndex const& partIndex) {
    std::vector<int32_t> subChunks;
    std::vector<uint32_t> htmIds;
    boost::shared_ptr<ChunkIndex> chunkIndex(new ChunkIndex());
    // loop over chunks
    for (std::vector<int32_t>::size_type i = 0; i < chunks.size(); ++i) {
        int32_t chunkId = chunks[i];
        subChunks.clear();
        chunker.getSubChunks(subChunks, chunkId);
        // loop over sub-chunks
        for (std::vector<int32_t>::size_type j = 0; j < subChunks.size(); ++j) {
            int32_t subChunkId = subChunks[j];
            SphericalBox box = chunker.getSubChunkBounds(chunkId, subChunkId);
            SphericalBox overlapBox = box;
            overlapBox.expand(chunker.getOverlap());
            htmIds.clear();
            box.htmIds(htmIds, index.getLevel());
            // loop over overlapping triangles
            for (std::vector<uint32_t>::size_type k = 0; k < htmIds.size(); ++k) {
                uint32_t targetHtmId = htmIds[k];
                uint32_t sourceHtmId = partIndex.mapToNonEmpty(targetHtmId);
                SphericalTriangle tri(targetHtmId);
                double a = tri.area();
                double x = std::min(tri.intersectionArea(box), a);
                ChunkLocation loc;
                loc.chunkId = chunkId;
                loc.subChunkId = subChunkId;
                uint64_t inTri = index(sourceHtmId);
                size_t inBox = static_cast<size_t>((x / a) * inTri);
                chunkIndex->add(loc, inBox);
                double ox = std::max(std::min(tri.intersectionArea(overlapBox), a), x);
                size_t inOverlap = static_cast<size_t>((ox / a) * inTri) - inBox;
                loc.overlap = true;
                chunkIndex->add(loc, inOverlap);
            }
        }
    }
    return chunkIndex;
}

boost::shared_ptr<ChunkIndex> const estimateStats(ConfigStore const& config) {
    Chunker chunker(config);
    if (!config.has("index") && !config.has("part.index")) {
        throw std::runtime_error(
                "One or both of the --index and --part.index "
                "options must be specified.");
    }
    // Load HTM indexes
    char const* opt = (config.has("index") ? "index" : "part.index");
    fs::path indexPath(config.get<std::string>(opt));
    opt = (config.has("part.index") ? "part.index" : "index");
    fs::path partIndexPath(config.get<std::string>(opt));
    boost::shared_ptr<HtmIndex> index(new HtmIndex(indexPath));
    boost::shared_ptr<HtmIndex> partIndex;
    if (partIndexPath != indexPath) {
        partIndex.reset(new HtmIndex(partIndexPath));
    } else {
        partIndex = index;
    }
    if (index->getLevel() != partIndex->getLevel()) {
        throw std::runtime_error(
                "Subdivision levels of input index (--index) "
                "and partitioning index (--part.index) do not "
                "match.");
    }
    std::vector<int32_t> chunks = chunksToDuplicate(chunker, config);
    if (config.flag("verbose")) {
        std::cerr << "Processing " << chunks.size() << " chunks" << std::endl;
    }
    return estimateStats(chunks, chunker, *index, *partIndex);
}

}  // namespace lsst::partition

static char const* help =
        "The spherical duplication statistics estimator estimates the row count\n"
        "for each chunk and sub-chunk in a duplicated data-set, allowing\n"
        "partitioning parameters to be tuned without actually running the\n"
        "duplicator.\n";

int main(int argc, char const* const* argv) {
    namespace part = lsst::partition;
    try {
        po::options_description options;
        part::defineOptions(options);
        part::ConfigStore config = part::parseCommandLine(options, argc, argv, help);
        part::makeOutputDirectory(config, true);
        boost::shared_ptr<part::ChunkIndex> index = part::estimateStats(config);
        if (!index->empty()) {
            fs::path d(config.get<std::string>("out.dir"));
            fs::path f = config.get<std::string>("part.prefix") + "_index.bin";
            index->write(d / f, true);
        }
        if (config.flag("verbose")) {
            index->write(std::cout, 0);
            std::cout << std::endl;
        } else {
            std::cout << *index << std::endl;
        }
    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
