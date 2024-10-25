#include <string>
#pragma once

namespace dads::benchmarks::LazyLoading::config::paths {
inline std::string RBL_PATH =
    "/data/david/DADSRemoteBinaryLoaderEngine/"
    "releaseBuild/libDADSRemoteBinaryLoaderEngine.so";
inline std::string WD_PATH =
    "/data/david/DADSWisentDeserialiserEngine/"
    "releaseBuild/libDADSWisentDeserialiserEngine.so";
inline std::string DE_PATH =
    "/data/david/DADSDictionaryEncoderEngine/"
    "releaseBuild/libDADSDictionaryEncoderEngine.so";
inline std::string VELOX_PATH =
    "/data/david/DADSVeloxEngine/premade_lib/"
    "libDADSVeloxEngine.so";
inline std::string PR_PATH =
    "/data/david/DADSParquetReaderEngine/releaseBuild/"
    "libDADSParquetReaderEngine.so";
inline std::string CE_PATH =
    "/data/david/DADSConditionalEvaluationEngine/releaseBuild/"
    "libDADSConditionalEvaluationEngine.so";

inline std::string coordinator =
    "/data/david/DADSLazyLoadingCoordinatorEngine/"
    "releaseBuild/libDADSLazyLoadingCoordinatorEngine.so";

inline std::string OVERHEAD_CSV_SIZE_PATH =
    "/data/david/DADSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/transfer_size_overhead.csv";
inline std::string OVERHEAD_CSV_TIME_PATH =
    "/data/david/DADSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/transfer_time_overhead.csv";

inline std::string QUERY_OVERHEAD_CSV_SIZE_PATH =
    "/data/david/DADSLazyLoadingCoordinatorEngine/"
    "releaseBuild/query_0_transfer_size_overhead.csv";
inline std::string QUERY_OVERHEAD_CSV_TIME_PATH =
    "/data/david/DADSLazyLoadingCoordinatorEngine/"
    "releaseBuild/query_0_transfer_time_overhead.csv";

inline std::string MINISEED_OVERHEAD_CSV_SIZE_PATH =
    "/data/david/DADSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/miniseed_transfer_size_overhead.csv";
inline std::string MINISEED_OVERHEAD_CSV_TIME_PATH =
    "/data/david/DADSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/miniseed_transfer_time_overhead.csv";

inline std::string TPCH_OVERHEAD_CSV_SIZE_PATH =
    "/data/david/DADSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/tpch_transfer_size_overhead.csv";
inline std::string TPCH_OVERHEAD_CSV_TIME_PATH =
    "/data/david/DADSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/tpch_transfer_time_overhead.csv";

inline std::string IN_MEM_CONV_CSV_SIZE_PATH =
    "/data/david/DADSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/transfer_size_overhead_mem_conv.csv";
inline std::string IN_MEM_CONV_CSV_TIME_PATH =
    "/data/david/DADSLazyLoadingCoordinatorEngine/"
    "releaseBuild/results/transfer_time_overhead_mem_conv.csv";
} // namespace dads::benchmarks::LazyLoading::config::paths
