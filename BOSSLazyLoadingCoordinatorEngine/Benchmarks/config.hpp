#include <string>
#pragma once

namespace boss::benchmarks::LazyLoading::config::paths {
inline std::string RBL_PATH =
    "/enter/path/to/BOSSRemoteBinaryLoaderEngine/"
    "build/libBOSSRemoteBinaryLoaderEngine.so";
inline std::string WD_PATH =
    "/enter/path/to/BOSSWisentDeserialiserEngine/"
    "build/libBOSSWisentDeserialiserEngine.so";
inline std::string DE_PATH =
    "/enter/path/to/BOSSDictionaryEncoderEngine/"
    "build/libBOSSDictionaryEncoderEngine.so";
inline std::string VELOX_PATH =
    "/enter/path/to/BOSSVeloxEngine/build/"
    "libBOSSVeloxEngine.so";
inline std::string CE_PATH =
    "/enter/path/to/BOSSVeloxEngine/build/"
    "libBOSSConditionalEvaluationEngine.so";

inline std::string coordinator =
    "/enter/path/to/BOSSLazyLoadingCoordinatorEngine/"
    "build/libBOSSLazyLoadingCoordinatorEngine.so";

inline std::string OVERHEAD_CSV_SIZE_PATH =
    "/enter/path/to/BOSSLazyLoadingCoordinatorEngine/"
    "build/results/transfer_size_overhead.csv";
inline std::string OVERHEAD_CSV_TIME_PATH =
    "/enter/path/to/BOSSLazyLoadingCoordinatorEngine/"
    "build/results/transfer_time_overhead.csv";

inline std::string QUERY_OVERHEAD_CSV_SIZE_PATH =
    "/enter/path/to/BOSSLazyLoadingCoordinatorEngine/"
    "build/query_0_transfer_size_overhead.csv";
inline std::string QUERY_OVERHEAD_CSV_TIME_PATH =
    "/enter/path/to/BOSSLazyLoadingCoordinatorEngine/"
    "build/query_0_transfer_time_overhead.csv";

inline std::string MINISEED_OVERHEAD_CSV_SIZE_PATH =
    "/enter/path/to/BOSSLazyLoadingCoordinatorEngine/"
    "build/results/miniseed_transfer_size_overhead.csv";
inline std::string MINISEED_OVERHEAD_CSV_TIME_PATH =
    "/enter/path/to/BOSSLazyLoadingCoordinatorEngine/"
    "build/results/miniseed_transfer_time_overhead.csv";

inline std::string TPCH_OVERHEAD_CSV_SIZE_PATH =
    "/enter/path/to/BOSSLazyLoadingCoordinatorEngine/"
    "build/results/tpch_transfer_size_overhead.csv";
inline std::string TPCH_OVERHEAD_CSV_TIME_PATH =
    "/enter/path/to/BOSSLazyLoadingCoordinatorEngine/"
    "build/results/tpch_transfer_time_overhead.csv";

inline std::string IN_MEM_CONV_CSV_SIZE_PATH =
    "/enter/path/to/BOSSLazyLoadingCoordinatorEngine/"
    "build/results/transfer_size_overhead_mem_conv.csv";
inline std::string IN_MEM_CONV_CSV_TIME_PATH =
    "/enter/path/to/BOSSLazyLoadingCoordinatorEngine/"
    "build/results/transfer_time_overhead_mem_conv.csv";
} // namespace boss::benchmarks::LazyLoading::config::paths
