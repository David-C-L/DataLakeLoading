#include "ITTNotifySupport.hpp"
#include "MiniseedQueries.hpp"
#include "TPCHQueries.hpp"
#include "ParquetTPCHQueries.hpp"
#include "config.hpp"
#include "utilities.hpp"
#include <DADS.hpp>
#include <ExpressionUtilities.hpp>
#include <benchmark/benchmark.h>
#include <random>
#include <unordered_set>

namespace {

VTuneAPIInterface vtune{"DADS"};

using namespace std;
using namespace dads;
using utilities::operator""_; // NOLINT(misc-unused-using-decls) clang-tidy bug
using dads::benchmarks::LazyLoading::config::paths::coordinator;
using dads::benchmarks::LazyLoading::config::paths::DE_PATH;
using dads::benchmarks::LazyLoading::config::paths::RBL_PATH;
using dads::benchmarks::LazyLoading::config::paths::VELOX_PATH;
using dads::benchmarks::LazyLoading::config::paths::WD_PATH;
using dads::benchmarks::LazyLoading::config::paths::CE_PATH;
using dads::benchmarks::LazyLoading::config::paths::PR_PATH;
  
using dads::benchmarks::LazyLoading::config::paths::MINISEED_OVERHEAD_CSV_SIZE_PATH;
using dads::benchmarks::LazyLoading::config::paths::MINISEED_OVERHEAD_CSV_TIME_PATH;
using dads::benchmarks::LazyLoading::config::paths::TPCH_OVERHEAD_CSV_SIZE_PATH;
using dads::benchmarks::LazyLoading::config::paths::TPCH_OVERHEAD_CSV_TIME_PATH;

using dads::benchmarks::LazyLoading::MiniseedQueries::dadsQueries;
using dads::benchmarks::LazyLoading::MiniseedQueries::dadsRangesQueries;
using dads::benchmarks::LazyLoading::MiniseedQueries::dadsCycleRangesQueries;
using dads::benchmarks::LazyLoading::MiniseedQueries::dataVaultsQueries;
  
using dads::benchmarks::LazyLoading::TPCHQueries::dadsQueriesTPCH;
using dads::benchmarks::LazyLoading::TPCHQueries::dadsCycleQueriesTPCH;
using dads::benchmarks::LazyLoading::TPCHQueries::dataVaultsQueriesTPCH;
using dads::benchmarks::LazyLoading::TPCHQueries::TPCH_SF;
  // extern int64_t dads::benchmarks::LazyLoading::TPCHQueries::NUM_THREADS;
  
using dads::benchmarks::LazyLoading::ParquetTPCHQueries::dadsParquetCycleQueriesTPCH;

using dads::benchmarks::LazyLoading::utilities::writeInMemConvergenceData;
using dads::benchmarks::LazyLoading::utilities::writeOverheadData;
using dads::benchmarks::LazyLoading::utilities::writeOverheadQueryData;
using dads::benchmarks::LazyLoading::utilities::writeCURLData;

std::vector<string>
    librariesToTest{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
  int64_t TOP_OF_RANGE_MAX = 100000;
  int64_t TOP_OF_RANGE = 100;
  
  int64_t MAX_FETCH_RANGE = 1048576;
  double IN_MEM_CONV_RAND_INTERVAL_SELECTIVITY = 0.005;
  int64_t OPTIMAL_FETCH_RANGE = 512;
  int64_t MAX_NUM_QUERIES = 8192;
  int64_t BENCHMARK_LAZY = 0;
  int64_t BENCHMARK_EAGER = 1;

int64_t NUM_OVERHEAD_INTS = 134217728;
  int64_t HALF_OVERHEAD_INTS = NUM_OVERHEAD_INTS / 2;
  int64_t QUARTER_OVERHEAD_INTS = NUM_OVERHEAD_INTS / 4;
  int64_t OVERHEAD_ITERATIONS = 3;
std::string OVERHEAD_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/overhead_table.bin";

std::unordered_map<std::string, std::vector<std::function<dads::Expression()>>>
    queryMap{{"DADS", dadsCycleRangesQueries}, {"DATA_VAULTS", dataVaultsQueries}};
  
std::unordered_map<std::string, std::vector<std::function<dads::Expression(TPCH_SF)>>>
    queryMapTPCH{{"DADS", dadsCycleQueriesTPCH}, {"DATA_VAULTS", dataVaultsQueriesTPCH}};

std::unordered_map<std::string, std::vector<std::function<dads::Expression(TPCH_SF)>>>
    queryMapTPCHFileFormat{{"WISENT", dadsCycleQueriesTPCH}, {"PARQUET", dadsParquetCycleQueriesTPCH}};

std::vector<std::string> systemKeys{"DADS", "DATA_VAULTS"};

std::vector<std::string> fileFormatKeys{"WISENT", "PARQUET"};

dads::ComplexExpression getEnginesAsList() {
  if (librariesToTest.empty()) {
    return "List"_(RBL_PATH, WD_PATH, DE_PATH, VELOX_PATH, CE_PATH);
  }
  return {"List"_,
          {},
          dads::ExpressionArguments(librariesToTest.begin(),
                                    librariesToTest.end())};
};
  
dads::ComplexExpression getEnginesAsListWithParquet() {
  if (librariesToTest.empty()) {
    return "List"_(RBL_PATH, PR_PATH, DE_PATH, VELOX_PATH, CE_PATH);
  }
  return {"List"_,
          {},
          dads::ExpressionArguments(librariesToTest.begin(),
                                    librariesToTest.end())};
};

} // namespace

static void BenchmarkQueriesTPCH(benchmark::State &state) {
  if (benchmark::internal::GetGlobalContext() != nullptr &&
      benchmark::internal::GetGlobalContext()->count("EnginePipeline")) {
    auto pipelines =
        benchmark::internal::GetGlobalContext()->at("EnginePipeline") + ";";
    while (pipelines.length() > 0) {
      librariesToTest.push_back(pipelines.substr(0, pipelines.find(";")));
      pipelines.erase(0, pipelines.find(";") + 1);
    }
  }

  auto const systemKey = state.range(0);
  auto const queryIdx = state.range(1);
  auto const sf = static_cast<TPCH_SF>(state.range(2));
  
  std::string curlHeadersPreamble = "System,Query,Scale,Iteration,";
  std::string curlDataInitialPreamble = std::to_string(systemKey) + "," + std::to_string(queryIdx) + "," + std::to_string(sf) + ",";
  auto const query = systemKey == 0 ? "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      std::move(queryMapTPCH[systemKeys[systemKey]][queryIdx](sf))) :
    "DelegateBootstrapping"_(
			     coordinator, getEnginesAsList(),
			     "NonLECycle"_, std::move(queryMapTPCH[systemKeys[systemKey]][queryIdx](sf)));
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
  auto const clearTablesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearTables"_()));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearCaches"_()));
  auto const startTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StartTrackingOverhead"_()));
  auto const stopTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StopTrackingOverhead"_()));
  auto const getTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      "NonLECycle"_, std::move("GetTotalOverhead"_()));

  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(dads::evaluate(std::move(resetCloned)));

  for (int i = 0; i < 3; i++) {
    std::cout << "warmup" << std::endl;
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);

    // std::cout << "QUERY: \n" << cloned << "\n" << std::endl; 
    benchmark::DoNotOptimize(dads::evaluate(std::move(clearTablesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(clearBuffersCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(loadEnginesCloned)));
    // benchmark::DoNotOptimize(dads::evaluate(std::move(startTrackingCloned)));
    auto res = dads::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
    if(get<dads::ComplexExpression>(res).getHead() ==
       "ErrorWhenEvaluatingExpression"_) {
      std::stringstream output;
      output << res;
      state.SkipWithError(std::move(output).str());
    }
  }
  auto loadEnginesClonedFirst =
      loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(dads::evaluate(std::move(loadEnginesClonedFirst)));
  auto i = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    benchmark::DoNotOptimize(dads::evaluate(std::move(clearTablesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(clearBuffersCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(loadEnginesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(startTrackingCloned)));

    vtune.startSampling("TPCH - DADS");
    state.ResumeTiming();
    auto res = dads::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
    state.PauseTiming();
    vtune.stopSampling();
    auto overheadData = dads::evaluate(std::move(getTrackingCloned));
    benchmark::DoNotOptimize(dads::evaluate(std::move(stopTrackingCloned)));
    std::string curlDataPreamble = curlDataInitialPreamble + std::to_string(i++) + ",";
    writeCURLData(
        std::move(std::get<dads::ComplexExpression>(overheadData)),
	curlHeadersPreamble, curlDataPreamble, TPCH_OVERHEAD_CSV_SIZE_PATH, TPCH_OVERHEAD_CSV_TIME_PATH
        );
  }

  auto resetClonedAgain =
      resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearTablesClonedAgain =
      clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearBuffersClonedAgain =
      clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(dads::evaluate(std::move(clearTablesClonedAgain)));
  benchmark::DoNotOptimize(dads::evaluate(std::move(clearBuffersClonedAgain)));
  benchmark::DoNotOptimize(dads::evaluate(std::move(resetClonedAgain)));
}

static void BenchmarkQueriesTPCHParquet(benchmark::State &state) {
  if (benchmark::internal::GetGlobalContext() != nullptr &&
      benchmark::internal::GetGlobalContext()->count("EnginePipeline")) {
    auto pipelines =
        benchmark::internal::GetGlobalContext()->at("EnginePipeline") + ";";
    while (pipelines.length() > 0) {
      librariesToTest.push_back(pipelines.substr(0, pipelines.find(";")));
      pipelines.erase(0, pipelines.find(";") + 1);
    }
  }
  vtune.stopSampling();

  auto const fileFormatKey = state.range(0);
  auto const queryIdx = state.range(1);
  auto const sf = static_cast<TPCH_SF>(state.range(2));
  auto const numThreads = static_cast<int64_t>(state.range(3));
  auto const numRanges = static_cast<int64_t>(state.range(4));

  dads::benchmarks::LazyLoading::TPCHQueries::NUM_THREADS = numThreads;
  dads::benchmarks::LazyLoading::TPCHQueries::NUM_RANGES = numRanges;

  auto const getEngineList = fileFormatKey == 0 ? &getEnginesAsList : &getEnginesAsListWithParquet;
  std::string curlHeadersPreamble = "FileFormat,Query,Scale,Iteration,";
  std::string curlDataInitialPreamble = std::to_string(fileFormatKey) + "," + std::to_string(queryIdx) + "," + std::to_string(sf) + ",";
  auto const query = fileFormatKey == 0 ? "DelegateBootstrapping"_(
      coordinator, getEngineList(),
      std::move(queryMapTPCHFileFormat[fileFormatKeys[fileFormatKey]][queryIdx](sf))) :
    "DelegateBootstrapping"_(
			     coordinator, getEngineList(),
			     std::move(queryMapTPCHFileFormat[fileFormatKeys[fileFormatKey]][queryIdx](sf)));
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEngineList(), std::move("HELLO :)"_()));
  auto const clearTablesQuery =
      "EvaluateInEngines"_(getEngineList(), std::move("ClearTables"_()));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getEngineList(), std::move("ClearCaches"_()));
  auto const clearWisentCachesQuery =
      "EvaluateInEngines"_(getEngineList(), std::move("ClearWisentCaches"_()));
  auto const startTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEngineList(), "NonLECycle"_, std::move("StartTrackingOverhead"_()));
  auto const stopTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEngineList(), "NonLECycle"_, std::move("StopTrackingOverhead"_()));
  auto const getTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEngineList(),
      "NonLECycle"_, std::move("GetTotalOverhead"_()));

  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(dads::evaluate(std::move(resetCloned)));

  for (int i = 0; i < 1; i++) {
    std::cout << "warmup" << std::endl;
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearWisentCachesCloned =
        clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);

    benchmark::DoNotOptimize(dads::evaluate(std::move(clearTablesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(clearBuffersCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(clearWisentCachesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(loadEnginesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(startTrackingCloned)));
    auto res = dads::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
  }

  auto loadEnginesClonedFirst =
      loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(dads::evaluate(std::move(loadEnginesClonedFirst)));
  auto i = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearWisentCachesCloned =
        clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    benchmark::DoNotOptimize(dads::evaluate(std::move(clearTablesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(clearBuffersCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(clearWisentCachesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(loadEnginesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(startTrackingCloned)));

    vtune.startSampling("TPCH - DADS");
    state.ResumeTiming();
    auto res = dads::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
    state.PauseTiming();
    vtune.stopSampling();
    auto overheadData = dads::evaluate(std::move(getTrackingCloned));
    benchmark::DoNotOptimize(dads::evaluate(std::move(stopTrackingCloned)));
    std::string curlDataPreamble = curlDataInitialPreamble + std::to_string(i++) + ",";
    
    std::string fileStr = std::to_string(fileFormatKey);
    std::string qIdxStr = std::to_string(queryIdx);
    std::string sfStr = std::to_string(static_cast<int32_t>(sf));

    std::string keyword = "results/";
    std::string sizeCopy = TPCH_OVERHEAD_CSV_SIZE_PATH;
    size_t sizePos = sizeCopy.find(keyword);
    std::string timeCopy = TPCH_OVERHEAD_CSV_TIME_PATH;
    time_t timePos = timeCopy.find(keyword);

    if (pos != std::string::npos) {
      sizeCopy.insert(sizePos + keyword.length(), "-" + fileStr + "-" + qIdxStr + "-" sfStr + "/");
      timeCopy.insert(timePos + keyword.length(), "-" + fileStr + "-" + qIdxStr + "-" sfStr + "/");
    }
    writeCURLData(
        std::move(std::get<dads::ComplexExpression>(overheadData)),
	curlHeadersPreamble, curlDataPreamble, sizeCopy, timeCopy
        );
  }

  for (int i = 0; i < 1; i++) {
    std::cout << "cooldown" << std::endl;
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearWisentCachesCloned =
        clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);

    benchmark::DoNotOptimize(dads::evaluate(std::move(clearTablesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(clearBuffersCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(clearWisentCachesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(loadEnginesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(startTrackingCloned)));
    auto res = dads::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
  }

  auto resetClonedAgain =
      resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearTablesClonedAgain =
      clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearBuffersClonedAgain =
      clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearWisentCachesClonedAgain =
        clearWisentCachesQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(dads::evaluate(std::move(clearTablesClonedAgain)));
  benchmark::DoNotOptimize(dads::evaluate(std::move(clearBuffersClonedAgain)));
  benchmark::DoNotOptimize(dads::evaluate(std::move(clearWisentCachesClonedAgain)));
  benchmark::DoNotOptimize(dads::evaluate(std::move(resetClonedAgain)));
}

static void BenchmarkQueries(benchmark::State &state) {
  vtune.stopSampling();
  if (benchmark::internal::GetGlobalContext() != nullptr &&
      benchmark::internal::GetGlobalContext()->count("EnginePipeline")) {
    auto pipelines =
        benchmark::internal::GetGlobalContext()->at("EnginePipeline") + ";";
    while (pipelines.length() > 0) {
      librariesToTest.push_back(pipelines.substr(0, pipelines.find(";")));
      pipelines.erase(0, pipelines.find(";") + 1);
    }
  }

  auto const systemKey = state.range(0);
  auto const queryIdx = state.range(1);
  std::string curlHeadersPreamble = "System,Query,Iteration,";
  std::string curlDataInitialPreamble = std::to_string(systemKey) + "," + std::to_string(queryIdx) + ",";
  auto const query = systemKey == 0 ? "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      std::move(queryMap[systemKeys[systemKey]][queryIdx]())) :
    "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      "NonLECycle"_, std::move(queryMap[systemKeys[systemKey]][queryIdx]()));
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
  auto const clearTablesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearTables"_()));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearCaches"_()));
  auto const startTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StartTrackingOverhead"_()));
  auto const stopTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StopTrackingOverhead"_()));
  auto const getTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      "NonLECycle"_, std::move("GetTotalOverhead"_()));
  
  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  dads::evaluate(std::move(resetCloned));

  for (int i = 0; i < 3; i++) {
    std::cout << "warmup" << std::endl;
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);

    dads::evaluate(std::move(clearTablesCloned));
    dads::evaluate(std::move(clearBuffersCloned));
    dads::evaluate(std::move(loadEnginesCloned));
    auto res = dads::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
    if(queryIdx != 7 && get<dads::ComplexExpression>(res).getHead() ==
       "ErrorWhenEvaluatingExpression"_) {
      std::stringstream output;
      output << res;
      state.SkipWithError(std::move(output).str());
    }
  }
  auto loadEnginesClonedFirst =
      loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
  dads::evaluate(std::move(loadEnginesClonedFirst));
  auto i = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    dads::evaluate(std::move(loadEnginesCloned));
    dads::evaluate(std::move(clearTablesCloned));
    dads::evaluate(std::move(clearBuffersCloned));
    dads::evaluate(std::move(startTrackingCloned));
    state.ResumeTiming();
    vtune.startSampling("MINISEED - DADS");
    auto res = dads::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(res);
    vtune.stopSampling();
    state.PauseTiming();
    auto overheadData = dads::evaluate(std::move(getTrackingCloned));
    dads::evaluate(std::move(stopTrackingCloned));
    std::string curlDataPreamble = curlDataInitialPreamble + std::to_string(i++) + ",";
    writeCURLData(
        std::move(std::get<dads::ComplexExpression>(overheadData)),
	curlHeadersPreamble, curlDataPreamble, MINISEED_OVERHEAD_CSV_SIZE_PATH, MINISEED_OVERHEAD_CSV_TIME_PATH
        );
  }

  auto resetClonedAgain =
      resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearTablesClonedAgain =
      clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearBuffersClonedAgain =
      clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
  dads::evaluate(std::move(clearTablesClonedAgain));
  dads::evaluate(std::move(clearBuffersClonedAgain));
  dads::evaluate(std::move(resetClonedAgain));
}

static void BenchmarkQueriesOverhead(benchmark::State &state) {
  if (benchmark::internal::GetGlobalContext() != nullptr &&
      benchmark::internal::GetGlobalContext()->count("EnginePipeline")) {
    auto pipelines =
        benchmark::internal::GetGlobalContext()->at("EnginePipeline") + ";";
    while (pipelines.length() > 0) {
      librariesToTest.push_back(pipelines.substr(0, pipelines.find(";")));
      pipelines.erase(0, pipelines.find(";") + 1);
    }
  }

  auto const systemKey = state.range(0);
  auto const queryIdx = state.range(1);
  auto const query = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      std::move(queryMap[systemKeys[systemKey]][queryIdx]()));
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
  auto const clearTablesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearTables"_()));
  auto loadEnginesClonedFirst =
      loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto const startTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StartTrackingOverhead"_()));
  auto const stopTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StopTrackingOverhead"_()));
  auto const getTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(),
      "NonLECycle"_, std::move("GetTotalOverhead"_(OVERHEAD_TABLE_URL)));

  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearTablesCloned =
        clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    benchmark::DoNotOptimize(dads::evaluate(std::move(clearTablesCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(resetCloned)));
    benchmark::DoNotOptimize(dads::evaluate(std::move(loadEnginesCloned)));
    dads::evaluate(std::move(startTrackingCloned));
    vtune.startSampling("FrameworkOverhead - DADS");
    state.ResumeTiming();
    benchmark::DoNotOptimize(dads::evaluate(std::move(cloned)));
    state.PauseTiming();
    vtune.stopSampling();
    auto overheadData = dads::evaluate(std::move(getTrackingCloned));
    dads::evaluate(std::move(stopTrackingCloned));
    writeOverheadQueryData(
        std::move(std::get<dads::ComplexExpression>(overheadData)), systemKey);
    state.ResumeTiming();
  }

  auto resetClonedAgain =
      resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  auto clearTablesClonedAgain =
      clearTablesQuery.clone(expressions::CloneReason::FOR_TESTING);
  benchmark::DoNotOptimize(dads::evaluate(std::move(clearTablesClonedAgain)));
  benchmark::DoNotOptimize(dads::evaluate(std::move(resetClonedAgain)));
}

static void BenchmarkFrameworkOverhead(benchmark::State &state) {
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));

  for (auto _ : state) {
    state.PauseTiming();
    auto query =
        "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
    for (auto i = 0; i < 8; i++) {
      query = "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                                       std::move(queryMap[systemKeys[0]][i]()));
      auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
      auto resetCloned =
          resetQuery.clone(expressions::CloneReason::FOR_TESTING);
      auto loadEnginesCloned =
          loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
      dads::evaluate(std::move(resetCloned));
      dads::evaluate(std::move(loadEnginesCloned));
      vtune.startSampling("FrameworkOverhead - DADS");
      state.ResumeTiming();
      auto result = dads::evaluate(std::move(cloned));
      benchmark::DoNotOptimize(result);
      state.PauseTiming();
      vtune.stopSampling();
    }
  }
}

static void BenchmarkOverhead(benchmark::State &state) {
  auto const selectivity = static_cast<int64_t>(state.range(0));
  auto realSelectivity = static_cast<double>(selectivity) / TOP_OF_RANGE_MAX;
  auto numSelected = static_cast<int64_t>(realSelectivity * NUM_OVERHEAD_INTS);

  auto getRandVec = [&](int64_t size, int64_t upperBound) {
    if (size == NUM_OVERHEAD_INTS) {
      std::vector<int64_t> indices(size);
      std::iota(indices.begin(), indices.end(), 0);
      return indices;
    }
    std::cout << "SIZE: " << size << " BOUND: " << upperBound << std::endl;
    std::vector<int64_t> indices;
    std::unordered_set<int64_t> taken;

    std::random_device rd;
    std::mt19937 gen(rd());

    std::uniform_int_distribution<int64_t> distrib(0, upperBound - 1);

    while (indices.size() < size) {
      int64_t num = distrib(gen);
      if (taken.insert(num).second) {
        indices.push_back(num);
      }
    }

    return indices;
  };

  auto getGather = [&]() {
    std::vector<int64_t> indices = getRandVec(numSelected, NUM_OVERHEAD_INTS);
    dads::ExpressionArguments args;
    args.push_back(OVERHEAD_TABLE_URL);
    args.push_back(RBL_PATH);
    args.push_back("List"_(
        "List"_(dads::Span<int64_t const>(std::move(vector(indices))))));
    args.push_back("List"_("value1"_));

    auto gather = dads::ComplexExpression{"Gather"_, {}, std::move(args), {}};
    return std::move(gather);
  };

  auto const query = "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                                              std::move(getGather()));
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
  auto const startTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StartTrackingOverhead"_()));
  auto const stopTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StopTrackingOverhead"_()));
  auto const getTrackingQuery =
      "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                               "NonLECycle"_, std::move("GetOverhead"_(OVERHEAD_TABLE_URL)));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearCaches"_()));

  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  dads::evaluate(std::move(resetCloned));
    
  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    dads::evaluate(std::move(clearBuffersCloned));
    dads::evaluate(std::move(loadEnginesCloned));
    dads::evaluate(std::move(startTrackingCloned));
    state.ResumeTiming();
    auto result = dads::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(result);
    state.PauseTiming();
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto overheadData = dads::evaluate(std::move(getTrackingCloned));
    dads::evaluate(std::move(stopTrackingCloned));
    writeOverheadData(
        std::move(std::get<dads::ComplexExpression>(overheadData)),
        realSelectivity);
    state.ResumeTiming();
  }

  auto resetClonedAgain = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  dads::evaluate(std::move(resetClonedAgain));
}

static void BenchmarkRanges(benchmark::State &state) {
  auto const numRanges = static_cast<int64_t>(state.range(0));
  int64_t chunkToGather = HALF_OVERHEAD_INTS / numRanges;

  auto getRangedVecs = [&]() {
    std::cout << "NUM_RANGES: " << numRanges << std::endl;
    std::vector<int64_t> starts;
    std::vector<int64_t> ends;
    starts.reserve(numRanges);
    ends.reserve(numRanges);

    for (auto i = 0; i < NUM_OVERHEAD_INTS; i += 2 * chunkToGather) {
      starts.push_back(i);
      ends.push_back(i + chunkToGather);
    }

    return std::make_pair(std::move(starts), std::move(ends));
  };

  auto getRangedGather = [&]() {
    auto const &[starts, ends] = getRangedVecs();
    dads::ExpressionArguments args;
    args.push_back(OVERHEAD_TABLE_URL);
    args.push_back(RBL_PATH);
    args.push_back("starts"_);
    args.push_back("ends"_);
    args.push_back("Table"_(
        "starts"_(
            "List"_(dads::Span<int64_t const>(std::move(vector(starts))))),
        "ends"_("List"_(dads::Span<int64_t const>(std::move(vector(ends)))))));
    args.push_back("List"_("value1"_));
    args.push_back(numRanges);

    auto gatherRanges =
        dads::ComplexExpression{"GatherRanges"_, {}, std::move(args), {}};
    return std::move(gatherRanges);
  };

  auto const query = "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                                              std::move(getRangedGather()));
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearCaches"_()));

  
  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  dads::evaluate(std::move(resetCloned));
  
  for (auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    dads::evaluate(std::move(clearBuffersCloned));
    dads::evaluate(std::move(loadEnginesCloned));
    state.ResumeTiming();
    auto result = dads::evaluate(std::move(cloned));
    benchmark::DoNotOptimize(result);
  }

  auto resetClonedAgain = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  dads::evaluate(std::move(resetClonedAgain));
}

static void BenchmarkInMemConvergence(benchmark::State &state) {
  auto const benchmarkState = static_cast<int64_t>(state.range(0));
  auto const numQueries = static_cast<int64_t>(state.range(1));

  auto getRandRange = [&](int64_t upperBound) {
    int64_t intervalSize = IN_MEM_CONV_RAND_INTERVAL_SELECTIVITY * upperBound;

    std::random_device rd;
    std::mt19937 gen(rd());

    std::uniform_int_distribution<int64_t> distrib(0, upperBound - 1);
    int64_t start = distrib(gen);
    int64_t end = upperBound - 1 - start < intervalSize ? upperBound
                                                        : start + intervalSize;

    std::vector<int64_t> starts;
    std::vector<int64_t> ends;
    starts.push_back(start);
    ends.push_back(end);

    return std::make_pair(std::move(starts), std::move(ends));
  };

  auto getRandRangedGather = [&]() {
    auto const &[starts, ends] = getRandRange(NUM_OVERHEAD_INTS);
    dads::ExpressionArguments args;
    args.push_back(OVERHEAD_TABLE_URL);
    args.push_back(RBL_PATH);
    args.push_back("starts"_);
    args.push_back("ends"_);
    args.push_back("Table"_(
        "starts"_(
            "List"_(dads::Span<int64_t const>(std::move(vector(starts))))),
        "ends"_("List"_(dads::Span<int64_t const>(std::move(vector(ends)))))));
    args.push_back("List"_("value1"_));
    args.push_back(OPTIMAL_FETCH_RANGE);

    auto gatherRanges = dads::ComplexExpression{"GatherRanges"_, {}, std::move(args), {}};
    auto project = "Project"_(std::move(gatherRanges), "As"_("value1"_, "value1"_));
    return std::move(project);
  };

  auto getRandSelect = [&](dads::Expression &&table) {
    auto const &[starts, ends] = getRandRange(NUM_OVERHEAD_INTS);
    auto start = starts[0];
    auto end = ends[0];

    auto select = "Select"_(std::move(table),
                            "Where"_("And"_("Greater"_("value1"_, start),
                                            "Greater"_(end, "value1"_))));

    return std::move(select);
  };

  auto getParseTables = [&]() {
    std::vector<std::string> overheadURLFiles = {OVERHEAD_TABLE_URL};
    auto parseTables = "ParseTables"_(
        RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
                      std::move(vector(overheadURLFiles)))))));
    return std::move(parseTables);
  };

  auto getOverheadTable = [&]() {
    auto query = "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                                          std::move(getParseTables()));
    auto table = dads::evaluate(std::move(query));
    return std::move(table);
  };
  
  auto const resetQuery = "ResetEngines"_();
  auto const loadEnginesQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("HELLO :)"_()));
  auto const startTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StartTrackingOverhead"_()));
  auto const stopTrackingQuery = "DelegateBootstrapping"_(
      coordinator, getEnginesAsList(), "NonLECycle"_, std::move("StopTrackingOverhead"_()));
  auto const getTrackingQuery =
      "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                               "NonLECycle"_, std::move("GetOverhead"_(OVERHEAD_TABLE_URL)));
  auto const clearBuffersQuery =
      "EvaluateInEngines"_(getEnginesAsList(), std::move("ClearCaches"_()));

  auto resetCloned = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  dads::evaluate(std::move(resetCloned));

  for (auto _ : state) {
    state.PauseTiming();
    auto loadEnginesCloned =
        loadEnginesQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto startTrackingCloned =
        startTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto stopTrackingCloned =
        stopTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto getTrackingCloned =
        getTrackingQuery.clone(expressions::CloneReason::FOR_TESTING);
    auto clearBuffersCloned =
        clearBuffersQuery.clone(expressions::CloneReason::FOR_TESTING);
    dads::evaluate(std::move(clearBuffersCloned));
    dads::evaluate(std::move(loadEnginesCloned));
    dads::evaluate(std::move(startTrackingCloned));
    if (benchmarkState == BENCHMARK_LAZY) {
      state.ResumeTiming();
      for (auto i = 0; i < numQueries; i++) {
        state.PauseTiming();
        auto query = "DelegateBootstrapping"_(coordinator, getEnginesAsList(),
                                              std::move(getRandRangedGather()));
        state.ResumeTiming();
        auto result = dads::evaluate(std::move(query));
        benchmark::DoNotOptimize(result);
      }
    } else if (benchmarkState == BENCHMARK_EAGER) {
      auto overheadTable = getOverheadTable();
      state.ResumeTiming();
      for (auto i = 0; i < numQueries; i++) {
        state.PauseTiming();
        auto overheadTableClone =
            overheadTable.clone(expressions::CloneReason::FOR_TESTING);
        auto query = "DelegateBootstrapping"_(
            coordinator, getEnginesAsList(),
            "NonLECycle"_, std::move(getRandSelect(std::move(overheadTableClone))));
        state.ResumeTiming();
        auto result = dads::evaluate(std::move(query));
        benchmark::DoNotOptimize(result);
      }
    }
    state.PauseTiming();
    auto overheadData = dads::evaluate(std::move(getTrackingCloned));
    dads::evaluate(std::move(stopTrackingCloned));
    writeInMemConvergenceData(
        std::move(std::get<dads::ComplexExpression>(overheadData)), numQueries,
        benchmarkState);
    state.ResumeTiming();
  }

  auto resetClonedAgain = resetQuery.clone(expressions::CloneReason::FOR_TESTING);
  dads::evaluate(std::move(resetClonedAgain));
}

static void TPCHArguments(benchmark::internal::Benchmark* b) {
  std::vector<int> sysKeys = {1};
  std::vector<int> queryIdxs = {2};
  std::vector<int> sfs = {4};
  for (auto sysKey : sysKeys) {
    for (auto queryIdx : queryIdxs) {
      for (auto sf : sfs) {
	b->Args({sysKey, queryIdx, sf});
      }
    }
  }
}

static void TPCHParquetArguments(benchmark::internal::Benchmark* b) {
  std::vector<int> fileFormatKeys = {0,1};
  std::vector<int> queryIdxs = {0,1,2,3,4};
  std::vector<int> sfs = {3,4,5};
  std::vector<int> numThreads = {6};
  std::vector<int> numRanges = {600000};

  
  for (auto fileFormatKey : fileFormatKeys) {
    for (auto queryIdx : queryIdxs) {
      for (auto sf : sfs) {
	for (auto nr : numRanges) {
	  for (auto nt : numThreads) {
	    b->Args({fileFormatKey, queryIdx, sf, nt, nr});
	  }
	}
      }
    }
  }
}

BENCHMARK(BenchmarkQueriesTPCH)
    ->Unit(benchmark::kMillisecond)
    ->Apply(TPCHArguments)
    ->Iterations(3);

BENCHMARK(BenchmarkQueriesTPCHParquet)
    ->Unit(benchmark::kMillisecond)
    ->Apply(TPCHParquetArguments)
    ->Iterations(3);

BENCHMARK(BenchmarkQueries)
->Unit(benchmark::kMillisecond)
->ArgsProduct({{0,1},
	       {0}})
->Iterations(3);

BENCHMARK(BenchmarkQueriesOverhead)
->Unit(benchmark::kMillisecond)
->ArgsProduct({{0,1},
	       {6}})
->Iterations(3);

BENCHMARK(BenchmarkFrameworkOverhead)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1);

BENCHMARK(BenchmarkRanges)
->Unit(benchmark::kMillisecond)
->RangeMultiplier(2)->Range(1, MAX_FETCH_RANGE)
->Iterations(3);

BENCHMARK(BenchmarkInMemConvergence)
->Unit(benchmark::kMillisecond)
->ArgsProduct({{0, 1}, benchmark::CreateRange(1, MAX_NUM_QUERIES, 2)})
->Iterations(3);

BENCHMARK(BenchmarkOverhead)
->Unit(benchmark::kMillisecond)
->RangeMultiplier(10)->Range(1, TOP_OF_RANGE_MAX)
->Iterations(1); //->Iterations(OVERHEAD_ITERATIONS);

BENCHMARK_MAIN();

