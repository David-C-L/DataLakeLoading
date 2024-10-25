#include "BOSSWisentDeserialiserEngine.hpp"
#include <BOSS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <type_traits>
#include <unordered_set>
#include <queue>
#include <future>

// #define DEBUG

#ifdef DEBUG
#ifndef _MSC_VER
#include <cxxabi.h>
#include <memory>
#endif
#endif

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::ExpressionArguments;
using boss::Span;
using boss::Symbol;
using boss::expressions::ExpressionSpanArguments;

// int64_t NUM_THREADS = 40;

boss::Expression applyEngine(Expression &&e, EvalFunction eval) {
  auto *r = new BOSSExpression{std::move(e)};
  auto *oldWrapper = r;
  r = eval(r);
  delete oldWrapper;
  auto result = std::move(r->delegate);
  delete r;
  return std::move(result);
}
  
template <typename T>
double averageDifference(const std::vector<T>& vec) {
    if (vec.size() < 2) {
        // Not enough elements to calculate differences
        return 0.0;
    }

    // Calculate the total sum of differences between adjacent elements
    size_t totalDifference = 0;
    for (size_t i = 1; i < vec.size(); ++i) {
        totalDifference += vec[i] - vec[i - 1];
    }

    // Calculate the average difference
    double average = static_cast<double>(totalDifference) / (vec.size() - 1);
    return average;
}

template <typename T>
std::pair<std::vector<std::pair<T, T>>, std::vector<T>>
createAdjacentIntervalsAndNonAdjacents(const std::vector<T>& input) {
  std::vector<std::pair<T, T>> intervals;
  std::vector<T> nonAdjacent;

  if (input.empty()) {
    return {intervals, nonAdjacent};
  }

  intervals.reserve(input.size() / 2);
  nonAdjacent.reserve(input.size() / 2);

  T start = input[0];
  T prev = input[0];
  bool inInterval = false;
  
  for (size_t i = 1; i < input.size(); i++) {
    if (input[i] == prev + 1) {
      inInterval = true;
    } else {
      if (inInterval) {
	intervals.emplace_back(start, prev);
      } else {
	nonAdjacent.push_back(prev);
      }

      start = input[i];
      inInterval = false;
    }
    prev = input[i];
  }

  if (inInterval) {
    intervals.emplace_back(start, prev);
  } else {
    nonAdjacent.push_back(prev);
  }  
  
  return {intervals, nonAdjacent};
}

template <typename T>
std::vector<std::pair<T, T>> createEvenIntervals(const std::vector<T>& sortedValues, int32_t numRanges) {
  auto n = sortedValues.size();
  std::vector<std::pair<T, T>> result;
  result.reserve(numRanges);
  
  // Edge case: If the number of ranges exceeds the number of values
  if (numRanges >= n) {
    for (size_t i = 0; i < n; i++) {
      result.emplace_back(sortedValues[i], sortedValues[i]);
    }
    return result;
  }

  size_t baseSize = n / numRanges;
  size_t remainder = n % numRanges;

  size_t startI = 0;
  for (size_t rangeI = 0; rangeI < numRanges; rangeI++) {
    size_t endI = startI + baseSize + (rangeI < remainder ? 1 : 0) - 1;
    result.emplace_back(sortedValues[startI], sortedValues[endI]);
    startI = endI + 1;
  }
  
  return std::move(result);
}

template <typename T>
std::vector<std::pair<T, T>> createMinimisedIntervals(const std::vector<T>& sortedValues, int32_t numRanges) {
  auto n = sortedValues.size();
  size_t mergeIndicator = std::numeric_limits<T>::max();
  std::vector<std::pair<T, T>> result;
  result.reserve(numRanges);

  // Edge case: If the number of ranges exceeds the number of values
  if (numRanges >= n) {
    for (size_t i = 0; i < n; i++) {
      result.emplace_back(sortedValues[i], sortedValues[i]);
    }
    return result;
  }

  std::vector<size_t> differences;
  differences.reserve(n);
  for (size_t i = 1; i < n; i++) {
    auto diff = sortedValues[i] - sortedValues[i - 1];
    differences.push_back(diff);
  }

  auto numMergesNeeded = sortedValues.size() - numRanges;

  using Element = std::pair<T, size_t>;
  std::priority_queue<Element, std::vector<Element>, std::greater<Element>> minHeap;

  // Populate the heap with differences and their indices
  for (size_t i = 0; i < differences.size(); i++) {
    minHeap.push({differences[i], i});
  }

  // Merge the smallest differences
  for (size_t i = 0; i < numMergesNeeded; i++) {
    auto [minDiff, minIndex] = minHeap.top();
    minHeap.pop();

    // Mark the difference as merged by setting it to max
    differences[minIndex] = mergeIndicator;
  }

  // Final merging process using the updated differences
  bool merging = false;
  size_t currMergeStart = 0;
  size_t currMergeEnd = 0;

  for (size_t i = 0; i < differences.size(); i++) {
    if (differences[i] == mergeIndicator) {
      if (!merging) {
	currMergeStart = sortedValues[i]; // Start a new merge interval
	currMergeEnd = sortedValues[i + 1];
	merging = true;
      } else {
	currMergeEnd = sortedValues[i + 1]; // Extend the current merge
      }
      if (i == differences.size() - 1) {
	result.emplace_back(currMergeStart, currMergeEnd);
      }
    } else {
      // We ended a merge interval
      if (merging) {
	result.emplace_back(currMergeStart, currMergeEnd);
	merging = false;
      }
      // Add the non-merged element as an interval
      if (i < differences.size() - 1 && differences[i + 1] != mergeIndicator) {
	result.emplace_back(sortedValues[i + 1], sortedValues[i + 1]);
      }
    }
  }

#ifdef DEBUG
  std::cout << "NUM RANGES REQUESTED: " << numRanges << " NUM RANGES PRODUCED: " << result.size() << std::endl;
#endif
  
  return std::move(result);
}

template <typename T>
std::vector<std::pair<T, T>> processIntervalsAsync(const std::vector<T>& sortedList, int32_t numRanges, int32_t numPartitions) {
  size_t n = sortedList.size();
  if (numPartitions > n) {
    numPartitions = n;
  }

  std::vector<std::future<std::vector<std::pair<T, T>>>> futures;
  std::vector<std::pair<T, T>> result;
  result.reserve(numRanges);

  size_t partitionSize = n / numPartitions;
  size_t remainder = n % numPartitions;

  size_t startI = 0;

  std::vector<std::vector<T>> partitions;
  partitions.reserve(numPartitions);
  std::vector<int32_t> partitionRanges;
  partitionRanges.reserve(numPartitions);

  for (size_t partitionI = 0; partitionI < numPartitions; partitionI++) {
    size_t endI = startI + partitionSize + (partitionI < remainder ? 1 : 0);

    std::vector<T> sortedPartitionList(sortedList.begin() + startI, sortedList.begin() + endI);
    int32_t partitionRange = static_cast<int32_t>((static_cast<double_t>(sortedPartitionList.size()) / n) * numRanges);
    if (partitionRange == 0) {
      partitionRange = 1;
    }
    partitions.emplace_back(std::move(sortedPartitionList));
    partitionRanges.emplace_back(std::move(partitionRange));
    startI = endI;
  }

  for (size_t partitionI = 0; partitionI < numPartitions; partitionI++) {
    futures.push_back(std::async(std::launch::async, [&partitions, &partitionRanges, partitionI]() mutable {
      return createMinimisedIntervals<T>(partitions[partitionI], partitionRanges[partitionI]);
    }));
  }

  for (auto& future : futures) {
    auto tempRes = future.get();
    result.insert(result.end(), std::make_move_iterator(tempRes.begin()), std::make_move_iterator(tempRes.end()));
  }

  return std::move(result);
}

boss::Expression
createFetchExpression(const std::string &url,
		      std::vector<int64_t> &bounds, bool trackingCache, int64_t numArgs) {
  auto numBounds = bounds.size();
  auto avgDiff = averageDifference<int64_t>(bounds);
  ExpressionArguments args;

  int64_t padding = 0;
  int64_t alignment = 128;
  int64_t ranges = 200;
  int64_t requests = -1;
  int64_t numThreads = NUM_THREADS;
  if (!trackingCache) {
    // padding = 2 * (int64_t) avgDiff;
    padding = 0;
    alignment = 1;
    ranges = 200;
    requests = -1;
  } else if (numBounds < 2 * ranges) {
    alignment = 1;
  }
  if (numArgs >= 0 && numArgs < 10000000) {
    alignment = 128;
    ranges = 1;
  }

#ifdef DEBUG
  // std::cout << "PADDING: " << padding << std::endl;
  // std::cout << "ALIGNMENT: " << alignment << std::endl;
  // std::cout << "RANGES: " << ranges << std::endl;
  // std::cout << "REQUESTS: " << requests << std::endl;
#endif
  
  args.push_back(std::move("List"_(std::move(boss::Span<int64_t>(bounds)))));
  args.push_back(url);
  args.push_back(padding);
  args.push_back(alignment);
  args.push_back(ranges);
  args.push_back(requests);
  args.push_back(trackingCache);
  args.push_back(numThreads);

  return boss::ComplexExpression("Fetch"_, {}, std::move(args), {});
}

boss::Span<int8_t> getByteSequence(boss::ComplexExpression &&expression) {
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head != "ByteSequence"_) {
    throw std::runtime_error(
        "Unsupported expression: deserialiseByteSequence function does not "
        "support deserialising from non-ByteSequence_ expressions");
  }

  if (spans.empty()) {
    throw std::runtime_error(
        "Wisent Deserialiser: No spans present in ByteSequence_ expression");
  }

  auto &typedSpan = spans[0];
  if (!std::holds_alternative<boss::Span<int8_t>>(typedSpan)) {
    throw std::runtime_error(
        "unsupported span type: ByteSequence span is not an int8_t span");
  }
  auto &byteSpan = std::get<boss::Span<int8_t>>(typedSpan);
  return std::move(byteSpan);
}


namespace boss::engines::WisentDeserialiser {
using EvalFunction = BOSSExpression *(*)(BOSSExpression *);
using std::move;
namespace utilities {} // namespace utilities

#ifdef DEBUG
template <typename T> void print_type_name() {
  const char *typeName = typeid(T).name();

#ifndef _MSC_VER
  // Demangle the type name on GCC/Clang
  int status = -1;
  std::unique_ptr<char, void (*)(void *)> res{
      abi::__cxa_demangle(typeName, nullptr, nullptr, &status), std::free};
  std::cout << (status == 0 ? res.get() : typeName) << std::endl;
#else
  // On MSVC, typeid().name() returns a human-readable name.
  std::cout << typeName << std::endl;
#endif
}
#endif

template <typename T>
T convertFromByteSpan(boss::Span<int8_t> &byteSpan, size_t start) {
  T res = 0;
  std::memcpy(&res, &(byteSpan[start]), sizeof(T));
  return res;
}

boss::Expression
deserialiseByteSequenceCopy(boss::ComplexExpression &&expression) {
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head != "ByteSequence"_) {
    throw std::runtime_error(
        "Unsupported expression: deserialiseByteSequence function does not "
        "support deserialising from non-ByteSequence_ expressions");
  }

  if (spans.empty()) {
    throw std::runtime_error(
        "Wisent Deserialiser: No spans present in ByteSequence_ expression");
  }

  auto &typedSpan = spans[0];
  if (!std::holds_alternative<boss::Span<int8_t>>(typedSpan)) {
    throw std::runtime_error(
        "unsupported span type: ByteSequence span is not an int8_t span");
  }
  auto &byteSpan = std::get<boss::Span<int8_t>>(typedSpan);
  uint64_t const argumentCount = convertFromByteSpan<uint64_t>(byteSpan, 0);
  uint64_t const expressionCount = convertFromByteSpan<uint64_t>(byteSpan, 8);
  uint64_t stringArgumentsFillIndex = convertFromByteSpan<size_t>(byteSpan, 16);

#ifdef DEBUG
  std::cout << "ARG COUNT: " << argumentCount << std::endl;
  std::cout << "EXPR COUNT: " << expressionCount << std::endl;
#endif

  size_t argumentsSize = byteSpan.size() - 32;

  RootExpression *root = [&]() -> RootExpression * {
    RootExpression *root =
        (RootExpression *) // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
        malloc(sizeof(RootExpression) + argumentsSize);
    *((uint64_t *)&root
          ->argumentCount) = // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
        argumentCount;
    *((uint64_t *)&root
          ->expressionCount) = // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
        expressionCount;
    *((uint64_t *)&root
          ->stringArgumentsFillIndex) = // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
        stringArgumentsFillIndex;
    *((void **)&root
          ->originalAddress) = // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
        root;
    return root;
  }();
  memcpy(root->arguments, &byteSpan[32], argumentsSize);
  auto res = SerializedExpression(root).deserialize();
  return std::move(res);
}

boss::Expression
deserialiseByteSequenceMove(boss::ComplexExpression &&expression) {
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head != "ByteSequence"_) {
    throw std::runtime_error(
        "Unsupported expression: deserialiseByteSequence function does not "
        "support deserialising from non-ByteSequence_ expressions");
  }

  if (spans.empty()) {
    throw std::runtime_error(
        "Wisent Deserialiser: No spans present in ByteSequence_ expression");
  }

  auto &typedSpan = spans[0];
  if (!std::holds_alternative<boss::Span<int8_t>>(typedSpan)) {
    throw std::runtime_error(
        "unsupported span type: ByteSequence span is not an int8_t span");
  }
  auto &byteSpan = std::get<boss::Span<int8_t>>(typedSpan);

#ifdef DEBUG
  std::cout << "NO BYTES: " << byteSpan.size() << std::endl;
#endif

  RootExpression *root = (RootExpression *)byteSpan.begin();
  *((void **)&root->originalAddress) = byteSpan.begin();

  auto res = SerializedExpression<nullptr, nullptr, nullptr>(root);
#ifdef DEBUG
  std::cout << "ARG COUNT: " << res.argumentCount() << std::endl;
  std::cout << "EXPR COUNT: " << res.expressionCount() << std::endl;
  
  // std::ofstream file("/home/david/Documents/PhD/datasets/tpc_h_wisent_no_dict_enc/lineitemWisent_1MB_4096.txt", std::ios::binary);
  // if (!file) {
  //   throw std::runtime_error("Error opening file");
  // }
  // file << res << std::endl;
  // file.close();
  // std::cout << "WISENT: \n\n" << res << std::endl;
#endif
  return std::move(std::move(res).deserialize());
}

boss::Expression parseByteSequences(boss::ComplexExpression &&expression) {
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  std::transform(std::make_move_iterator(dynamics.begin()),
                 std::make_move_iterator(dynamics.end()), dynamics.begin(),
                 [&](auto &&arg) {
                   return deserialiseByteSequenceMove(
                       std::move(get<boss::ComplexExpression>(arg)));
                 });

  auto mergeLists = [&](boss::ComplexExpression &&listA,
                        boss::ComplexExpression &&listB) {
    auto [headA, unused_A, dynamicsA, spansA] = std::move(listA).decompose();
    auto [headB, unused_B, dynamicsB, spansB] = std::move(listB).decompose();

    std::transform(std::make_move_iterator(spansB.begin()),
                   std::make_move_iterator(spansB.end()),
                   std::back_inserter(spansA),
                   [](auto &&span) { return std::move(span); });

    return boss::ComplexExpression(std::move(headA), {}, {}, std::move(spansA));
  };
  auto mergeColumns = [&](boss::ComplexExpression &&colA,
                          boss::ComplexExpression &&colB) {
    ExpressionArguments args;
    auto [headA, unused_A, dynamicsA, spansA] = std::move(colA).decompose();
    auto [headB, unused_B, dynamicsB, spansB] = std::move(colB).decompose();
    args.push_back(std::move(
        mergeLists(std::move(get<boss::ComplexExpression>(dynamicsA[0])),
                   std::move(get<boss::ComplexExpression>(dynamicsB[0])))));

    return boss::ComplexExpression(std::move(headA), {}, std::move(args), {});
  };
  auto mergeTables = [&](boss::ComplexExpression &&tableA,
                         boss::ComplexExpression &&tableB) {
    ExpressionArguments args;
    auto [headA, unused_A, dynamicsA, spansA] = std::move(tableA).decompose();
    auto [headB, unused_B, dynamicsB, spansB] = std::move(tableB).decompose();

    auto itA = std::make_move_iterator(dynamicsA.begin());
    auto itB = std::make_move_iterator(dynamicsB.begin());
    for (; itA != std::make_move_iterator(dynamicsA.end()) &&
           itB != std::make_move_iterator(dynamicsB.end());
         ++itA, ++itB) {
      args.emplace_back(std::move(
          mergeColumns(std::move(get<boss::ComplexExpression>(*itA)),
                       std::move(get<boss::ComplexExpression>(*itB)))));
    }

    return boss::ComplexExpression("Table"_, {}, std::move(args), {});
  };

  auto resTable = std::move(dynamics[0]);
  for (auto it = std::make_move_iterator(std::next(dynamics.begin()));
       it != std::make_move_iterator(dynamics.end()); ++it) {
    resTable =
        std::move(mergeTables(std::move(get<boss::ComplexExpression>(resTable)),
                              std::move(get<boss::ComplexExpression>(*it))));
  }
  return std::move(resTable);
}
  
std::vector<boss::Symbol> getColumns(boss::ComplexExpression &&expression) {
  std::vector<boss::Symbol> columns;
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head != "List"_) {
    return columns;
  }

  std::for_each(std::make_move_iterator(dynamics.begin()),
                std::make_move_iterator(dynamics.end()),
                [&columns](auto &&arg) {
                  if (std::holds_alternative<boss::Symbol>(arg)) {
#ifdef DEBUG
                    std::cout << "SELECTED: " << arg << std::endl;
#endif
                    columns.emplace_back(std::get<boss::Symbol>(arg));
                  }
                });

  return std::move(columns);
}

  boss::Expression addIndexColumnToTable(boss::ComplexExpression &&expression, ExpressionSpanArguments &&indices, boss::Symbol &&colHead = "__internal_indices_"_) {
    auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
    if (head != "Table"_) {
      return expression;
    }

    auto listExpr = boss::ComplexExpression("List"_, {}, {}, std::move(indices));
    boss::ExpressionArguments colArgs;
    colArgs.push_back(std::move(listExpr));
    auto indexColExpr = boss::ComplexExpression(std::move(colHead), {}, std::move(colArgs), {});

    dynamics.push_back(std::move(indexColExpr));
    auto tableExpr = boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
    return std::move(tableExpr);
  }

ExpressionSpanArguments
getExpressionSpanArgs(boss::ComplexExpression &&expression) {
  ExpressionSpanArguments spanArgs;
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head == "Table"_) {
    if (dynamics.size() == 0) {
      std::vector<int64_t> filler = {};
      spanArgs.emplace_back(boss::Span<int64_t const>(std::move(filler)));
      return spanArgs;
    }
    return getExpressionSpanArgs(
        std::move(std::get<boss::ComplexExpression>(std::move(dynamics[0]))));
  }
  std::for_each(std::make_move_iterator(dynamics.begin()),
                std::make_move_iterator(dynamics.end()), [&](auto &&arg) {
                  auto [head, unused_, dynamics, spans] =
                      std::move(get<boss::ComplexExpression>(arg)).decompose();
                  std::for_each(std::make_move_iterator(spans.begin()),
                                std::make_move_iterator(spans.end()),
                                [&](auto &&span) {
                                  spanArgs.emplace_back(std::move(span));
                                });
                });
  return std::move(spanArgs);
}

ExpressionSpanArguments
getExpressionSpanArgs(boss::ComplexExpression &&expression,
                      boss::Symbol &&colHead) {
  ExpressionSpanArguments spanArgs;
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head == "Table"_) {
    size_t idx = 0;
    for (; idx < dynamics.size(); idx++) {
      // std::cout << "HEAD: " <<
      // std::get<boss::ComplexExpression>(dynamics[idx]).getHead() << "AT: " <<
      // idx << std::endl;
      if (std::get<boss::ComplexExpression>(dynamics[idx]).getHead() ==
          colHead) {
        // std::cout << "FOUND: " << colHead << "AT: " << idx << std::endl;
        break;
      }
    }
    if (dynamics.size() == idx) {
      std::vector<int64_t> filler = {};
      spanArgs.emplace_back(boss::Span<int64_t const>(std::move(filler)));
      return spanArgs;
    }
    return getExpressionSpanArgs(
        std::move(std::get<boss::ComplexExpression>(std::move(dynamics[idx]))));
  }
  std::for_each(std::make_move_iterator(dynamics.begin()),
                std::make_move_iterator(dynamics.end()), [&](auto &&arg) {
                  auto [head, unused_, dynamics, spans] =
                      std::move(get<boss::ComplexExpression>(arg)).decompose();
                  std::for_each(std::make_move_iterator(spans.begin()),
                                std::make_move_iterator(spans.end()),
                                [&](auto &&span) {
                                  spanArgs.emplace_back(std::move(span));
                                });
                });
  return std::move(spanArgs);
}

ExpressionSpanArguments combineSpanArgs(ExpressionSpanArguments &&args1,
                                        ExpressionSpanArguments &&args2) {
  ExpressionSpanArguments alternatingArgs;
  if (args1.size() != args2.size()) {
    return std::move(alternatingArgs);
  }
  for (size_t i = 0; i < args1.size(); i++) {
    // TODO: Account for incorrect number of indices on each i.e.
    // args1[i].size() != args2[i].size()
    alternatingArgs.push_back(std::move(args1[i]));
    alternatingArgs.push_back(std::move(args2[i]));
  }

  return std::move(alternatingArgs);
}

ExpressionSpanArguments
getExpressionSpanArgsRanged(boss::ComplexExpression &&expression,
                            boss::Symbol &&startCol, boss::Symbol &&endCol) {
  ExpressionSpanArguments spanArgs;
  auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
  if (head == "Table"_) {
    int64_t startIdx = -1;
    int64_t endIdx = -1;
    for (auto i = 0; i < dynamics.size(); i++) {
      if (std::get<boss::ComplexExpression>(dynamics[i]).getHead() ==
          startCol) {
        startIdx = i;
      }
      if (std::get<boss::ComplexExpression>(dynamics[i]).getHead() == endCol) {
        endIdx = i;
      }
    }
    if (startIdx == -1 || endIdx == -1 || startIdx == endIdx) {
      std::vector<int64_t> filler = {};
      spanArgs.emplace_back(boss::Span<int64_t const>(std::move(filler)));
      return spanArgs;
    }
    auto startArgs = getExpressionSpanArgs(
        std::move(get<boss::ComplexExpression>(std::move(dynamics[startIdx]))));
    auto endArgs = getExpressionSpanArgs(
        std::move(get<boss::ComplexExpression>(std::move(dynamics[endIdx]))));
    return std::move(combineSpanArgs(std::move(startArgs), std::move(endArgs)));
  }
  std::vector<int64_t> filler = {};
  spanArgs.emplace_back(boss::Span<int64_t const>(std::move(filler)));
  return spanArgs;
}
  
  ArgumentType Engine::getTypeOfColumn(LazilyDeserializedExpression &lazyExpr, uint64_t startChildOffset, TableManager &tableMan, const std::string &url, EvalFunction &loader) {
    tableMan.loadArgAndTypeOffsets(url, {startChildOffset}, loader);
 
    auto tempLazyChildExpr = lazyExpr[0];
    auto const &type = tempLazyChildExpr.getCurrentExpressionType();
    return type;
  }
  
boss::expressions::ExpressionSpanArgument Engine::getSpanFromIndexRanges(
    LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
    IndicesManager &indicesMan,
    const std::string &url, EvalFunction &loader, bool isSpan) {

  auto exprOffsets = lazyExpr.expression();

  auto const &startChildOffset = exprOffsets.startChildOffset;
  auto const &endChildOffset = exprOffsets.endChildOffset;
  auto const numChildren = endChildOffset - startChildOffset;
  
  auto const &type = getTypeOfColumn(lazyExpr, startChildOffset, tableMan, url, loader);

  auto [startOffsets, sizes] = indicesMan.getArgOffsetsRanged(startChildOffset);
  tableMan.loadArgOffsetsContiguous(url, startOffsets, sizes, loader);

#ifdef DEBUG
  std::cout << "TYPE: " << type << std::endl;
#endif
  if (type == ArgumentType::ARGUMENT_TYPE_STRING ||
      type == ArgumentType::ARGUMENT_TYPE_SYMBOL) {
    auto childStrOffsets = indicesMan.getStringOffsets(lazyExpr, numChildren, isSpan);
    tableMan.loadStringOffsets(url, childStrOffsets, 64, loader);
  }
  return std::move(
		   spanFromRangeFunctors.at(type)(type, lazyExpr, indicesMan));
}
  
boss::expressions::ExpressionSpanArgument Engine::getSpanFromIndices(
    LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
    IndicesManager &indicesMan,
    const std::string &url, EvalFunction &loader, bool isSpan) {

  auto exprOffsets = lazyExpr.expression();

  auto const &startChildOffset = exprOffsets.startChildOffset;
  auto const &endChildOffset = exprOffsets.endChildOffset;
  auto const numChildren = endChildOffset - startChildOffset;
  // std::cout << "NUM INDICES: " << indicesMan.numIndices << std::endl;
#ifdef DEBUG
  std::cout << "NUM INDICES: " << indicesMan.numIndices << std::endl;
#endif
  
  auto const &type = getTypeOfColumn(lazyExpr, startChildOffset, tableMan, url, loader);

  // auto totalRanges = ABSOLUTE_MAX_RANGES * ABSOLUTE_MAX_REQUESTS;
  auto totalRanges = CHANGEABLE_RANGES;
  if (indicesMan.numIndices < totalRanges) {
    auto argOffsets = indicesMan.getArgOffsets(startChildOffset, numChildren);
    tableMan.loadArgOffsets(url, argOffsets, loader);
  } else {
    auto minArgIntervals = indicesMan.getMinArgIntervals(startChildOffset, totalRanges);
    tableMan.loadArgOffsets(url, minArgIntervals, loader);
  }

#ifdef DEBUG
  std::cout << "TYPE: " << type << std::endl;
#endif
  if (type == ArgumentType::ARGUMENT_TYPE_STRING ||
      type == ArgumentType::ARGUMENT_TYPE_SYMBOL) {
    auto childStrOffsets = indicesMan.getStringOffsets(lazyExpr, numChildren, isSpan);
    tableMan.loadStringOffsets(url, childStrOffsets, 2, loader);
  }
  return std::move(spanFunctors.at(type)(type, lazyExpr, indicesMan, numChildren));
}

  boss::Expression Engine::lazyGather(LazilyDeserializedExpression &lazyExpr,
				      TableManager &tableMan,
				      IndicesManager &indicesMan,
				      const std::vector<boss::Symbol> &columns,
				      const std::string &url,
				      EvalFunction &loader) {
  ExpressionArguments args;
  ExpressionSpanArguments spanArgs;

  auto exprOffsets = lazyExpr.expression();
  auto head = lazyExpr.getCurrentExpressionHead();

  auto const &startChildOffset = exprOffsets.startChildOffset;
  auto const &endChildOffset = exprOffsets.endChildOffset;
  auto const numChildren = endChildOffset - startChildOffset;

#ifdef DEBUG
  std::cout << "HEAD: " << head << std::endl;
  std::cout << "START CHILD OFFSET: " << startChildOffset << std::endl;
  std::cout << "END CHILD OFFSET: " << endChildOffset << std::endl;
#endif
  bool isSpan = (head == "List"_);
#ifdef DEBUG
  std::cout << "IS SPAN: " << isSpan << std::endl;
#endif
  if (isSpan) {
    if (indicesMan.isSet) {
      if (!indicesMan.isRanged) {
        spanArgs.push_back(std::move(getSpanFromIndices(
            lazyExpr, tableMan, indicesMan, url, loader, isSpan)));
      } else {
        spanArgs.push_back(std::move(getSpanFromIndexRanges(
           lazyExpr, tableMan, indicesMan, url, loader, isSpan)));
      }
    } else {
      // Assumes same type for all spans and that all children are spans
      
      auto const &type = getTypeOfColumn(lazyExpr, startChildOffset, tableMan, url, loader);
      tableMan.loadArgOffsetsContiguous(url, startChildOffset, numChildren, loader);

      auto lazyChildExpr = lazyExpr[0];
      auto const &size = lazyChildExpr.currentIsRLE();

#ifdef DEBUG
      std::cout << "TYPE: " << type << std::endl;
#endif

      if (size != 0) {
	if (type == ArgumentType::ARGUMENT_TYPE_STRING ||
	    type == ArgumentType::ARGUMENT_TYPE_SYMBOL) {
	  std::vector<size_t> childStrOffsets;
	  childStrOffsets.reserve(numChildren);
	  for (auto i = 0; i < numChildren; i++) {
	    auto lazyStrChildExpr = lazyExpr[i];
	    childStrOffsets.emplace_back(
					 lazyStrChildExpr.getCurrentExpressionAsString(size != 0));
	  }
	  tableMan.loadStringOffsets(url, childStrOffsets, 64, loader);
	}
	spanArgs.push_back(std::move(lazyChildExpr.getCurrentExpressionAsSpanWithTypeAndSize(type, numChildren)));
      }
    }
    
            // int64_t totalSize = 0;

//       for (auto offset = 0; offset < numChildren; offset++) {
// 	std::vector<size_t> firstElemOffsets;
// 	firstElemOffsets.push_back(startChildOffset + offset);
// 	std::vector<int64_t> firstElemBounds = getArgAndTypeBounds(firstElemOffsets, root);
// 	resetRoot(root, byteSpan, url, firstElemBounds, loader);
	
//         auto lazyChildExpr = lazyExpr[offset];
// 	auto const &type = lazyChildExpr.getCurrentExpressionType();
//         auto const &size = lazyChildExpr.currentIsRLE();
	
// 	std::vector<int64_t> argBounds =
//           getArgBoundsContiguous(startChildOffset + offset, size, root);
// 	resetRoot(root, byteSpan, url, argBounds, loader);

//         totalSize += size;
	
// 	std::cout << "LOADED: " << totalSize << " OF " << numChildren << std::endl;
      
//         if (size != 0) {
//           if (type == ArgumentType::ARGUMENT_TYPE_STRING ||
//               type == ArgumentType::ARGUMENT_TYPE_SYMBOL) {
//             std::vector<size_t> childStrOffsets;
//             childStrOffsets.reserve(size);
//             for (auto i = 0; i < size; i++) {
//               auto lazyStrChildExpr = lazyExpr[offset + i];
//               childStrOffsets.emplace_back(
//                   lazyStrChildExpr.getCurrentExpressionAsString(size != 0));
//             }
//             std::vector<int64_t> childStrBounds =
//                 getStringBounds(childStrOffsets, root, 64);
//             resetRoot(root, byteSpan, url, childStrBounds, loader);
//           }
	  
//           spanArgs.push_back(
//               std::move(lazyChildExpr.getCurrentExpressionAsSpan()));
//           offset += size - 1;
//         }
//       }
// #ifdef DEBUG
//       std::cout << "EXPECTED: " << numChildren << std::endl;
//       std::cout << "GOT: " << totalSize << std::endl;
// #endif
  } else {

    tableMan.loadArgAndTypeOffsetsContiguous(url, startChildOffset, numChildren, loader);
    for (auto offset = 0; offset < numChildren; offset++) {

      auto lazyChildExpr = lazyExpr[offset];
#ifdef DEBUG
      std::cout << "CHILD: " << offset
                << " IS EXPR: " << lazyChildExpr.currentIsExpression()
                << std::endl;
#endif
      if (lazyChildExpr.currentIsExpression()) {

	tableMan.loadExprOffsets(url, {startChildOffset + offset}, loader);
	
        auto childExprOffsets = lazyChildExpr.expression();
	tableMan.loadStringOffsets(url, {childExprOffsets.symbolNameOffset}, 16, loader);

        if (head == "Table"_) {
          auto childIsGathered =
              std::find(columns.begin(), columns.end(),
                        lazyChildExpr.getCurrentExpressionHead()) !=
              columns.end();
#ifdef DEBUG
          std::cout << "CHILD HEAD: "
                    << lazyChildExpr.getCurrentExpressionHead()
                    << " IS GATHERED: " << childIsGathered << std::endl;
#endif
          if (columns.empty() || childIsGathered) {
            auto childArg = lazyGather(lazyChildExpr, tableMan, indicesMan,
                                       columns, url, loader);
            args.emplace_back(std::move(childArg));
          }
        } else {

          auto childArg = lazyGather(lazyChildExpr, tableMan, indicesMan,
                                     columns, url, loader);
          args.emplace_back(std::move(childArg));
        }
      } else {

        args.emplace_back(std::move(lazyChildExpr.getCurrentExpression()));
      }
    }
  }

  return boss::ComplexExpression(std::move(head), {}, std::move(args),
                                 std::move(spanArgs));
}

boss::Expression Engine::evaluate(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            if (head == "Parse"_) {
              if (dynamics.empty()) {
                throw std::runtime_error("Wisent Deserialiser: No "
                                         "subexpressions in Parse_ expression");
              }

              if (std::holds_alternative<boss::ComplexExpression>(
                      dynamics[0])) {
                auto &subExpr = std::get<boss::ComplexExpression>(dynamics[0]);
                if (subExpr.getHead() == "ByteSequence"_) {
                  auto res = deserialiseByteSequenceMove(std::move(subExpr));
                  return std::move(res);
                }
              }
            } else if (head == "GetEngineCapabilities"_) {
              return std::move("List"_("Parse"_, "ParseTables"_, "Gather"_,
                                       "GatherRanges"_, "ClearWisentCaches"_));
            } else if (head == "ClearWisentCaches"_) {
	      tableMap.clear();
	      return "CachesCleared"_;
            } else if (head == "ParseTables"_) {
              if (dynamics.empty()) {
                throw std::runtime_error("Wisent Deserialiser: No "
                                         " urls in ParseTables_ expression");
              }
              const auto &loaderPath = get<std::string>(dynamics[0]);
              EvalFunction loader = reinterpret_cast<EvalFunction>(
                  libraries.at(loaderPath).evaluateFunction);
              ExpressionSpanArguments urls;
              if (dynamics.size() == 3) {
                auto colHead = std::move(get<boss::Symbol>(dynamics[1]));
                urls = getExpressionSpanArgs(
                    std::move(get<boss::ComplexExpression>(dynamics[2])),
                    std::move(colHead));
              } else if (dynamics.size() == 2) {
                urls = getExpressionSpanArgs(
                    std::move(get<boss::ComplexExpression>(dynamics[1])));
              }
              std::unordered_set<std::string> seenURLs;
              ExpressionArguments args;
              std::for_each(
                  urls.begin(), urls.end(),
                  [&args, &seenURLs](const auto &span) {
                    if (std::holds_alternative<boss::Span<std::string const>>(
                            span)) {
                      std::for_each(
                          get<boss::Span<std::string const>>(span).begin(),
                          get<boss::Span<std::string const>>(span).end(),
                          [&](auto url) {
                            if (seenURLs.find(url) == seenURLs.end()) {
                              seenURLs.insert(url);
                              args.emplace_back(url);
                            }
                          });
                    } else if (std::holds_alternative<boss::Span<std::string>>(
                                   span)) {
                      std::for_each(get<boss::Span<std::string>>(span).begin(),
                                    get<boss::Span<std::string>>(span).end(),
                                    [&](auto url) {
                                      if (seenURLs.find(url) ==
                                          seenURLs.end()) {
                                        seenURLs.insert(url);
                                        args.emplace_back(url);
                                      }
                                    });
                    }
                  });
              auto fetchExpr =
                  boss::ComplexExpression("Fetch"_, {}, std::move(args), {});
	      auto byteSeqs = applyEngine(std::move(fetchExpr), loader);

              return parseByteSequences(
                  std::move(get<boss::ComplexExpression>(byteSeqs)));
            } else if (head == "Gather"_) {
              if (dynamics.size() < 4 || dynamics.size() > 7) {
                throw std::runtime_error(
                    "Wisent Deserialiser: Gather_ expression must be of form "
                    "Gather_[url, loaderPath, List_[List_[spanIndices...]]], "
                    "List_[column...], maxRanges?, indicesColumnName?, numThreads?]");
              }
              const auto &url = get<std::string>(dynamics[0]);
              const auto &loaderPath = get<std::string>(dynamics[1]);
              EvalFunction loader = reinterpret_cast<EvalFunction>(
                  libraries.at(loaderPath).evaluateFunction);
              auto indices = getExpressionSpanArgs(std::move(
                  get<boss::ComplexExpression>(std::move(dynamics[2]))));
              auto columns = getColumns(std::move(
                  get<boss::ComplexExpression>(std::move(dynamics[3]))));

              maxRanges = DEFAULT_MAX_RANGES;
              if (dynamics.size() > 4) {
		CHANGEABLE_RANGES = get<int64_t>(dynamics[4]);
		maxRanges = -1;
                // maxRanges = get<int64_t>(dynamics[4]);
                // maxRanges = maxRanges < 1 ? DEFAULT_MAX_RANGES : maxRanges;
              }

	      boss::Symbol indicesColumnName = "__internal_indices_"_;
	      if (dynamics.size() > 5) {
	        indicesColumnName = std::move(get<boss::Symbol>(dynamics[5]));
	      }

	      if (dynamics.size() > 6) {
		NUM_THREADS = get<int64_t>(dynamics[6]);
	      }

	      auto tableIt = tableMap.find(url);
	      if (tableIt == tableMap.end()) {
		tableMap.emplace(url, Engine::TableManager(url, loader));
	      }
	      auto &tableMan = tableMap[url];
	      tableMan.loadArgAndTypeOffsets(url, {0}, loader);
	      tableMan.loadExprOffsets(url, {0}, loader);

	      RootExpression *&root = tableMan.root;

	      auto expr = SerializedExpression<nullptr, nullptr, nullptr>(root);
	      auto lazyExpr = expr.lazilyDeserialize();

              auto exprOffsets = lazyExpr.expression();
	      tableMan.loadStringOffsets(url, {exprOffsets.symbolNameOffset}, 16, loader);
	      
	      IndicesManager indicesMan = IndicesManager(std::move(indices), false);
              auto table = get<boss::ComplexExpression>(lazyGather(lazyExpr, tableMan, indicesMan, columns,
								   url, loader));
	      if (indicesMan.indices.empty()) {
		return std::move(table);
	      }
	      auto idxArgs = indicesMan.getIndicesAsExpressionSpanArguments();
	      auto res = addIndexColumnToTable(std::move(table), std::move(idxArgs), std::move(indicesColumnName));
              return std::move(res);
            } else if (head == "GatherRanges"_) {
              if (dynamics.size() < 6 || dynamics.size() > 7) {
                throw std::runtime_error(
                    "Wisent Deserialiser: Gather_ expression must be of form "
                    "Gather_[url, loaderPath, startColName, endColName, "
                    "Table_[col[List_[spanIndices...]]]], "
                    "List_[column...], maxRanges?]");
              }
              const auto &url = get<std::string>(dynamics[0]);
              const auto &loaderPath = get<std::string>(dynamics[1]);
              auto startColName = std::move(get<boss::Symbol>(dynamics[2]));
              auto endColName = std::move(get<boss::Symbol>(dynamics[3]));
              EvalFunction loader = reinterpret_cast<EvalFunction>(
                  libraries.at(loaderPath).evaluateFunction);
              auto indices = getExpressionSpanArgsRanged(
                  std::move(
                      get<boss::ComplexExpression>(std::move(dynamics[4]))),
                  std::move(startColName), std::move(endColName));
              auto columns = getColumns(std::move(
                  get<boss::ComplexExpression>(std::move(dynamics[5]))));

              maxRanges = DEFAULT_MAX_RANGES;
              if (dynamics.size() > 6) {
                maxRanges = get<int64_t>(dynamics[6]);
                maxRanges = maxRanges < 1 ? DEFAULT_MAX_RANGES : maxRanges;
              }
	      
	      auto tableIt = tableMap.find(url);
	      if (tableIt == tableMap.end()) {
		tableMap.emplace(url, Engine::TableManager(url, loader));
	      }
	      auto &tableMan = tableMap[url];
	      tableMan.loadArgAndTypeOffsets(url, {0}, loader);
	      tableMan.loadExprOffsets(url, {0}, loader);

	      RootExpression *&root = tableMan.root;

              auto expr = SerializedExpression<nullptr, nullptr, nullptr>(root);
              auto lazyExpr = expr.lazilyDeserialize();

              auto exprOffsets = lazyExpr.expression();
	      tableMan.loadStringOffsets(url, {exprOffsets.symbolNameOffset}, 16, loader);
	      
	      IndicesManager indicesMan = IndicesManager(std::move(indices), true);
              auto res = lazyGather(lazyExpr, tableMan, indicesMan, columns,
                                    url, loader);
              return std::move(res);
            }
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return evaluate(std::forward<decltype(arg)>(arg));
                           });
            return boss::ComplexExpression(
                std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}
} // namespace boss::engines::WisentDeserialiser

static auto &enginePtr(bool initialise = true) {
  static auto engine =
      std::unique_ptr<boss::engines::WisentDeserialiser::Engine>();
  if (!engine && initialise) {
    engine.reset(new boss::engines::WisentDeserialiser::Engine());
  }
  return engine;
}

extern "C" BOSSExpression *evaluate(BOSSExpression *e) {
  static std::mutex m;
  std::lock_guard lock(m);
  auto *r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
