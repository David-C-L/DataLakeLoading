#pragma once
#ifndef WISENT_DESERIALISER_H
#define WISENT_DESERIALISER_H
#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Serialization.hpp>
#include <filesystem>
#include <boost/dynamic_bitset.hpp>
#ifndef _WIN32
#include <dlfcn.h>
#else
#include <filesystem>
#include <iostream>
#define NOMINMAX // max macro in minwindef.h interfering with std::max...
#include <windows.h>
constexpr static int RTLD_NOW = 0;
constexpr static int RTLD_NODELETE = 0;

// #define HEADERDEBUG

static void *dlopen(LPCSTR lpLibFileName, int /*flags*/) {
  void *libraryPtr = LoadLibrary(lpLibFileName);
  if (libraryPtr != nullptr) {
    return libraryPtr;
  }
  // if it failed to load the standard way (searching dependent dlls in the exe
  // path) try one more time, with loading the dependent dlls from the dll path
  auto filepath = ::std::filesystem::path(lpLibFileName);
  if (filepath.is_absolute()) {
    return LoadLibraryEx(lpLibFileName, NULL, LOAD_WITH_ALTERED_SEARCH_PATH);
  } else {
    auto absFilepath = ::std::filesystem::absolute(filepath).string();
    LPCSTR lpAbsFileName = absFilepath.c_str();
    return LoadLibraryEx(lpAbsFileName, NULL, LOAD_WITH_ALTERED_SEARCH_PATH);
  }
}
static auto dlclose(void *hModule) {
  auto resetFunction = GetProcAddress((HMODULE)hModule, "reset");
  if (resetFunction != NULL) {
    (*reinterpret_cast<void (*)()>(resetFunction))();
  }
  return FreeLibrary((HMODULE)hModule);
}
static auto dlerror() {
  auto errorCode = GetLastError();
  LPSTR pBuffer = NULL;
  auto msg =
      FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS |
                        FORMAT_MESSAGE_ALLOCATE_BUFFER,
                    NULL, errorCode, 0, (LPSTR)&pBuffer, 0, NULL);
  if (msg > 0) {
    // Assign buffer to smart pointer with custom deleter so that memory gets
    // released in case String's constructor throws an exception.
    auto deleter = [](void *p) { ::LocalFree(p); };
    ::std::unique_ptr<TCHAR, decltype(deleter)> ptrBuffer(pBuffer, deleter);
    return "(" + ::std::to_string(errorCode) + ") " +
           ::std::string(ptrBuffer.get(), msg);
  }
  return ::std::to_string(errorCode);
}
static void *dlsym(void *hModule, LPCSTR lpProcName) {
  return GetProcAddress((HMODULE)hModule, lpProcName);
}
#endif // _WIN32

constexpr static int64_t FETCH_PADDING = 0;
constexpr static int64_t FETCH_ALIGNMENT = 4096;
constexpr static int64_t DEFAULT_MAX_RANGES = 512;
static inline const int64_t ABSOLUTE_MAX_RANGES = 255;
static inline const int64_t ABSOLUTE_MAX_REQUESTS = 350;

extern int64_t CHANGEABLE_RANGES;
int64_t CHANGEABLE_RANGES = ABSOLUTE_MAX_RANGES;

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::ExpressionArguments;
using boss::Span;
using boss::Symbol;
using boss::expressions::ExpressionSpanArguments;
using boss::Expression;
using boss::serialization::Argument;
using boss::serialization::ArgumentType;
using boss::serialization::RootExpression;
using boss::serialization::SerializedExpression;
using LazilyDeserializedExpression = boss::serialization::SerializedExpression<
    nullptr, nullptr, nullptr>::LazilyDeserializedExpression;
using SerializationExpression = boss::serialization::Expression;

using EvalFunction = BOSSExpression *(*)(BOSSExpression *);

template <typename T>
std::vector<std::pair<T, T>> createMinimisedIntervals(const std::vector<T>& sortedValues, int32_t numRanges);

template <typename T>
std::pair<std::vector<std::pair<T, T>>, std::vector<T>>
createAdjacentIntervalsAndNonAdjacents(const std::vector<T>& sortedValues);

boss::Expression
createFetchExpression(const std::string &url,
		      std::vector<int64_t> &bounds, bool trackingCache);

boss::Expression applyEngine(Expression &&e, EvalFunction eval);

boss::Span<int8_t> getByteSequence(boss::ComplexExpression &&expression);
namespace boss::engines::WisentDeserialiser {

class Engine {

public:
  Engine(Engine &) = delete;

  Engine &operator=(Engine &) = delete;

  Engine(Engine &&) = default;

  Engine &operator=(Engine &&) = delete;

  Engine() = default;

  ~Engine() = default;

  boss::Expression evaluate(boss::Expression &&e);

  struct LibraryAndEvaluateFunction {
    void *library, *evaluateFunction;
  };

  struct LibraryCache
      : private ::std::unordered_map<::std::string,
                                     LibraryAndEvaluateFunction> {
    LibraryAndEvaluateFunction const &at(::std::string const &libraryPath) {
      if (count(libraryPath) == 0) {
        const auto *n = libraryPath.c_str();
        if (auto *library = dlopen(
                n, RTLD_NOW | RTLD_NODELETE)) { // NOLINT(hicpp-signed-bitwise)
          if (auto *sym = dlsym(library, "evaluate")) {
            emplace(libraryPath, LibraryAndEvaluateFunction{library, sym});
          } else {
            throw ::std::runtime_error(
                "library \"" + libraryPath +
                "\" does not provide an evaluate function: " + dlerror());
          }
        } else {
          throw ::std::runtime_error("library \"" + libraryPath +
                                     "\" could not be loaded: " + dlerror());
        }
      };
      return unordered_map::at(libraryPath);
    }
    ~LibraryCache() {
      for (const auto &[name, library] : *this) {
        dlclose(library.library);
      }
    }

    LibraryCache() = default;
    LibraryCache(LibraryCache const &) = delete;
    LibraryCache(LibraryCache &&) = default;
    LibraryCache &operator=(LibraryCache const &) = delete;
    LibraryCache &operator=(LibraryCache &&) = delete;
  } libraries;

private:

  struct TableManager {

    boss::Span<int8_t> byteSpan;
    RootExpression *root;
    size_t argumentCount;
    
    boost::dynamic_bitset<uint8_t> loadedArguments;

    void updateSpan(const std::string &url, std::vector<int64_t> &bounds, EvalFunction &loader, bool trackingCache) {      
      auto fetchExpr = createFetchExpression(url, bounds, trackingCache);
      auto byteSeqExpr = std::get<boss::ComplexExpression>(applyEngine(std::move(fetchExpr), loader));
      byteSpan = std::move(getByteSequence(std::move(byteSeqExpr)));
    }

    void updateRoot(const std::string &url, std::vector<int64_t> &bounds, EvalFunction &loader, bool trackingCache) {
      updateSpan(url, bounds, loader, trackingCache);
      root = (RootExpression *)byteSpan.begin();
      *((void **)&root->originalAddress) = root;
    }

    std::vector<int64_t> getArgBounds(std::vector<size_t> const &offsets) {
      std::vector<int64_t> bounds;
      bounds.reserve(offsets.size() * 2);
      size_t currByteOffset = sizeof(RootExpression);
      for (auto offset : offsets) {
	// Argument Array
	bounds.emplace_back(currByteOffset + (offset * sizeof(Argument)) - 1);
	bounds.emplace_back(currByteOffset + ((offset + 1) * sizeof(Argument)) + 1);
      }

      return bounds;
    }

    std::vector<int64_t> getArgAndTypeBounds(std::vector<size_t> const &offsets) {
      std::vector<int64_t> bounds;
      bounds.reserve(offsets.size() * 4);
      size_t currByteOffset = sizeof(RootExpression);
      for (auto offset : offsets) {
	// Argument Array
	bounds.emplace_back(currByteOffset + (offset * sizeof(Argument)) - 1);
	bounds.emplace_back(currByteOffset + ((offset + 1) * sizeof(Argument)) + 1);
      }
      currByteOffset += root->argumentCount * sizeof(Argument);
      for (auto offset : offsets) {
	// ArgumentType Array
	bounds.emplace_back(currByteOffset + (offset * sizeof(ArgumentType)) - 1);
	bounds.emplace_back(currByteOffset + ((offset + 1) * sizeof(ArgumentType)) + 1);
      }

      return bounds;
    }

    std::vector<int64_t> getArgBoundsFromIntervals(std::vector<std::pair<size_t, size_t>> const &intervals) {
      std::vector<int64_t> bounds;
      bounds.reserve(intervals.size() * 2);
      size_t currByteOffset = sizeof(RootExpression);
      for (auto const &[start, end] : intervals) {
	// Argument Array
	bounds.emplace_back(currByteOffset + (start * sizeof(Argument)) - 1);
	bounds.emplace_back(currByteOffset + ((end + 1) * sizeof(Argument)) + 1);
      }

      return bounds;
    }

    std::vector<int64_t> getArgBoundsContiguous(size_t const &startOffset,
						size_t const &size) {
      std::vector<int64_t> bounds;
      auto endOffset = startOffset + size - 1;

      size_t currByteOffset = sizeof(RootExpression);
      // Argument Array
      bounds.emplace_back(currByteOffset + (startOffset * sizeof(Argument)) - 1);
      bounds.emplace_back(currByteOffset + ((endOffset + 1) * sizeof(Argument)) + 1);

      return bounds;
    }

    std::vector<int64_t> getArgAndTypeBoundsContiguous(size_t const &startOffset,
						       size_t const &size) {
      std::vector<int64_t> bounds;
      auto endOffset = startOffset + size - 1;

      size_t currByteOffset = sizeof(RootExpression);
      // Argument Array
      bounds.emplace_back(currByteOffset + (startOffset * sizeof(Argument)) - 1);
      bounds.emplace_back(currByteOffset + ((endOffset + 1) * sizeof(Argument)) + 1);
      // ArgumentType Array
      currByteOffset += root->argumentCount * sizeof(Argument);
      bounds.emplace_back(currByteOffset + (startOffset * sizeof(ArgumentType)) - 1);
      bounds.emplace_back(currByteOffset +
			  ((endOffset + 1) * sizeof(ArgumentType)) + 1);

      return bounds;
    }

    std::vector<int64_t> getExprBounds(std::vector<size_t> const &offsets) {
      std::vector<int64_t> bounds;
      bounds.reserve(offsets.size() * 2);
      for (auto offset : offsets) {
	size_t currByteOffset = sizeof(RootExpression) +
	  root->argumentCount * sizeof(Argument) +
	  root->argumentCount * sizeof(ArgumentType);
	// Expression Array
	bounds.emplace_back(currByteOffset +
			    (offset * sizeof(SerializationExpression)) - 1);
	bounds.emplace_back(currByteOffset +
			    ((offset + 1) * sizeof(SerializationExpression)) + 1);
      }
      return bounds;
    }

    std::vector<int64_t> getStringBounds(std::vector<size_t> const &offsets,
					 size_t approxLength) {
      std::vector<int64_t> bounds;
      bounds.reserve(offsets.size() * 2);
      for (auto offset : offsets) {
	size_t currByteOffset =
	  sizeof(RootExpression) + root->argumentCount * sizeof(Argument) +
	  root->argumentCount * sizeof(ArgumentType) +
	  root->expressionCount * sizeof(SerializationExpression);
	// String Array
	bounds.emplace_back(currByteOffset + offset - 1);
	bounds.emplace_back(currByteOffset + offset + approxLength + 1);
      }

      return bounds;
    }
  
    std::vector<size_t> requestArguments(const std::vector<size_t> &indicesOffsets) {
      std::vector<size_t> needed;

      for (auto const &offset : indicesOffsets) {
	if (!loadedArguments.test(offset)) {
	  needed.emplace_back(offset);
	  loadedArguments.set(offset);
	}
      }

      return std::move(needed);
    }
    
    std::vector<std::pair<size_t, size_t>> requestArgumentRanges(const std::vector<std::pair<size_t, size_t>> &intervalOffsets) {
      std::vector<std::pair<size_t, size_t>> needed;

      for (auto const &[start, end] : intervalOffsets) {
	// add check for overlap split while keeping amount of ranges the same
	if (!loadedArguments.test(start) && !loadedArguments.test(end)) {
	  for (auto i = start; i <= end; i++) {
	    loadedArguments.set(i);
	  }
	  needed.emplace_back(start, end);
	}
      }
      return std::move(needed);
    }

    void markArgumentsContiguous(size_t startOffset, size_t size) {
      size_t endOffset = startOffset + size;
      // boost::dynamic_bitset<uint8_t> mask(loadedArguments.size());
      // mask >>= (loadedArguments.size() - endOffset);
      // mask <<= startOffset;

      // loadedArguments |= mask;
      
      for (auto currOffset = startOffset; currOffset < startOffset + size; currOffset++) {
	//if (!loadedArguments.test(currOffset))
	loadedArguments.set(currOffset);
      }
    }

    void loadArgOffsets(const std::string &url, const std::vector<size_t> &offsets, EvalFunction &loader) {
      // auto totalRanges = ABSOLUTE_MAX_RANGES * ABSOLUTE_MAX_REQUESTS;
      auto totalRanges = CHANGEABLE_RANGES * ABSOLUTE_MAX_REQUESTS;
      std::vector<int64_t> argBounds;
      if (offsets.size() < totalRanges) {
	auto needed = requestArguments(offsets);
	argBounds = getArgBounds(needed);
      } else {
	auto intervals = createMinimisedIntervals<size_t>(offsets, totalRanges);
       	auto neededIntervals = requestArgumentRanges(intervals);
	argBounds = getArgBoundsFromIntervals(neededIntervals);
      }
#ifdef HEADERDEBUG
      std::cout << "TOTAL RANGES: " << totalRanges << " NUM BOUNDS: " << argBounds.size() << std::endl;
#endif
      updateRoot(url, argBounds, loader, false);
    }

    void loadArgOffsets(const std::string &url, const std::vector<std::pair<size_t, size_t>> &intervals, EvalFunction &loader) {
      std::cout << "NUM RANGES REQUESTED: " << intervals.size() << std::endl;
      auto neededIntervals = requestArgumentRanges(intervals);
      std::cout << "NUM RANGES NEEDED: " << neededIntervals.size() << std::endl;
      auto argBounds = getArgBoundsFromIntervals(neededIntervals);
      std::cout << "NUM BOUNDS: " << argBounds.size() << std::endl;
#ifdef HEADERDEBUG
      // auto totalRanges = ABSOLUTE_MAX_RANGES * ABSOLUTE_MAX_REQUESTS;
      auto totalRanges = CHANGEABLE_RANGES * ABSOLUTE_MAX_REQUESTS;
      std::cout << "TOTAL RANGES: " << totalRanges << " NUM BOUNDS: " << argBounds.size() << std::endl;
#endif
      updateRoot(url, argBounds, loader, false);
    }
    
    void loadArgAndTypeOffsets(const std::string &url, const std::vector<size_t> &offsets, EvalFunction &loader) {
      auto neededOffsets = requestArguments(offsets);
      auto argBounds = getArgAndTypeBounds(neededOffsets);
      updateRoot(url, argBounds, loader, true);
    }

    void loadArgOffsetsContiguous(const std::string &url, size_t startOffset, size_t size, EvalFunction &loader) {
      markArgumentsContiguous(startOffset, size);
      auto argBounds = getArgBoundsContiguous(startOffset, size);
      updateRoot(url, argBounds, loader, true);
    }

    void loadArgAndTypeOffsetsContiguous(const std::string &url, size_t startOffset, size_t size, EvalFunction &loader) {
      markArgumentsContiguous(startOffset, size);
      auto argBounds = getArgAndTypeBoundsContiguous(startOffset, size);
      updateRoot(url, argBounds, loader, true);
    }

    void loadArgOffsetsContiguous(const std::string &url, std::vector<size_t> startOffsets, std::vector<size_t> sizes, EvalFunction &loader) {
      if (startOffsets.size() != sizes.size()) {
	return;
      }
      std::vector<int64_t> argBounds;
      for (auto i = 0; i < startOffsets.size(); i++) {
	auto startOffset = startOffsets[i];
	auto size = sizes[i];
	markArgumentsContiguous(startOffset, size);
	auto tempBounds = getArgBoundsContiguous(startOffset, size);
        argBounds.reserve(argBounds.size() + tempBounds.size());
        argBounds.insert(argBounds.end(),
                         std::make_move_iterator(tempBounds.begin()),
                         std::make_move_iterator(tempBounds.end()));
      }
      updateRoot(url, argBounds, loader, true);
    }
    
    void loadArgAndTypeOffsetsContiguous(const std::string &url, std::vector<size_t> startOffsets, std::vector<size_t> sizes, EvalFunction &loader) {
      if (startOffsets != sizes) {
	return;
      }
      std::vector<int64_t> argBounds;
      for (auto i = 0; i < startOffsets.size(); i++) {
	auto startOffset = startOffsets[i];
	auto size = sizes[i];
	markArgumentsContiguous(startOffset, size);
	auto tempBounds = getArgAndTypeBoundsContiguous(startOffset, size);
        argBounds.reserve(argBounds.size() + tempBounds.size());
        argBounds.insert(argBounds.end(),
                         std::make_move_iterator(tempBounds.begin()),
                         std::make_move_iterator(tempBounds.end()));
      }
      updateRoot(url, argBounds, loader, true);
    }

    void loadStringOffsets(const std::string &url, const std::vector<size_t> &offsets, size_t approxLength, EvalFunction &loader) {
      auto strBounds = getStringBounds(offsets, approxLength);
      updateRoot(url, strBounds, loader, true);
    }

    void loadExprOffsets(const std::string &url, const std::vector<size_t> &offsets, EvalFunction &loader) {
      auto exprBounds = getExprBounds(offsets);
      updateRoot(url, exprBounds, loader, true);
    }

    explicit TableManager(const std::string &url, EvalFunction &loader) {
      std::vector<int64_t> metadataBounds = {0, 32};
      updateRoot(url, metadataBounds, loader, true);
      argumentCount = root->argumentCount;
      loadedArguments.resize(argumentCount, 0);
    }

    TableManager(TableManager const&) = delete;
    TableManager &operator=(TableManager const&) = delete;
    
    TableManager(TableManager &&other) noexcept
      : argumentCount(other.argumentCount)
    {
      byteSpan = std::move(other.byteSpan);
      root = (RootExpression *)byteSpan.begin();
      *((void **)&root->originalAddress) = root;
      loadedArguments = std::move(other.loadedArguments);
      other.root = nullptr;
      other.argumentCount = 0;
    };
    TableManager &operator=(TableManager &&other) noexcept {
      if (this != &other) {
	byteSpan = std::move(other.byteSpan);
	root = (RootExpression *)byteSpan.begin();
	*((void **)&root->originalAddress) = root;
	argumentCount = other.argumentCount;
	loadedArguments = std::move(other.loadedArguments);
	other.root = nullptr;
	other.argumentCount = 0;
      }
      return *this;
    };
    TableManager() = default;
    ~TableManager() = default;
  };

  
  struct IndicesManager {

    std::vector<int64_t> indices;
    std::unordered_map<int32_t, std::vector<std::pair<int64_t, int64_t>>> minimisedIntervalsMap;
    size_t numIndices;
    bool isRanged;
    bool isSet;

    
    size_t countIndices(const ExpressionSpanArguments &indices) {
      return std::accumulate(
			     indices.begin(), indices.end(), (size_t)0,
			     [](auto runningSum, auto const &argument) {
			       if (std::holds_alternative<boss::Span<int64_t const>>(argument)) {
				 return runningSum + get<boss::Span<int64_t const>>(argument).size();
			       } else if (std::holds_alternative<boss::Span<int64_t>>(argument)) {
				 return runningSum + get<boss::Span<int64_t>>(argument).size();
			       }
			       return runningSum;
			     });
    } 
    
    bool isValidIndexType(const ExpressionSpanArguments &indices) {
      if (indices.empty()) {
	return false;
      }
      bool isConstInt64 =
	std::holds_alternative<boss::Span<int64_t const>>(indices[0]) &&
	get<boss::Span<int64_t const>>(indices[0]).size() > 0;
      bool isInt64 = std::holds_alternative<boss::Span<int64_t>>(indices[0]) &&
	get<boss::Span<int64_t>>(indices[0]).size() > 0;
      return isConstInt64 || isInt64;
    }
    
    std::vector<int64_t> getIndicesFromSpanArgs(const ExpressionSpanArguments &indices) {
      size_t numIndices = countIndices(indices);
      std::vector<int64_t> res;
      res.reserve(numIndices);
      std::for_each(
		    indices.begin(), indices.end(),
		    [&res](const auto &span) {
		      if (std::holds_alternative<boss::Span<int64_t const>>(span)) {
			std::for_each(get<boss::Span<int64_t const>>(span).begin(),
				      get<boss::Span<int64_t const>>(span).end(),
				      [&res](auto indice) {
					res.emplace_back(indice);
				      });
		      } else if (std::holds_alternative<boss::Span<int64_t>>(span)) {
			std::for_each(get<boss::Span<int64_t>>(span).begin(),
				      get<boss::Span<int64_t>>(span).end(),
				      [&res](auto indice) {
					res.emplace_back(indice);
				      });
		      }
		    });
    
      return std::move(res);
    }

    std::vector<int64_t> getIndicesFromSpanArgsRanged(const ExpressionSpanArguments &indices) {
      size_t numIndices = countIndices(indices);
      std::vector<int64_t> res;
      res.reserve(numIndices);

      for (size_t i = 0; i < indices.size(); i += 2) {
	if (std::holds_alternative<boss::Span<int64_t const>>(indices[i])) {
	  auto &starts = get<boss::Span<int64_t const>>(indices[i]);
	  auto &ends = get<boss::Span<int64_t const>>(indices[i + 1]);
	  if (starts.size() != ends.size()) {
	    std::cerr << "INCORRECT SIZING IN RANGE SPANS" << std::endl;
	  }
	  for (size_t j = 0; j < starts.size(); j++) {
	    auto &start = starts[j];
	    auto &end = ends[j];
	    size_t size = end - start;
	    res.emplace_back(start);
	    res.emplace_back(size);
	  }
	  
	} else if (std::holds_alternative<boss::Span<int64_t>>(indices[i])) {
	  auto &starts = get<boss::Span<int64_t>>(indices[i]);
	  auto &ends = get<boss::Span<int64_t>>(indices[i + 1]);
	  if (starts.size() != ends.size()) {
	    std::cerr << "INCORRECT SIZING IN RANGE SPANS" << std::endl;
	  }
	  for (size_t j = 0; j < starts.size(); j++) {
	    auto &start = starts[j];
	    auto &end = ends[j];
	    size_t size = end - start;
	    res.emplace_back(start);
	    res.emplace_back(size);
	  }
	}
      }

      return std::move(res);
    }

    size_t countIndicesVector() {
      if (isRanged) {
	size_t totalIndicesCount = 0;
	for (auto i = 1; i < indices.size(); i += 2) {
	  totalIndicesCount += indices[i];
	}
	return totalIndicesCount;
      }
      return indices.size();
    }

    void setIndicesFromSpans(ExpressionSpanArguments &&indicesSpans) {
      if (isRanged) {
	indices = std::move(getIndicesFromSpanArgsRanged(indicesSpans));
      } else {
	indices = std::move(getIndicesFromSpanArgs(indicesSpans));
      }
    }
    
    explicit IndicesManager(ExpressionSpanArguments &&indicesSpans, bool isRanged) : isRanged(isRanged) {
      if (indicesSpans.empty()) {
	isSet = false;
      } else {
	if (isValidIndexType(indicesSpans)) {
	  setIndicesFromSpans(std::move(indicesSpans));
	  isSet = true;
	  numIndices = countIndicesVector();
	  std::sort(indices.begin(), indices.end());
	  indices.erase(std::unique(indices.begin(), indices.end()), indices.end());
#ifdef HEADERDEBUG
	  std::cout << "NUM INDICES: " << numIndices << " IS RANGED: " << isRanged << std::endl;
#endif
	}
      }
    }

    ExpressionSpanArguments getIndicesAsExpressionSpanArguments() {      
      ExpressionSpanArguments args;
      if (!isSet) {
	throw std::runtime_error("Attempt to call getIndicesAsExpressionSpanArguments on Unset IndicesManager. Operation is not possible.");
      }
      if (isRanged) {
	std::vector<int64_t> fullIndices;
	fullIndices.reserve(numIndices);
	for (auto i = 0; i < indices.size(); i += 2) {
	  auto &start = indices[i];
	  auto &size = indices[i + 1];
	  for (auto j = 0; j < size; j++) {
	    fullIndices.emplace_back(start + j);
	  }
	}
	args.emplace_back(boss::Span<int64_t>(std::move(fullIndices)));
      } else {
	args.emplace_back(boss::Span<int64_t>(std::move(indices)));
      }
      return std::move(args);
    }

    std::vector<std::pair<int64_t, int64_t>> getMinimisedIntervals(int32_t numRanges) {
      if (!isSet) {
	throw std::runtime_error("Attempt to call getMinimisedIntervals on Unset IndicesManager. Operation is not possible.");
      }
      if (isRanged) {
	throw std::runtime_error("Attempt to produce minimised intervals for ranged indices. Operation is not possible.");
      }
      auto minIntervalsIt = minimisedIntervalsMap.find(numRanges);
      if (minIntervalsIt == minimisedIntervalsMap.end()) {
	auto [intervals, nonAdjacent] = createAdjacentIntervalsAndNonAdjacents(indices);
	if (intervals.size() < numRanges) {
	  auto minNonAdjIntervals = createMinimisedIntervals(nonAdjacent, numRanges - intervals.size());
	  if (intervals.size() > minNonAdjIntervals.size()) {
	    intervals.reserve(intervals.size() + minNonAdjIntervals.size());
	    intervals.insert(intervals.end(),
			     std::make_move_iterator(minNonAdjIntervals.begin()),
			     std::make_move_iterator(minNonAdjIntervals.end()));
	    minimisedIntervalsMap.emplace(numRanges, std::move(intervals));
	  } else {
	    minNonAdjIntervals.reserve(minNonAdjIntervals.size() + intervals.size());
	    minNonAdjIntervals.insert(minNonAdjIntervals.end(),
			     std::make_move_iterator(intervals.begin()),
			     std::make_move_iterator(intervals.end()));
	    minimisedIntervalsMap.emplace(numRanges, std::move(minNonAdjIntervals));
	  }
	} else {
	  auto minIntervals = createMinimisedIntervals(indices, numRanges);
	  minimisedIntervalsMap.emplace(numRanges, std::move(minIntervals));
	}
      }
      auto &minIntervals = minimisedIntervalsMap[numRanges];
      return minIntervals;
    }

    std::vector<size_t> getArgOffsets(size_t startChildOffset, size_t numChildren) {
      if (!isSet) {
	throw std::runtime_error("Attempt to call getArgOffsets on Unset IndicesManager. Operation is not possible.");
      }
      if (isRanged) {
	throw std::runtime_error("Attempt to call getArgOffsets for ranged indices. Operation is not possible. Try getArgOffsetsRanged.");
      }
      std::vector<size_t> resOffsets;
      resOffsets.reserve(numIndices);
      std::transform(indices.begin(), indices.end(), std::back_inserter(resOffsets),
		     [&startChildOffset, &numChildren](const auto &index) {
		       return (index < 0 || index > numChildren) ?
			 startChildOffset :
			 startChildOffset + index;
		     });
      return std::move(resOffsets);
    }
    
    std::vector<std::pair<size_t, size_t>> getMinArgIntervals(size_t startChildOffset, int32_t numRanges) {
      if (!isSet) {
	throw std::runtime_error("Attempt to call getMinArgIntervals on Unset IndicesManager. Operation is not possible.");
      }
      if (isRanged) {
	throw std::runtime_error("Attempt to call getArgOffsets for ranged indices. Operation is not possible. Try getArgOffsetsRanged.");
      }
      std::vector<std::pair<size_t, size_t>> minimisedArgIntervals;
      const auto &minIntervals = getMinimisedIntervals(numRanges);
      minimisedArgIntervals.reserve(minIntervals.size());

      // std::cout << "STARTCHILDOFFSET: " << startChildOffset << std::endl;
      // std::stringstream intervals;
      for (const auto &[start, end] : minIntervals) {
	//	intervals << start << "-" << end << ", ";
	minimisedArgIntervals.emplace_back(start + startChildOffset, end + startChildOffset);
      }
      // std::cout << intervals.str() << std::endl;
      return std::move(minimisedArgIntervals);
    }

    std::pair<std::vector<size_t>, std::vector<size_t>> getArgOffsetsRanged(size_t startChildOffset) {
      if (!isSet) {
	throw std::runtime_error("Attempt to call getArgOffsetsRanged on Unset IndicesManager. Operation is not possible.");
      }
      if (!isRanged) {
	throw std::runtime_error("Attempt to call getArgOffsetsRanged for non-ranged indices. Operation is not possible. Try getArgOffets or getMinArgIntervals.");
      }
      std::vector<size_t> startOffsets;
      std::vector<size_t> sizes;
      startOffsets.reserve(indices.size() / 2);
      sizes.reserve(indices.size() / 2);
      for (auto i = 0; i < indices.size(); i += 2) {
	auto &start = indices[i];
	auto &size = indices[i + 1];
	startOffsets.emplace_back(start + startChildOffset);
	sizes.emplace_back(size);
      }
      auto res = std::make_pair(startOffsets, sizes);
      return std::move(res);
    }

    std::vector<size_t> getStringOffsets(LazilyDeserializedExpression &lazyExpr, size_t numChildren, bool isSpan) {
      if (!isSet) {
	throw std::runtime_error("Attempt to call getStringOffsets on Unset IndicesManager. Operation is not possible.");
      }
      std::vector<size_t> resOffsets;
      resOffsets.reserve(numIndices);
      if (isRanged) {
	for (auto i = 0; i < indices.size(); i += 2) {
	  auto &start = indices[i];
	  auto &size = indices[i + 1];
	  for (auto j = 0; j < size; j++) {
	    auto lazyChildExpr = lazyExpr[start + j];
	    resOffsets.emplace_back(lazyChildExpr.getCurrentExpressionAsString(isSpan));
	  }
	}
      } else {
	std::transform(indices.begin(), indices.end(), std::back_inserter(resOffsets),
		       [&lazyExpr, &numChildren, &isSpan](const auto &index) {
			 auto lazyChildExpr = (index < 0 || index > numChildren) ?
			   lazyExpr[0] :
			   lazyExpr[index];
			 return lazyChildExpr.getCurrentExpressionAsString(isSpan);
		       });
      }
      return std::move(resOffsets);
    }

    IndicesManager(IndicesManager const&) = delete;
    IndicesManager &operator=(IndicesManager const&) = delete;
    
    IndicesManager(IndicesManager &&other) noexcept
      : isRanged(other.isRanged), numIndices(other.numIndices)
    {
      indices = std::move(other.indices);
      minimisedIntervalsMap = std::move(other.minimisedIntervalsMap);
    };
    IndicesManager &operator=(IndicesManager &&other) noexcept {
      if (this != &other) {
        isRanged = other.isRanged;
        numIndices = other.numIndices;
	indices = std::move(other.indices);
	minimisedIntervalsMap = std::move(other.minimisedIntervalsMap);
      }
      return *this;
    };
    IndicesManager() = default;
    ~IndicesManager() = default;
  };
  
  ArgumentType getTypeOfColumn(LazilyDeserializedExpression &lazyExpr, uint64_t startChildOffset, TableManager &tableMan, const std::string &url, EvalFunction &loader);
  
  boss::expressions::ExpressionSpanArgument
  getSpanFromIndices(LazilyDeserializedExpression &lazyExpr,
		     TableManager &tableMan,
                     IndicesManager &indices,
                     const std::string &url, EvalFunction &loader, bool isSpan);

  boss::expressions::ExpressionSpanArgument getSpanFromIndexRanges(
      LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
      IndicesManager &indexRanges,
      const std::string &url, EvalFunction &loader, bool isSpan);

  boss::Expression
  lazyGather(LazilyDeserializedExpression &lazyExpr, TableManager &tableMan,
             IndicesManager &indices,
             const std::vector<boss::Symbol> &columns, const std::string &url,
             EvalFunction &loader);
  
  template <typename T>
  static boss::expressions::ExpressionSpanArgument
  getSpan(ArgumentType argType, LazilyDeserializedExpression &lazyExpr,
          IndicesManager &indicesMan,
          size_t numChildren) {
    auto &indices = indicesMan.indices;
    auto &numIndices = indicesMan.numIndices;
    std::vector<T> matches;
    matches.reserve(numIndices);
    for (auto &index : indices) {
      auto lazyChildExpr =
	(index < 0 || index > numChildren ? lazyExpr[0]
	 : lazyExpr[index]);

      T val = get<T>(lazyChildExpr.getCurrentExpressionAs(argType));
      // std::cout << index << ", " << val << std::endl;
      matches.emplace_back(std::move(val));
    }
    return boss::Span<T>(std::move(matches));
  }

  template <typename T>
  static boss::expressions::ExpressionSpanArgument
  getSpanFromRange(ArgumentType argType, LazilyDeserializedExpression &lazyExpr,
                   IndicesManager &indicesMan) {
    auto numIndices = indicesMan.numIndices;
    auto &indices = indicesMan.indices;
    std::vector<T> matches;
    matches.reserve(numIndices);

    for (size_t i = 0; i < indices.size(); i += 2) {
      auto &start = indices[i];
      auto &size = indices[i + 1];
      for (auto j = 0; j < size; j++) {
	auto lazyChildExpr = lazyExpr[start + j];
	matches.emplace_back(get<T>(lazyChildExpr.getCurrentExpressionAs(argType)));
      }
    }
    return boss::Span<T>(std::move(matches));
  }

  int64_t maxRanges;

  std::unordered_map<
      ArgumentType,
      std::function<boss::expressions::ExpressionSpanArgument(
	  ArgumentType argType, LazilyDeserializedExpression &lazyExpr,
          IndicesManager &indices, size_t numChildren)>>
      spanFunctors = {
          {ArgumentType::ARGUMENT_TYPE_BOOL, getSpan<bool>},
          {ArgumentType::ARGUMENT_TYPE_CHAR, getSpan<int8_t>},
          {ArgumentType::ARGUMENT_TYPE_INT, getSpan<int32_t>},
          {ArgumentType::ARGUMENT_TYPE_LONG, getSpan<int64_t>},
          {ArgumentType::ARGUMENT_TYPE_FLOAT, getSpan<float_t>},
          {ArgumentType::ARGUMENT_TYPE_DOUBLE, getSpan<double_t>},
          {ArgumentType::ARGUMENT_TYPE_STRING, getSpan<std::string>}};

  std::unordered_map<
      ArgumentType,
      std::function<boss::expressions::ExpressionSpanArgument(
							      ArgumentType argType,
							      LazilyDeserializedExpression &lazyExpr,
							      IndicesManager &indices)>>
      spanFromRangeFunctors = {
          {ArgumentType::ARGUMENT_TYPE_BOOL, getSpanFromRange<bool>},
          {ArgumentType::ARGUMENT_TYPE_CHAR, getSpanFromRange<int8_t>},
          {ArgumentType::ARGUMENT_TYPE_INT, getSpanFromRange<int32_t>},
          {ArgumentType::ARGUMENT_TYPE_LONG, getSpanFromRange<int64_t>},
          {ArgumentType::ARGUMENT_TYPE_FLOAT, getSpanFromRange<float_t>},
          {ArgumentType::ARGUMENT_TYPE_DOUBLE, getSpanFromRange<double_t>},
          {ArgumentType::ARGUMENT_TYPE_STRING, getSpanFromRange<std::string>}};

  std::unordered_map<std::string, TableManager> tableMap;

};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
} // namespace boss::engines::WisentDeserialiser

#endif
