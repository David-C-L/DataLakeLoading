#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <Serialization.hpp>
#include <filesystem>
#ifndef _WIN32
#include <dlfcn.h>
#else
#include <filesystem>
#include <iostream>
#define NOMINMAX // max macro in minwindef.h interfering with std::max...
#include <windows.h>
constexpr static int RTLD_NOW = 0;
constexpr static int RTLD_NODELETE = 0;
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

using boss::Expression;
using boss::serialization::Argument;
using boss::serialization::ArgumentType;
using boss::serialization::RootExpression;
using boss::serialization::SerializedExpression;
using LazilyDeserializedExpression = boss::serialization::SerializedExpression<
    nullptr, nullptr, nullptr>::LazilyDeserializedExpression;
using SerializationExpression = boss::serialization::Expression;

namespace boss::engines::WisentDeserialiser {

using EvalFunction = BOSSExpression *(*)(BOSSExpression *);

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
  void resetRoot(RootExpression *&root, boss::Span<int8_t> &byteSpan,
                 const std::string &url, std::vector<int64_t> &bounds,
                 EvalFunction &loader);

  boss::Span<int8_t> loadBoundsIntoSpan(const std::string &url,
                                        std::vector<int64_t> &bounds,
                                        EvalFunction &loader);

  boss::Expression createFetchExpression(const std::string &url,
                                         std::vector<int64_t> &bounds);

  boss::expressions::ExpressionSpanArgument
  getSpanFromIndices(LazilyDeserializedExpression &lazyExpr,
                     RootExpression *&root, boss::Span<int8_t> &byteSpan,
                     const boss::expressions::ExpressionSpanArguments &indices,
                     const std::string &url, EvalFunction &loader, bool isSpan);

  boss::expressions::ExpressionSpanArgument getSpanFromIndexRanges(
      LazilyDeserializedExpression &lazyExpr, RootExpression *&root,
      boss::Span<int8_t> &byteSpan,
      const boss::expressions::ExpressionSpanArguments &indexRanges,
      const std::string &url, EvalFunction &loader, bool isSpan);

  boss::Expression
  lazyGather(LazilyDeserializedExpression &lazyExpr, RootExpression *&root,
             boss::Span<int8_t> &byteSpan,
             const boss::expressions::ExpressionSpanArguments &indices,
             const std::vector<boss::Symbol> &columns, const std::string &url,
             EvalFunction &loader, bool rangedIndices);

  template <typename T>
  static boss::expressions::ExpressionSpanArgument
  getSpan(ArgumentType argType, LazilyDeserializedExpression &lazyExpr,
          const boss::expressions::ExpressionSpanArguments &indices,
          size_t &numIndices, size_t numChildren) {
    std::vector<T> matches;
    matches.reserve(numIndices);
    std::for_each(
        indices.begin(), indices.end(),
        [&matches, &argType, &lazyExpr, &numChildren](const auto &span) {
          if (std::holds_alternative<boss::Span<int64_t const>>(span)) {
            for (auto indice : get<boss::Span<int64_t const>>(span)) {
              auto lazyChildExpr =
                  (indice < 0 || indice > numChildren ? lazyExpr[0]
                                                      : lazyExpr[indice]);

	      auto currArgType = lazyChildExpr.getCurrentExpressionType();
#ifdef DEBUG
	      if (argType != currArgType) {
		std::cout << "TYPE WANTED: " << argType << " TYPE GOT: " << currArgType << " AT: " << indice << std::endl;
	      }
#endif
	      auto argExpr = lazyChildExpr.getCurrentExpressionAs(argType);
	      matches.emplace_back(std::move(get<T>(argExpr)));
            }
          } else if (std::holds_alternative<boss::Span<int64_t>>(span)) {
            for (auto indice : get<boss::Span<int64_t>>(span)) {
              auto lazyChildExpr =
                  (indice < 0 || indice > numChildren ? lazyExpr[0]
                                                      : lazyExpr[indice]);
	      auto argExpr = lazyChildExpr.getCurrentExpressionAs(argType);
	      matches.emplace_back(std::move(get<T>(argExpr)));
            }
          }
        });
    
    return boss::Span<T>(std::move(matches));
  }

  template <typename T>
  static boss::expressions::ExpressionSpanArgument
  getSpanFromRange(LazilyDeserializedExpression &lazyExpr,
                   const boss::expressions::ExpressionSpanArguments &indices,
                   size_t &numIndices) {
    std::vector<T> matches;
    matches.reserve(numIndices);

    for (size_t i = 0; i < indices.size(); i += 2) {
      if (std::holds_alternative<boss::Span<int64_t const>>(indices[i])) {
        auto &starts = get<boss::Span<int64_t const>>(indices[i]);
        auto &ends = get<boss::Span<int64_t const>>(indices[i + 1]);
        if (starts.size() != ends.size()) {
          std::cout << "INCORRECT SIZING IN SPANS" << std::endl;
        }
        for (size_t j = 0; j < starts.size(); j++) {
          auto &start = starts[j];
          auto &end = ends[j];
          for (size_t k = start; k != end; k++) {
            auto lazyChildExpr = lazyExpr[k];
            matches.emplace_back(get<T>(lazyChildExpr.getCurrentExpression()));
          }
        }
      } else if (std::holds_alternative<boss::Span<int64_t>>(indices[i])) {
        auto &starts = get<boss::Span<int64_t>>(indices[i]);
        auto &ends = get<boss::Span<int64_t>>(indices[i + 1]);
        if (starts.size() != ends.size()) {
          std::cout << "INCORRECT SIZING IN SPANS" << std::endl;
        }
        for (size_t j = 0; j < starts.size(); j++) {
          auto &start = starts[j];
          auto &end = ends[j];
          for (size_t k = start; k != end; k++) {
            auto lazyChildExpr = lazyExpr[k];
            matches.emplace_back(get<T>(lazyChildExpr.getCurrentExpression()));
          }
        }
      }
    }
    return boss::Span<T>(std::move(matches));
  }

  int64_t maxRanges;

  std::unordered_map<
      ArgumentType,
      std::function<boss::expressions::ExpressionSpanArgument(
	  ArgumentType argType, LazilyDeserializedExpression &lazyExpr,
          const boss::expressions::ExpressionSpanArguments &indices,
          size_t &numIndices, size_t numChildren)>>
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
          LazilyDeserializedExpression &lazyExpr,
          const boss::expressions::ExpressionSpanArguments &indices,
          size_t &numIndices)>>
      spanFromRangeFunctors = {
          {ArgumentType::ARGUMENT_TYPE_BOOL, getSpanFromRange<bool>},
          {ArgumentType::ARGUMENT_TYPE_CHAR, getSpanFromRange<int8_t>},
          {ArgumentType::ARGUMENT_TYPE_INT, getSpanFromRange<int32_t>},
          {ArgumentType::ARGUMENT_TYPE_LONG, getSpanFromRange<int64_t>},
          {ArgumentType::ARGUMENT_TYPE_FLOAT, getSpanFromRange<float_t>},
          {ArgumentType::ARGUMENT_TYPE_DOUBLE, getSpanFromRange<double_t>},
          {ArgumentType::ARGUMENT_TYPE_STRING, getSpanFromRange<std::string>}};
};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
} // namespace boss::engines::WisentDeserialiser
