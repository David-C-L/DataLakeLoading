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

constexpr static int64_t FETCH_PADDING = 0;
constexpr static int64_t FETCH_ALIGNMENT = 8192;
constexpr static int64_t DEFAULT_MAX_RANGES = 512;

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
      if (std::get<boss::ComplexExpression>(dynamics[idx]).getHead() ==
          colHead) {
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

std::vector<int64_t> getArgAndTypeBounds(std::vector<size_t> const &offsets,
                                         RootExpression *root) {
  std::vector<int64_t> bounds;
  bounds.reserve(offsets.size() * 4);
  size_t currByteOffset = sizeof(RootExpression);
  for (auto offset : offsets) {
    // Argument Array
    bounds.emplace_back(currByteOffset + (offset * sizeof(Argument)));
    bounds.emplace_back(currByteOffset + ((offset + 1) * sizeof(Argument)));
  }
  currByteOffset += root->argumentCount * sizeof(Argument);
  for (auto offset : offsets) {
    // ArgumentType Array
    bounds.emplace_back(currByteOffset + (offset * sizeof(ArgumentType)));
    bounds.emplace_back(currByteOffset + ((offset + 1) * sizeof(ArgumentType)));
  }

  return bounds;
}

std::vector<int64_t> getArgAndTypeBoundsContiguous(size_t const &startOffset,
                                                   size_t const &size,
                                                   RootExpression *root) {
  std::vector<int64_t> bounds;
  auto endOffset = startOffset + size - 1;

  size_t currByteOffset = sizeof(RootExpression);
  // Argument Array
  bounds.emplace_back(currByteOffset + (startOffset * sizeof(Argument)));
  bounds.emplace_back(currByteOffset + ((endOffset + 1) * sizeof(Argument)));
  // ArgumentType Array
  currByteOffset += root->argumentCount * sizeof(Argument);
  bounds.emplace_back(currByteOffset + (startOffset * sizeof(ArgumentType)));
  bounds.emplace_back(currByteOffset +
                      ((endOffset + 1) * sizeof(ArgumentType)));

  return bounds;
}

std::vector<int64_t> getExprBounds(std::vector<size_t> const &offsets,
                                   RootExpression *root) {
  std::vector<int64_t> bounds;
  bounds.reserve(offsets.size() * 2);
  for (auto offset : offsets) {
    size_t currByteOffset = sizeof(RootExpression) +
                            root->argumentCount * sizeof(Argument) +
                            root->argumentCount * sizeof(ArgumentType);
    // Expression Array
    bounds.emplace_back(currByteOffset +
                        (offset * sizeof(SerializationExpression)));
    bounds.emplace_back(currByteOffset +
                        ((offset + 1) * sizeof(SerializationExpression)));
  }
  return bounds;
}

std::vector<int64_t> getStringBounds(std::vector<size_t> const &offsets,
                                     RootExpression *root,
                                     size_t approxLength) {
  std::vector<int64_t> bounds;
  bounds.reserve(offsets.size() * 2);
  for (auto offset : offsets) {
    size_t currByteOffset =
        sizeof(RootExpression) + root->argumentCount * sizeof(Argument) +
        root->argumentCount * sizeof(ArgumentType) +
        root->expressionCount * sizeof(SerializationExpression);
    // String Array
    bounds.emplace_back(currByteOffset + offset);
    bounds.emplace_back(currByteOffset + offset + approxLength);
  }

  return bounds;
}

boss::Expression
Engine::createFetchExpression(const std::string &url,
                              std::vector<int64_t> &bounds) {
  ExpressionArguments args;
  args.push_back(std::move("List"_(boss::Span<int64_t>(bounds))));
  args.push_back(url);
  args.push_back((int64_t)FETCH_PADDING);
  args.push_back((int64_t)FETCH_ALIGNMENT);
  args.push_back((int64_t)maxRanges);

  return boss::ComplexExpression("Fetch"_, {}, std::move(args), {});
}

boss::Span<int8_t>
Engine::loadBoundsIntoSpan(const std::string &url,
                           std::vector<int64_t> &bounds,
                           EvalFunction &loader) {
  auto fetchExpr = createFetchExpression(url, bounds);
  auto byteSeqExpr = std::get<boss::ComplexExpression>(
      applyEngine(std::move(fetchExpr), loader));
  return std::move(getByteSequence(std::move(byteSeqExpr)));
}

void Engine::resetRoot(RootExpression *&root, boss::Span<int8_t> &byteSpan,
                       const std::string &url,
                       std::vector<int64_t> &bounds,
                       EvalFunction &loader) {
  byteSpan = loadBoundsIntoSpan(url, bounds, loader);
  root = (RootExpression *)byteSpan.begin();
  *((void **)&root->originalAddress) = root;
}

bool isValidIndexType(
    const boss::expressions::ExpressionSpanArguments &indices) {
  bool isConstInt64 =
      std::holds_alternative<boss::Span<int64_t const>>(indices[0]) &&
      get<boss::Span<int64_t const>>(indices[0]).size() > 0;
  bool isInt64 = std::holds_alternative<boss::Span<int64_t>>(indices[0]) &&
                 get<boss::Span<int64_t>>(indices[0]).size() > 0;
  return isConstInt64 || isInt64;
}

size_t countIndices(const boss::expressions::ExpressionSpanArguments &indices) {
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

size_t
countRangedIndices(const boss::expressions::ExpressionSpanArguments &indices) {
  size_t numIndices = 0;
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
        numIndices += size;
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
        numIndices += size;
      }
    }
  }
  return numIndices;
}

boss::expressions::ExpressionSpanArgument Engine::getSpanFromIndexRanges(
    LazilyDeserializedExpression &lazyExpr, RootExpression *&root,
    boss::Span<int8_t> &byteSpan,
    const boss::expressions::ExpressionSpanArguments &indexRanges,
    const std::string &url, EvalFunction &loader, bool isSpan) {

  auto exprOffsets = lazyExpr.expression();

  auto const &startChildOffset = exprOffsets.startChildOffset;
  auto const &endChildOffset = exprOffsets.endChildOffset;
  auto const numChildren = endChildOffset - startChildOffset;

  size_t numRanges = countIndices(indexRanges) / 2;
  size_t numIndices = countRangedIndices(indexRanges);

#ifdef DEBUG
  std::cout << "NUM RANGES: " << numRanges << std::endl;
  std::cout << "NUM INDICES: " << numIndices << std::endl;
#endif
  std::vector<int64_t> argBounds;
  for (size_t i = 0; i < indexRanges.size(); i += 2) {
    if (std::holds_alternative<boss::Span<int64_t const>>(indexRanges[i])) {
      auto &starts = get<boss::Span<int64_t const>>(indexRanges[i]);
      auto &ends = get<boss::Span<int64_t const>>(indexRanges[i + 1]);
      if (starts.size() != ends.size()) {
        std::cout << "INCORRECT SIZING IN SPANS" << std::endl;
      }
      for (size_t j = 0; j < starts.size(); j++) {
        auto &start = starts[j];
        auto &end = ends[j];
        auto startOffset = startChildOffset + start;
        size_t size = end - start;
        auto tempBounds =
            getArgAndTypeBoundsContiguous(startOffset, size, root);
        argBounds.reserve(argBounds.size() + tempBounds.size());
        argBounds.insert(argBounds.end(),
                         std::make_move_iterator(tempBounds.begin()),
                         std::make_move_iterator(tempBounds.end()));
      }
    } else if (std::holds_alternative<boss::Span<int64_t>>(indexRanges[i])) {
      auto &starts = get<boss::Span<int64_t>>(indexRanges[i]);
      auto &ends = get<boss::Span<int64_t>>(indexRanges[i + 1]);
      if (starts.size() != ends.size()) {
        std::cout << "INCORRECT SIZING IN SPANS" << std::endl;
      }
      for (size_t j = 0; j < starts.size(); j++) {
        auto &start = starts[j];
        auto &end = ends[j];
        auto startOffset = startChildOffset + start;
        size_t size = end - start;
        auto tempBounds =
            getArgAndTypeBoundsContiguous(startOffset, size, root);
        argBounds.reserve(argBounds.size() + tempBounds.size());
        argBounds.insert(argBounds.end(),
                         std::make_move_iterator(tempBounds.begin()),
                         std::make_move_iterator(tempBounds.end()));
      }
    }
  }
  resetRoot(root, byteSpan, url, argBounds, loader);

  auto firstIdx = 0;
  if (std::holds_alternative<boss::Span<int64_t const>>(indexRanges[0])) {
    firstIdx = get<boss::Span<int64_t const>>(indexRanges[0])[0];
  } else if (std::holds_alternative<boss::Span<int64_t>>(indexRanges[0])) {
    firstIdx = get<boss::Span<int64_t>>(indexRanges[0])[0];
  }
#ifdef DEBUG
  std::cout << "GOT FIRST ARG IDX: " << firstIdx << std::endl;
#endif
  auto tempLazyChildExpr = lazyExpr[firstIdx];
  auto const &type = tempLazyChildExpr.getCurrentExpressionType();

#ifdef DEBUG
  std::cout << "TYPE: " << type << std::endl;
#endif
  if (type == ArgumentType::ARGUMENT_TYPE_STRING ||
      type == ArgumentType::ARGUMENT_TYPE_SYMBOL) {
    std::vector<size_t> childStrOffsets;
    childStrOffsets.reserve(numIndices);

    for (size_t i = 0; i < indexRanges.size(); i += 2) {
      if (std::holds_alternative<boss::Span<int64_t const>>(indexRanges[i])) {
        auto &starts = get<boss::Span<int64_t const>>(indexRanges[i]);
        auto &ends = get<boss::Span<int64_t const>>(indexRanges[i + 1]);
        if (starts.size() != ends.size()) {
          std::cout << "INCORRECT SIZING IN SPANS" << std::endl;
        }
        for (size_t j = 0; j < starts.size(); j++) {
          auto &start = starts[j];
          auto &end = ends[j];
          for (size_t k = start; k != end; k++) {
            auto lazyChildExpr = lazyExpr[k];
            childStrOffsets.emplace_back(
                lazyChildExpr.getCurrentExpressionAsString(isSpan));
          }
        }
      } else if (std::holds_alternative<boss::Span<int64_t>>(indexRanges[i])) {
        auto &starts = get<boss::Span<int64_t>>(indexRanges[i]);
        auto &ends = get<boss::Span<int64_t>>(indexRanges[i + 1]);
        if (starts.size() != ends.size()) {
          std::cout << "INCORRECT SIZING IN SPANS" << std::endl;
        }
        for (size_t j = 0; j < starts.size(); j++) {
          auto &start = starts[j];
          auto &end = ends[j];
          for (size_t k = start; k != end; k++) {
            auto lazyChildExpr = lazyExpr[k];
            childStrOffsets.emplace_back(
                lazyChildExpr.getCurrentExpressionAsString(isSpan));
          }
        }
      }
    }
    std::vector<int64_t> childStrBounds =
        getStringBounds(childStrOffsets, root, 64);
    resetRoot(root, byteSpan, url, childStrBounds, loader);
  }
  return std::move(
      spanFromRangeFunctors.at(type)(lazyExpr, indexRanges, numIndices));
}

boss::expressions::ExpressionSpanArgument Engine::getSpanFromIndices(
    LazilyDeserializedExpression &lazyExpr, RootExpression *&root,
    boss::Span<int8_t> &byteSpan,
    const boss::expressions::ExpressionSpanArguments &indices,
    const std::string &url, EvalFunction &loader, bool isSpan) {

  auto exprOffsets = lazyExpr.expression();

  auto const &startChildOffset = exprOffsets.startChildOffset;
  auto const &endChildOffset = exprOffsets.endChildOffset;
  auto const numChildren = endChildOffset - startChildOffset;
  size_t numIndices = countIndices(indices);
#ifdef DEBUG
  std::cout << "NUM INDICES: " << numIndices << std::endl;
#endif
  std::vector<size_t> indicesOffsets;
  indicesOffsets.reserve(numIndices);
  std::for_each(
      indices.begin(), indices.end(),
      [&startChildOffset, &indicesOffsets, &numChildren](const auto &span) {
        if (std::holds_alternative<boss::Span<int64_t const>>(span)) {
          std::transform(get<boss::Span<int64_t const>>(span).begin(),
                         get<boss::Span<int64_t const>>(span).end(),
                         std::back_inserter(indicesOffsets),
                         [&startChildOffset, &numChildren](auto indice) {
                           return (indice < 0 || indice > numChildren
                                       ? startChildOffset
                                       : startChildOffset + indice);
                         });
        } else if (std::holds_alternative<boss::Span<int64_t>>(span)) {
          std::transform(get<boss::Span<int64_t>>(span).begin(),
                         get<boss::Span<int64_t>>(span).end(),
                         std::back_inserter(indicesOffsets),
                         [&startChildOffset, &numChildren](auto indice) {
                           return (indice < 0 || indice > numChildren
                                       ? startChildOffset
                                       : startChildOffset + indice);
                         });
        }
      });
  std::vector<int64_t> argBounds = getArgAndTypeBounds(indicesOffsets, root);
  resetRoot(root, byteSpan, url, argBounds, loader);
  
  auto firstIdx = 0;
  if (std::holds_alternative<boss::Span<int64_t const>>(indices[0])) {
    firstIdx = get<boss::Span<int64_t const>>(indices[0])[0];
  } else if (std::holds_alternative<boss::Span<int64_t>>(indices[0])) {
    firstIdx = get<boss::Span<int64_t>>(indices[0])[0];
  }
#ifdef DEBUG
  std::cout << "GOT FIRST ARG IDX: " << firstIdx << std::endl;
#endif
  auto tempLazyChildExpr = lazyExpr[firstIdx];
  auto const &type = tempLazyChildExpr.getCurrentExpressionType();

#ifdef DEBUG
  std::cout << "TYPE: " << type << std::endl;
#endif
  if (type == ArgumentType::ARGUMENT_TYPE_STRING ||
      type == ArgumentType::ARGUMENT_TYPE_SYMBOL) {
    std::vector<size_t> childStrOffsets;
    childStrOffsets.reserve(numIndices);

    std::for_each(
        indices.begin(), indices.end(),
        [&childStrOffsets, &lazyExpr, &isSpan, &numChildren](const auto &span) {
          if (std::holds_alternative<boss::Span<int64_t const>>(span)) {
            for (auto const &indice : get<boss::Span<int64_t const>>(span)) {
              auto lazyChildExpr =
                  (indice < 0 || indice > numChildren ? lazyExpr[0]
                                                      : lazyExpr[indice]);
              childStrOffsets.emplace_back(
                  lazyChildExpr.getCurrentExpressionAsString(isSpan));
            }
          } else if (std::holds_alternative<boss::Span<int64_t>>(span)) {
            for (auto const &indice : get<boss::Span<int64_t>>(span)) {
              auto lazyChildExpr =
                  (indice < 0 || indice > numChildren ? lazyExpr[0]
                                                      : lazyExpr[indice]);
              childStrOffsets.emplace_back(
                  lazyChildExpr.getCurrentExpressionAsString(isSpan));
            }
          }
        });

    std::vector<int64_t> childStrBounds =
        getStringBounds(childStrOffsets, root, 128);
    resetRoot(root, byteSpan, url, childStrBounds, loader);
  }
  return std::move(
   spanFunctors.at(type)(type, lazyExpr, indices, numIndices, numChildren));
}

boss::Expression Engine::lazyGather(LazilyDeserializedExpression &lazyExpr,
                                    RootExpression *&root,
                                    boss::Span<int8_t> &byteSpan,
                                    const ExpressionSpanArguments &indices,
                                    const std::vector<boss::Symbol> &columns,
                                    const std::string &url,
                                    EvalFunction &loader, bool rangedIndices) {
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
    if (indices.size() > 0 && isValidIndexType(indices)) {
      if (!rangedIndices) {
        spanArgs.push_back(std::move(getSpanFromIndices(
            lazyExpr, root, byteSpan, indices, url, loader, isSpan)));
      } else {
        spanArgs.push_back(std::move(getSpanFromIndexRanges(
            lazyExpr, root, byteSpan, indices, url, loader, isSpan)));
      }
    } else {
      std::vector<int64_t> argBounds =
          getArgAndTypeBoundsContiguous(startChildOffset, numChildren, root);
      resetRoot(root, byteSpan, url, argBounds, loader);
      int64_t totalSize = 0;

      for (auto offset = 0; offset < numChildren; offset++) {
        auto lazyChildExpr = lazyExpr[offset];
        auto const &type = lazyChildExpr.getCurrentExpressionType();
        auto const &size = lazyChildExpr.currentIsRLE();
        totalSize += size;
        if (size != 0) {
          if (type == ArgumentType::ARGUMENT_TYPE_STRING ||
              type == ArgumentType::ARGUMENT_TYPE_SYMBOL) {
            std::vector<size_t> childStrOffsets;
            childStrOffsets.reserve(size);
            for (auto i = 0; i < size; i++) {
              auto lazyStrChildExpr = lazyExpr[offset + i];
              childStrOffsets.emplace_back(
                  lazyStrChildExpr.getCurrentExpressionAsString(size != 0));
            }
            std::vector<int64_t> childStrBounds =
                getStringBounds(childStrOffsets, root, 64);
            resetRoot(root, byteSpan, url, childStrBounds, loader);
          }
	  
          spanArgs.push_back(
              std::move(lazyChildExpr.getCurrentExpressionAsSpan()));
          offset += size - 1;
        }
      }
#ifdef DEBUG
      std::cout << "EXPECTED: " << numChildren << std::endl;
      std::cout << "GOT: " << totalSize << std::endl;
#endif
    }
  } else {

    std::vector<int64_t> argBounds =
        getArgAndTypeBoundsContiguous(startChildOffset, numChildren, root);
    resetRoot(root, byteSpan, url, argBounds, loader);

    for (auto offset = 0; offset < numChildren; offset++) {

      auto lazyChildExpr = lazyExpr[offset];
#ifdef DEBUG
      std::cout << "CHILD: " << offset
                << " IS EXPR: " << lazyChildExpr.currentIsExpression()
                << std::endl;
#endif
      if (lazyChildExpr.currentIsExpression()) {

        std::vector<int64_t> exprBounds =
            getExprBounds({startChildOffset + offset}, root);
        resetRoot(root, byteSpan, url, exprBounds, loader);

        auto childExprOffsets = lazyChildExpr.expression();
        std::vector<int64_t> childStrBounds =
            getStringBounds({childExprOffsets.symbolNameOffset}, root, 16);
        resetRoot(root, byteSpan, url, childStrBounds, loader);

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
            auto childArg = lazyGather(lazyChildExpr, root, byteSpan, indices,
                                       columns, url, loader, rangedIndices);
            args.emplace_back(std::move(childArg));
          }
        } else {

          auto childArg = lazyGather(lazyChildExpr, root, byteSpan, indices,
                                     columns, url, loader, rangedIndices);
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
                                       "GatherRanges"_));
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
              if (dynamics.size() < 4 || dynamics.size() > 5) {
                throw std::runtime_error(
                    "Wisent Deserialiser: Gather_ expression must be of form "
                    "Gather_[url, loaderPath, List_[List_[spanIndices...]]], "
                    "List_[column...], maxRanges?]");
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
                maxRanges = get<int64_t>(dynamics[4]);
                maxRanges = maxRanges < 1 ? DEFAULT_MAX_RANGES : maxRanges;
              }

              RootExpression *root;
              boss::Span<int8_t> byteSpan;
              std::vector<int64_t> metadataBounds = {0, 32};
              resetRoot(root, byteSpan, url, metadataBounds, loader);

              auto argBounds = getArgAndTypeBounds({0}, root);
              auto exprBounds = getExprBounds({0}, root);
              argBounds.insert(argBounds.end(), exprBounds.begin(),
                               exprBounds.end());
              resetRoot(root, byteSpan, url, argBounds, loader);

              auto expr = SerializedExpression<nullptr, nullptr, nullptr>(root);
              auto lazyExpr = expr.lazilyDeserialize();

              auto exprOffsets = lazyExpr.expression();
              std::vector<int64_t> strBounds =
                  getStringBounds({exprOffsets.symbolNameOffset}, root, 16);
              resetRoot(root, byteSpan, url, strBounds, loader);

              auto res = lazyGather(lazyExpr, root, byteSpan, indices, columns,
                                    url, loader, false);
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

              RootExpression *root;
              boss::Span<int8_t> byteSpan;
              std::vector<int64_t> metadataBounds = {0, 32};
              resetRoot(root, byteSpan, url, metadataBounds, loader);

              auto argBounds = getArgAndTypeBounds({0}, root);
              auto exprBounds = getExprBounds({0}, root);
              argBounds.insert(argBounds.end(), exprBounds.begin(),
                               exprBounds.end());
              resetRoot(root, byteSpan, url, argBounds, loader);

              auto expr = SerializedExpression<nullptr, nullptr, nullptr>(root);
              auto lazyExpr = expr.lazilyDeserialize();

              auto exprOffsets = lazyExpr.expression();
              std::vector<int64_t> strBounds =
                  getStringBounds({exprOffsets.symbolNameOffset}, root, 16);
              resetRoot(root, byteSpan, url, strBounds, loader);

              auto res = lazyGather(lazyExpr, root, byteSpan, indices, columns,
                                    url, loader, true);
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
