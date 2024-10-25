#include "DADSDictionaryEncoderEngine.hpp"
#include <DADS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <iostream>
#include <mutex>

// #define DEBUG

using std::string_literals::operator""s;
using dads::utilities::operator""_;
using dads::ComplexExpression;
using dads::Span;
using dads::Symbol;

using dads::Expression;

static dads::ComplexExpression shallowCopy(dads::ComplexExpression const& e) {
  auto const& head = e.getHead();
  auto const& dynamics = e.getDynamicArguments();
  auto const& spans = e.getSpanArguments();
  dads::ExpressionArguments dynamicsCopy;
  std::transform(dynamics.begin(), dynamics.end(), std::back_inserter(dynamicsCopy),
                 [](auto const& arg) {
                   return std::visit(
                       dads::utilities::overload(
                           [&](dads::ComplexExpression const& expr) -> dads::Expression {
                             return shallowCopy(expr);
                           },
                           [](auto const& otherTypes) -> dads::Expression { return otherTypes; }),
                       arg);
                 });
  dads::expressions::ExpressionSpanArguments spansCopy;
  std::transform(spans.begin(), spans.end(), std::back_inserter(spansCopy), [](auto const& span) {
    return std::visit(
        [](auto const& typedSpan) -> dads::expressions::ExpressionSpanArgument {
          // just do a shallow copy of the span
          // the storage's span keeps the ownership
          // (since the storage will be alive until the query finishes)
          using SpanType = std::decay_t<decltype(typedSpan)>;
          using T = std::remove_const_t<typename SpanType::element_type>;
          if constexpr(std::is_same_v<T, bool>) {
            return SpanType(typedSpan.begin(), typedSpan.size(), []() {});
          } else {
            // force non-const value for now (otherwise expressions cannot be moved)
            auto* ptr = const_cast<T*>(typedSpan.begin()); // NOLINT
            return dads::Span<T>(ptr, typedSpan.size(), []() {});
          }
        },
        span);
  });
  return dads::ComplexExpression(head, {}, std::move(dynamicsCopy), std::move(spansCopy));
}

namespace dads::engines::DictionaryEncoder {
namespace utilities {} // namespace utilities

dads::Expression Engine::encodeColumn(Expression &&e) {
  return std::visit(
      dads::utilities::overload(
          [this](ComplexExpression &&expression) -> dads::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

            auto tempHead = std::move(head);
	    auto saveHead = tempHead;

            std::transform(
                std::make_move_iterator(dynamics.begin()),
                std::make_move_iterator(dynamics.end()), dynamics.begin(),
                [&](auto &&arg) {
                  return std::visit(
                      dads::utilities::overload(
                          [&](ComplexExpression &&listExpression)
                              -> dads::Expression {
                            auto [listHead, listUnused_, listDynamics,
                                  listSpans] =
                                std::move(listExpression).decompose();

                            std::transform(
                                std::make_move_iterator(listSpans.begin()),
                                std::make_move_iterator(listSpans.end()),
                                listSpans.begin(),
                                [&](dads::expressions::ExpressionSpanArgument
                                        &&arg)
                                    -> dads::expressions::
                                        ExpressionSpanArgument {
                                          if (std::holds_alternative<
                                                  dads::Span<std::string>>(
                                                  arg)) {
                                            Dictionary &dict =
                                                dictionaries[tempHead];
                                            return std::move(dict.encode(
                                                std::move(std::get<dads::Span<
                                                              std::string>>(
                                                    std::move(arg)))));
                                          }
                                          return std::move(arg);
                                        });
                            return dads::ComplexExpression(
                                std::move(listHead), {},
                                std::move(listDynamics), std::move(listSpans));
                          },
                          [this](Symbol &&symbol) -> dads::Expression {
                            return std::move(symbol);
                          },
                          [](auto &&arg) -> dads::Expression {
                            return std::forward<decltype(arg)>(arg);
                          }),
                      std::move(arg));
                });
            return dads::ComplexExpression(
                std::move(saveHead), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> dads::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> dads::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

dads::Expression Engine::encodeTable(ComplexExpression &&e) {
  auto [head, unused_, dynamics, spans] = std::move(e).decompose();

  std::transform(std::make_move_iterator(dynamics.begin()),
                 std::make_move_iterator(dynamics.end()), dynamics.begin(),
                 [this](auto &&arg) {
                   return encodeColumn(std::forward<decltype(arg)>(arg));
                 });
  return dads::ComplexExpression(std::move(head), {}, std::move(dynamics),
                                 std::move(spans));
}

dads::Expression Engine::decodeColumn(Expression &&e) {
  return std::visit(
      dads::utilities::overload(
          [this](ComplexExpression &&expression) -> dads::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    auto tempHead = std::move(head);
	    auto saveHead = tempHead;

            if (dictionaries.find(tempHead) != dictionaries.end()) {
	      Dictionary &dict = dictionaries[tempHead];

              std::transform(
                  std::make_move_iterator(dynamics.begin()),
                  std::make_move_iterator(dynamics.end()), dynamics.begin(),
                  [&](auto &&arg) {
                    return std::visit(
                        dads::utilities::overload(
                            [&](ComplexExpression &&listExpression)
                                -> dads::Expression {
                              auto [listHead, listUnused_, listDynamics,
                                    listSpans] =
                                  std::move(listExpression).decompose();

                              std::transform(
                                  std::make_move_iterator(listSpans.begin()),
                                  std::make_move_iterator(listSpans.end()),
                                  listSpans.begin(),
                                  [&](dads::expressions::ExpressionSpanArgument
                                          &&arg)
                                      -> dads::expressions::
                                          ExpressionSpanArgument {
                                            if (std::holds_alternative<
                                                    dads::Span<int32_t const>>(
                                                    arg)) {
                                              return std::move(dict.decode(
                                                  std::move(std::get<dads::Span<
                                                                int32_t const>>(
                                                      std::move(arg)))));
                                            } else if (std::holds_alternative<
                                                    dads::Span<int32_t>>(
                                                    arg)) {
                                              return std::move(dict.decode(
                                                  std::move(std::get<dads::Span<
                                                                int32_t>>(
                                                      std::move(arg)))));
                                            }
                                            return std::move(arg);
                                          });
                              return dads::ComplexExpression(
                                  std::move(listHead), {},
                                  std::move(listDynamics),
                                  std::move(listSpans));
                            },
                            [this](Symbol &&symbol) -> dads::Expression {
                              return std::move(symbol);
                            },
                            [](auto &&arg) -> dads::Expression {
                              return std::forward<decltype(arg)>(arg);
                            }),
                        std::move(arg));
                  });
            }
            return dads::ComplexExpression(
                std::move(saveHead), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> dads::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> dads::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

dads::Expression Engine::decodeTable(ComplexExpression &&e) {
  auto [head, unused_, dynamics, spans] = std::move(e).decompose();

  std::transform(std::make_move_iterator(dynamics.begin()),
                 std::make_move_iterator(dynamics.end()), dynamics.begin(),
                 [this](auto &&arg) {
                   return decodeColumn(std::forward<decltype(arg)>(arg));
                 });
  return dads::ComplexExpression(std::move(head), {}, std::move(dynamics),
                                 std::move(spans));
}

dads::Expression Engine::evaluate(Expression &&e) {
  return std::visit(
      dads::utilities::overload(
          [this](ComplexExpression &&expression) -> dads::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

            if (head == "EncodeTable"_) {
              if (std::holds_alternative<dads::ComplexExpression>(
                      dynamics[0])) {
                return encodeTable(std::move(
                    std::get<dads::ComplexExpression>(std::move(dynamics[0]))));
              }
            } else if (head == "DecodeTable"_) {
              if (std::holds_alternative<dads::ComplexExpression>(
                      dynamics[0])) {
                return decodeTable(std::move(
                    std::get<dads::ComplexExpression>(std::move(dynamics[0]))));
              }
            } else if (head == "GetEncodingFor"_) {
              if (std::holds_alternative<std::string>(
                      dynamics[0]) &&
		  std::holds_alternative<dads::Symbol>(
                      dynamics[1])) {
		auto key = std::get<std::string>(std::move(dynamics[0]));
		auto colSymbol = std::get<dads::Symbol>(std::move(dynamics[1]));
		auto dictsIt = dictionaries.find(colSymbol);
		if (dictsIt == dictionaries.end()) {
		  return "InvalidKeyForColumn"_("Key"_(std::move(key)), "Column"_(std::move(colSymbol)));
		}
		auto colDict = dictionaries[colSymbol];
                return colDict.getEncoding(key);
              }
            } else if (head == "SaveTable"_) {
	      auto table = std::get<dads::ComplexExpression>(std::move(dynamics[0]));
	      auto tableSymbol = std::get<dads::Symbol>(std::move(dynamics[1]));
	      tables[tableSymbol] = std::move(table);
	      return shallowCopy(std::get<dads::ComplexExpression>(tables[tableSymbol]));
	    } else if (head == "GetTable"_) {
	      auto tableSymbol = std::get<dads::Symbol>(std::move(dynamics[0]));
	      if (tables.find(tableSymbol) == tables.end()) {
		throw std::runtime_error("No saved table called: " + tableSymbol.getName());
	      }
	      return shallowCopy(std::get<dads::ComplexExpression>(tables[tableSymbol]));
	    } else if (head == "ClearTables"_) {
	      tables.clear();
	      return "Success"_;
	    } else if (head == "GetEngineCapabilities"_) {
	      return "List"_("EncodeTable"_, "DecodeTable"_, "SaveTable"_, "GetTable"_, "ClearTables"_, "GetEncodingFor"_);
	    }
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return evaluate(std::forward<decltype(arg)>(arg));
                           });
            return dads::ComplexExpression(
                std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> dads::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> dads::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

} // namespace dads::engines::DictionaryEncoder

static auto &enginePtr(bool initialise = true) {
  static auto engine =
      std::unique_ptr<dads::engines::DictionaryEncoder::Engine>();
  if (!engine && initialise) {
    engine.reset(new dads::engines::DictionaryEncoder::Engine());
  }
  return engine;
}

extern "C" DADSExpression *evaluate(DADSExpression *e) {
  static std::mutex m;
  std::lock_guard lock(m);
  auto *r = new DADSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
