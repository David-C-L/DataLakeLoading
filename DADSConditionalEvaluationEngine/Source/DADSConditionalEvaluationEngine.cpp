#include "DADSConditionalEvaluationEngine.hpp"
#include <DADS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <unordered_map>
#include <iostream>

using std::string_literals::operator""s;
using dads::utilities::operator""_;
using dads::ComplexExpression;
using dads::Span;
using dads::Symbol;

using dads::Expression;

namespace dads::engines::ConditionalEvaluation {
using std::move;
namespace utilities {
static dads::Expression shallowCopy(dads::Expression const &expression);
static dads::ComplexExpression
shallowCopyComplex(dads::ComplexExpression const &expression);

static dads::ComplexExpression
shallowCopyComplex(dads::ComplexExpression const &expression) {
  auto const &head = expression.getHead();
  auto const &dynamics = expression.getDynamicArguments();
  auto const &spans = expression.getSpanArguments();
  dads::ExpressionArguments dynamicsCopy;
  std::transform(dynamics.begin(), dynamics.end(),
                 std::back_inserter(dynamicsCopy), shallowCopy);
  dads::expressions::ExpressionSpanArguments spansCopy;
  std::transform(
      spans.begin(), spans.end(), std::back_inserter(spansCopy),
      [](auto const &span) {
        return std::visit(
            [](auto const &typedSpan)
                -> dads::expressions::ExpressionSpanArgument {
              // just do a shallow copy of the span
              // the storage's span keeps the ownership
              // (since the storage will be alive until the query finishes)
              using SpanType = std::decay_t<decltype(typedSpan)>;
              using T = std::remove_const_t<typename SpanType::element_type>;
              if constexpr (std::is_same_v<T, bool>) {
                // TODO: this would still keep const spans for bools, need to
                // fix later
                return SpanType(typedSpan.begin(), typedSpan.size(), []() {});
              } else {
                // force non-const value for now (otherwise expressions cannot
                // be moved)
                auto *ptr = const_cast<T *>(typedSpan.begin()); // NOLINT
                return dads::Span<T>(ptr, typedSpan.size(), []() {});
              }
            },
            span);
      });
  return dads::ComplexExpression(head, {}, std::move(dynamicsCopy),
                                 std::move(spansCopy));
}

static dads::Expression shallowCopy(dads::Expression const &expression) {
  return std::visit(
      dads::utilities::overload(
          [&](dads::ComplexExpression const &e) -> dads::Expression {
            return shallowCopyComplex(e);
          },
          [](auto const &arg) -> dads::Expression { return arg; }),
      expression);
}
} // namespace utilities

bool Engine::evaluateCondition(Expression &&e) {
  return std::visit(
      dads::utilities::overload(
          [this](ComplexExpression &&expression) {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return evaluate(std::forward<decltype(arg)>(arg));
                           });
            return evaluateCondition(
                std::move(evaluate(std::move(dads::ComplexExpression(
                    std::move(head), {}, std::move(dynamics),
                    std::move(spans))))));
          },
          [this](Symbol &&symbol) {
            auto it = vars.find(symbol);
            if (it != vars.end() && std::holds_alternative<bool>(it->second)) {
              return std::get<bool>(it->second);
            }
            return false;
          },
          [this](bool result) { return result; },
          [](auto &&arg) { return false; }),
      std::move(e));
}

dads::Expression Engine::evaluateSetOperand(dads::Expression &&e) {
  return std::visit(
      dads::utilities::overload(
          [this](ComplexExpression &&expression) -> dads::Expression {
            return evaluate(std::move(expression));
          },
          [this](Symbol &&symbol) -> dads::Expression {
            auto it = vars.find(symbol);
            if (it == vars.end()) {
              throw std::runtime_error(
                  "Conditional Evaluation: Attempt to assign value of unknown "
                  "symbol in Set expression");
            }
            return utilities::shallowCopy(it->second);
          },
          [](auto &&arg) -> dads::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

int64_t Engine::evaluateIncrementOperand(dads::Expression &&e) {
  return std::visit(dads::utilities::overload(
                        [this](ComplexExpression &&expression) {
                          return evaluateIncrementOperand(
                              std::move(evaluate(std::move(expression))));
                        },
                        [this](Symbol &&symbol) {
                          auto it = vars.find(symbol);
                          if (it == vars.end()) {
                            throw std::runtime_error(
                                "Conditional Evaluation: Attempt to assign "
                                "value of unknown symbol in Set expression");
                          }
                          return evaluateIncrementOperand(
                              std::move(utilities::shallowCopy(it->second)));
                        },
                        [this](int64_t &&arg) { return arg + 1; },
                        [this](int32_t &&arg) { return ((int64_t)arg + 1); },
                        [this](int8_t &&arg) { return ((int64_t)arg + 1); },
                        [](auto &&arg) {
                          return (int64_t)0;
                          // throw std::runtime_error("Conditional Evaluation:
                          // Attempt to increment non-int type");
                        }),
                    std::move(e));
}

std::string removePrefix(std::string str, const std::string& prefix) {
    if (str.size() >= prefix.size() && str.substr(0, prefix.size()) == prefix) {
        str.erase(0, prefix.size());
    }
    return str;
}

std::string addPrefix(const std::string& str, const std::string& prefix) {
  if (str.size() >= prefix.size() && str.substr(0, prefix.size()) == prefix) {
    return str;
  }
  return prefix + str;
}
  
dads::Expression Engine::toggleExpressionMangling(Expression &&e, bool mangle) {
  return std::visit(
      dads::utilities::overload(
				[this, &mangle](ComplexExpression &&expression) -> dads::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (mangle) {
	      head = dads::Symbol(addPrefix(head.getName(), "__"));
	    } else {
	      head = dads::Symbol(removePrefix(head.getName(), "__"));
	    }
	    
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

dads::Expression Engine::evaluateArgs(Expression &&e) {
  return std::visit(
      dads::utilities::overload(
          [this](ComplexExpression &&expression) -> dads::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
           
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
  
dads::Expression Engine::evaluate(Expression &&e) {
  return std::visit(
      dads::utilities::overload(
          [this](ComplexExpression &&expression) -> dads::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            if (head == "Set"_) {
              auto &varSymbol = std::get<dads::Symbol>(dynamics[0]);
              vars[varSymbol] = evaluateSetOperand(std::move(dynamics[1]));
              return utilities::shallowCopy(vars[varSymbol]);
            } else if (head == "Increment"_) {
              return evaluateIncrementOperand(std::move(dynamics[0]));
            } else if (head == "Equals"_) {
              auto first = evaluate(std::move(dynamics[0]));
              auto second = evaluate(std::move(dynamics[1]));
              return first == second;
            } else if (head == "NotEquals"_) {
              auto first = evaluate(std::move(dynamics[0]));
              auto second = evaluate(std::move(dynamics[1]));
              return first != second;
            } else if (head == "EvaluateIf"_) {
              if (dynamics.empty()) {
                throw std::runtime_error(
                    "Conditional Evaluation: No "
                    "subexpressions in EvaluateIf_ expression");
              }
              auto condResult = evaluateCondition(
                  std::move(utilities::shallowCopy(dynamics[0])));
              if (condResult) {
		dynamics[1] = std::move(toggleExpressionMangling(std::move(dynamics[1]), false));
                return evaluate(std::move(dynamics[1]));
              }
	      
	      dynamics[1] = std::move(toggleExpressionMangling(std::move(dynamics[1]), true));
	      dynamics[1] = std::move(evaluateArgs(std::move(dynamics[1])));
	      
              return dads::ComplexExpression(
                  std::move(head), {}, std::move(dynamics), std::move(spans));
            } else if (head == "GetEngineCapabilities"_) {
	      return "List"_("Set"_, "Increment"_, "Equals"_, "NotEquals"_, "EvaluateIf"_);
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
            auto it = vars.find(symbol);
            if (it == vars.end()) {
              return std::move(symbol);
            }
            return utilities::shallowCopy(it->second);
          },
          [](auto &&arg) -> dads::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}
} // namespace dads::engines::ConditionalEvaluation

static auto &enginePtr(bool initialise = true) {
  static auto engine =
      std::unique_ptr<dads::engines::ConditionalEvaluation::Engine>();
  if (!engine && initialise) {
    engine.reset(new dads::engines::ConditionalEvaluation::Engine());
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
