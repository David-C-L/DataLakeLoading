#include "BOSSLazyLoadingCoordinatorEngine.hpp"
#include <BOSS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <iostream>
#include <tuple>
#include <unordered_map>
extern "C" {
#include <BOSS.h>
}

// #define DEBUG

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;

namespace boss::engines::LazyLoadingCoordinator {
using EvalFunction = BOSSExpression *(*)(BOSSExpression *);
using EngineCapabilities = std::vector<Symbol>;
using std::move;
namespace utilities {} // namespace utilities

boss::Expression applyEngine(Expression &&e, EvalFunction eval) {
  auto *r = new BOSSExpression{std::move(e)};
  auto *oldWrapper = r;
  r = eval(r);
  delete oldWrapper;
  auto result = std::move(r->delegate);
  delete r;
  return std::move(result);
}

bool Engine::hasDependents(boss::ComplexExpression const &e) {
  bool res = false;
  std::for_each(e.getDynamicArguments().begin(), e.getDynamicArguments().end(),
                [this, &res](auto const &arg) {
                  if (std::holds_alternative<boss::ComplexExpression>(arg)) {
                    auto &child = get<boss::ComplexExpression>(arg);
                    auto it_capToEngine = capToEngineMap.find(child.getHead());
                    res |= it_capToEngine != capToEngineMap.end() ||
                           hasDependents(child);
                  }
                });
  return res;
}

boss::Expression Engine::depthFirstEvaluate(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [&, this](ComplexExpression &&expression) -> boss::Expression {
            auto isLeaf = !hasDependents(expression);
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            auto it_capToEngine = capToEngineMap.find(head);
            if (it_capToEngine == capToEngineMap.end()) {
              return boss::ComplexExpression(
                  std::move(head), {}, std::move(dynamics), std::move(spans));
            }

            auto &evalFunc = evalFuncs[it_capToEngine->second];
#ifdef DEBUG
            std::cout << "ENGINE: " << it_capToEngine->second << std::endl;
            std::cout << "HEAD: " << it_capToEngine->first << std::endl;
            std::cout << "IS LEAF: " << isLeaf << std::endl;
#endif

            if (isLeaf) {
#ifdef DEBUG
              std::cout << "APPLY LEAF: " << it_capToEngine->first << std::endl;
#endif
              return applyEngine(std::move(boss::ComplexExpression(
                                     std::move(head), {}, std::move(dynamics),
                                     std::move(spans))),
                                 evalFunc);
            } else {
              std::transform(std::make_move_iterator(dynamics.begin()),
                             std::make_move_iterator(dynamics.end()),
                             dynamics.begin(), [this](auto &&arg) {
                               return depthFirstEvaluate(
                                   std::forward<decltype(arg)>(arg));
                             });
#ifdef DEBUG
              std::cout << "APPLY NON-LEAF: " << it_capToEngine->first
                        << std::endl;
#endif
              return applyEngine(std::move(boss::ComplexExpression(
                                     std::move(head), {}, std::move(dynamics),
                                     std::move(spans))),
                                 evalFunc);
            }
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

boss::Expression Engine::depthFirstEvaluate(Expression &&e, bool isNesting) {
  return std::visit(
      boss::utilities::overload(
          [&, this](ComplexExpression &&expression) -> boss::Expression {
            auto isLeaf = !hasDependents(expression);
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            auto it_capToEngine = capToEngineMap.find(head);
            if (it_capToEngine == capToEngineMap.end()) {
	      
	      std::transform(std::make_move_iterator(dynamics.begin()),
			     std::make_move_iterator(dynamics.end()),
			     dynamics.begin(), [this](auto &&arg) {
			       return depthFirstEvaluate(
							 std::forward<decltype(arg)>(arg), true);
			     });
              return boss::ComplexExpression(
                  std::move(head), {}, std::move(dynamics), std::move(spans));
            }

            auto &evalFunc = evalFuncs[it_capToEngine->second];
#ifdef DEBUG
	    
            std::cout << "\nENGINE: " << it_capToEngine->second << std::endl;
            std::cout << "HEAD: " << it_capToEngine->first << std::endl;
            std::cout << "IS LEAF: " << isLeaf << std::endl;
#endif
            if ((it_capToEngine->first != "Project"_ &&
                 it_capToEngine->first != "Select"_ &&
                 it_capToEngine->first != "Group"_ &&
                 it_capToEngine->first != "Join"_&&
                 it_capToEngine->first != "Top"_ &&
		 it_capToEngine->first != "Order"_) &&
                isLeaf) {
#ifdef DEBUG
              std::cout << "APPLY LEAF: " << it_capToEngine->first << std::endl;
#endif
              return applyEngine(std::move(boss::ComplexExpression(
                                     std::move(head), {}, std::move(dynamics),
                                     std::move(spans))),
                                 evalFunc);
            } else if (it_capToEngine->first != "Project"_ &&
                       it_capToEngine->first != "Select"_ &&
                       it_capToEngine->first != "Group"_ &&
                       it_capToEngine->first != "Join"_ &&
                       it_capToEngine->first != "Top"_ &&
                       it_capToEngine->first != "Order"_) {
              std::transform(std::make_move_iterator(dynamics.begin()),
                             std::make_move_iterator(dynamics.end()),
                             dynamics.begin(), [this](auto &&arg) {
                               return depthFirstEvaluate(
                                   std::forward<decltype(arg)>(arg), false);
                             });
#ifdef DEBUG
              std::cout << "APPLY NON-LEAF: " << it_capToEngine->first
                        << std::endl;
#endif
              return applyEngine(std::move(boss::ComplexExpression(
                                     std::move(head), {}, std::move(dynamics),
                                     std::move(spans))),
                                 evalFunc);
            }
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return depthFirstEvaluate(
                                 std::forward<decltype(arg)>(arg), true);
                           });

            if (isNesting) {
#ifdef DEBUG
              std::cout << "NESTED EXPRESSION: " << it_capToEngine->first
                        << std::endl;
#endif	    
              return boss::ComplexExpression(
                  std::move(head), {}, std::move(dynamics), std::move(spans));
            }
#ifdef DEBUG
            std::cout << "APPLY: " << it_capToEngine->first << std::endl;

            if (head == "Group"_) {
#ifdef DEBUG
              std::cout << "SETTING THREAD TO 2" << std::endl;
#endif
              applyEngine("Set"_("maxThreads"_, 2), evalFunc);
            } else {
#ifdef DEBUG
              std::cout << "SETTING THREAD TO 1" << std::endl;
#endif
              applyEngine("Set"_("maxThreads"_, 1), evalFunc);
            }
#endif
            return applyEngine(std::move(boss::ComplexExpression(
                                   std::move(head), {}, std::move(dynamics),
                                   std::move(spans))),
                               evalFunc);
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

  boss::Expression Engine::evaluateCycle(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [&, this](ComplexExpression &&expression) -> boss::Expression {
	    
#ifdef DEBUG	    
            std::cout << "\n\nSTAGE: " << stage << std::endl;
            std::cout << "START EXPR: " << expression << std::endl;
#endif
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Table"_) {
	      return std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));
	    }

	    int32_t &condEng = engineNames["Conditional"_];
	    int32_t &veloxEng = engineNames["Velox"_];
	    int32_t &dictEncEng = engineNames["DictionaryEncoder"_];
	    int32_t &wisentEng = engineNames["Wisent"_];

	    EvalFunction &condEvalFunc = evalFuncs[condEng];
	    EvalFunction &veloxEvalFunc = evalFuncs[veloxEng];
	    EvalFunction &dictEncEvalFunc = evalFuncs[dictEncEng];
	    EvalFunction &wisentEvalFunc = evalFuncs[wisentEng];
	    
	    Expression curr = std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));

	    curr = inputStageValue(std::move(curr));
#ifdef DEBUG	    
            std::cout << "\nAFTER INPUT STAGE VALUE: " << curr << std::endl;
#endif
	    curr = applyEngine(std::move(curr), condEvalFunc);
#ifdef DEBUG	    
            std::cout << "\nAFTER COND EVAL: " << curr << std::endl;
#endif
	    curr = inputStageSymbol(std::move(curr));
#ifdef DEBUG	    
            std::cout << "\nAFTER INPUT STAGE SYMBOL: " << curr << std::endl;
#endif
	    curr = applyEngine(std::move(curr), wisentEvalFunc);	    
#ifdef DEBUG	    
            std::cout << "\nAFTER WISENT: " << curr << std::endl;
#endif
	    curr = applyEngine(std::move(curr), dictEncEvalFunc);	    	    
#ifdef DEBUG	    
            std::cout << "\nAFTER DICT 2: " << curr << std::endl;
#endif
	    curr = applyEngine(std::move(curr), veloxEvalFunc);	    
#ifdef DEBUG	    
            std::cout << "\nAFTER VELOX: " << curr << std::endl;
#endif
	    stage++;

	    return std::move(evaluateCycle(std::move(curr)));
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

  boss::Expression Engine::inputStageValue(Expression &&e) {
    return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
	    
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return inputStageValue(std::forward<decltype(arg)>(arg));
                           });
            return boss::ComplexExpression(
					   std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> boss::Expression {
	    if (symbol == "STAGE"_) {
	      return stage;
	    }
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
  }

  boss::Expression Engine::inputStageSymbol(Expression &&e) {
    return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Equals"_) {
	      dynamics[0] = "STAGE"_;
	      return boss::ComplexExpression(
					     std::move(head), {}, std::move(dynamics), std::move(spans));
	    }
	    
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return inputStageSymbol(std::forward<decltype(arg)>(arg));
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

boss::Expression Engine::evaluate(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            if (head == "EvaluateInEngines"_) {

              // SETUP
              auto &evalFuncAddrs = std::get<boss::Span<int64_t>>(spans.at(0));
              for (auto [it, i] = std::tuple{evalFuncAddrs.begin(), 0};
                   it != evalFuncAddrs.end(); ++it, ++i) {
#ifdef DEBUG
                std::cout << "ENGINE: " << i << std::endl;
#endif
                auto evalFunc = reinterpret_cast<EvalFunction>(*it);
                EngineCapabilities engineCaps;
                evalFuncs[i] = evalFunc;

                auto engineCap =
                    applyEngine(boss::ComplexExpression(
                                    "GetEngineCapabilities"_, {}, {}, {}),
                                evalFunc);
                auto args = get<ComplexExpression>(engineCap).getArguments();
                std::for_each(std::make_move_iterator(args.begin()),
                              std::make_move_iterator(args.end()),
                              [this, &engineCaps](auto &&argument) {
                                engineCaps.push_back(get<Symbol>(argument));
                              });
                for (auto &cap : engineCaps) {
                  capToEngineMap[cap] = i;
#ifdef DEBUG
                  std::cout << "CAP: " << cap << std::endl;
#endif
                }
                engineCapsMap[i] = engineCaps;

		if (std::find(engineCaps.begin(), engineCaps.end(), "EvaluateIf"_) != engineCaps.end()) {
		  engineNames["Conditional"_] = i;
		} else if (std::find(engineCaps.begin(), engineCaps.end(), "Project"_) != engineCaps.end()) {
		  engineNames["Velox"_] = i;
		} else if (std::find(engineCaps.begin(), engineCaps.end(), "EncodeTable"_) != engineCaps.end()) {
		  engineNames["DictionaryEncoder"_] = i;
		} else if (std::find(engineCaps.begin(), engineCaps.end(), "GatherRanges"_) != engineCaps.end()) {
		  engineNames["Wisent"_] = i;
		}

              }

	      if (std::holds_alternative<boss::Symbol>(dynamics[0]) && std::get<boss::Symbol>(dynamics[0]) == "NonLECycle"_) {
		std::for_each(std::make_move_iterator(std::next(dynamics.begin())),
			      std::make_move_iterator(std::prev(dynamics.end())),
			      [this](auto &&argument) {
				depthFirstEvaluate(std::move(argument), false);
			      });

		return depthFirstEvaluate(std::move(*std::prev(dynamics.end())),
					  false);
	      }
	      
              std::for_each(std::make_move_iterator(dynamics.begin()),
                            std::make_move_iterator(std::prev(dynamics.end())),
                            [this](auto &&argument) {
			      stage = 0;
                              evaluateCycle(std::move(argument));
                            });

	      stage = 0;
              return evaluateCycle(std::move(*std::prev(dynamics.end())));
            }
            return std::move("Failure"_);
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}
} // namespace boss::engines::LazyLoadingCoordinator

static auto &enginePtr(bool initialise = true) {
  static auto engine =
      std::unique_ptr<boss::engines::LazyLoadingCoordinator::Engine>();
  if (!engine && initialise) {
    engine.reset(new boss::engines::LazyLoadingCoordinator::Engine());
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
