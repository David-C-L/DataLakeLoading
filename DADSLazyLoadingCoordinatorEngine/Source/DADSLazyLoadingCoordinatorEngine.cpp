#include "DADSLazyLoadingCoordinatorEngine.hpp"
#include <DADS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <iostream>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <functional>
extern "C" {
#include <DADS.h>
}

// #define DEBUG

using std::string_literals::operator""s;
using dads::utilities::operator""_;
using dads::ComplexExpression;
using dads::Span;
using dads::Symbol;

using dads::Expression;

namespace dads::engines::LazyLoadingCoordinator {
using EvalFunction = DADSExpression *(*)(DADSExpression *);
using EngineCapabilities = std::vector<Symbol>;
using std::move;
namespace utilities {} // namespace utilities

  struct RemoteTableInfo {
    std::string url;
    bool hasRangeIndex = false;
    bool hasIndices = false;
    dads::Symbol rangeStart = "None"_;
    dads::Symbol rangeEnd = "None"_;
    dads::Expression indices = "None"_;
  };
  
dads::Expression applyEngine(Expression &&e, EvalFunction eval) {
#ifdef DEBUG
  // std::cout << "EXPRESSION: \n" << e << "\n" << std::endl;
#endif
  auto *r = new DADSExpression{std::move(e)};
  auto *oldWrapper = r;
  r = eval(r);
  delete oldWrapper;
  auto result = std::move(r->delegate);
  delete r;
  return std::move(result);
}
  
std::string RBL_PATH =
    "/home/david/Documents/PhD/symbol-store/DADSRemoteBinaryLoaderEngine/"
    "debugBuild/libDADSRemoteBinaryLoaderEngine.so";

// dads::Expression Engine::depthFirstExtract(Expression &&e) {
//   return std::visit(
//     dads::utilities::overload(
//         [this](ComplexExpression &&expression) -> dads::Expression {
// 	    auto [head, unused_, dynamics, spans] =
// 	      std::move(expression).decompose();

// 	    std::transform(std::make_move_iterator(dynamics.begin()),
// 			   std::make_move_iterator(dynamics.end()),
// 			   dynamics.begin(), [this](auto &&arg) {
// 			     return
// depthFirstExtract(std::forward<decltype(arg)>(arg));
// 			   });

// 	    auto it_capToEngine = capToEngineMap.find(head);
// 	    if (it_capToEngine == capToEngineMap.end()) {
// 	      return std::move(dynamics[0]);
// 	    }

// 	    auto& evalFunc = evalFuncs[it_capToEngine->second];

// 	    return
// applyEngine(std::move(dads::ComplexExpression(std::move(head), {},
// std::move(dynamics), std::move(spans))), evalFunc);
//         },
//         [this](Symbol &&symbol) -> dads::Expression {
//           return std::move(symbol);
//         },
//         [](auto &&arg) -> dads::Expression {
//           return std::forward<decltype(arg)>(arg);
//         }),
//     std::move(e));
// }

bool Engine::hasDependents(dads::ComplexExpression const &e) {
  bool res = false;
  std::for_each(e.getDynamicArguments().begin(), e.getDynamicArguments().end(),
                [this, &res](auto const &arg) {
                  if (std::holds_alternative<dads::ComplexExpression>(arg)) {
                    auto &child = get<dads::ComplexExpression>(arg);
                    auto it_capToEngine = capToEngineMap.find(child.getHead());
                    res |= it_capToEngine != capToEngineMap.end() ||
                           hasDependents(child);
                  }
                });
  return res;
}

dads::Expression Engine::depthFirstEvaluate(Expression &&e) {
  return std::visit(
      dads::utilities::overload(
          [&, this](ComplexExpression &&expression) -> dads::Expression {
            auto isLeaf = !hasDependents(expression);
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            auto it_capToEngine = capToEngineMap.find(head);
            if (it_capToEngine == capToEngineMap.end()) {
              return dads::ComplexExpression(
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
              return applyEngine(std::move(dads::ComplexExpression(
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
              return applyEngine(std::move(dads::ComplexExpression(
                                     std::move(head), {}, std::move(dynamics),
                                     std::move(spans))),
                                 evalFunc);
            }
          },
          [this](Symbol &&symbol) -> dads::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> dads::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

dads::Expression Engine::depthFirstEvaluate(Expression &&e, bool isNesting) {
  return std::visit(
      dads::utilities::overload(
          [&, this](ComplexExpression &&expression) -> dads::Expression {
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
              return dads::ComplexExpression(
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
              return applyEngine(std::move(dads::ComplexExpression(
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
              return applyEngine(std::move(dads::ComplexExpression(
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
              return dads::ComplexExpression(
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
            return applyEngine(std::move(dads::ComplexExpression(
                                   std::move(head), {}, std::move(dynamics),
                                   std::move(spans))),
                               evalFunc);
          },
          [this](Symbol &&symbol) -> dads::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> dads::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

// dads::Expression Engine::lazyLoadApplySelectHeuristic(Expression &&e) {
//   return std::visit(
//     dads::utilities::overload(
//         [this](ComplexExpression &&expression) -> dads::Expression {
// 	    auto [head, unused_, dynamics, spans] =
// 	      std::move(expression).decompose();

// 	    // auto loaded = depthFirstExtract(std::move(dynamics[1]));

// 	    auto it_capToEngine = capToEngineMap.find(head);
// 	    if (it_capToEngine == capToEngineMap.end()) {
// 	      return dads::ComplexExpression(std::move(head), {},
// std::move(dynamics), std::move(spans));
// 	    }

// 	    auto& evalFunc = evalFuncs[it_capToEngine->second];
// 	    std::cout << "ENGINE: " << it_capToEngine->second << std::endl;
// 	    std::cout << "HEAD: " << it_capToEngine->first << std::endl;

// 	    return
// applyEngine(std::move(dads::ComplexExpression(std::move(head), {},
// std::move(dynamics), std::move(spans))), evalFunc);
//         },
//         [this](Symbol &&symbol) -> dads::Expression {
//           return std::move(symbol);
//         },
//         [](auto &&arg) -> dads::Expression {
//           return std::forward<decltype(arg)>(arg);
//         }),
//     std::move(e));
// }

// dads::Expression Engine::lazyLoadEvaluate(Expression &&e) {
//   return std::visit(
//     dads::utilities::overload(
//         [this](ComplexExpression &&expression) -> dads::Expression {
// 	    auto head = expression.getHead();
// 	    if (head == "Select"_) {
// 	      return lazyLoadApplySelectHeuristic(Expression &&e);
// 	    }
// 	    return std::move("Failure"_);
//         },
//         [this](Symbol &&symbol) -> dads::Expression {
//           return std::move(symbol);
//         },
//         [](auto &&arg) -> dads::Expression {
//           return std::forward<decltype(arg)>(arg);
//         }),
//     std::move(e));
// }

  dads::Expression Engine::fullyWrapInConditionals(dads::Expression &&e) {
    return std::visit(
      dads::utilities::overload(
          [this](ComplexExpression &&expression) -> dads::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
	    
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return fullyWrapInConditionals(std::forward<decltype(arg)>(arg));
                           });
	    dads::ExpressionArguments evalIfDyns;
	    evalIfDyns.push_back("HOLD"_);
	    evalIfDyns.push_back(std::move(dads::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans))));
	    
	    return dads::ComplexExpression("EvaluateIf"_, {}, std::move(evalIfDyns), {});
          },
          [this](Symbol &&symbol) -> dads::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> dads::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
  }

  dads::ComplexExpression getListFromSet(std::unordered_set<dads::Symbol> &colNames) {
    dads::ExpressionArguments args;
    args.reserve(colNames.size());
    for (auto &colName : colNames) {
      args.push_back(colName);
    }

    return dads::ComplexExpression("List"_, {}, std::move(args), {});
  }
  
  dads::Expression getColumnNames(dads::Expression const &e, std::unordered_set<dads::Symbol> &colNames) {
    return std::visit(
      dads::utilities::overload(
	  [&colNames](ComplexExpression const &expression) -> dads::Expression {
            auto &dynamics = expression.getDynamicArguments();
	    if (expression.getHead().getName() == "As") {
	      for (int i = 1; i < dynamics.size(); i += 2) {
		getColumnNames(dynamics[i], colNames);
	      }
	    } else {
	      for (int i = 0; i < dynamics.size(); i++) {
		getColumnNames(dynamics[i], colNames);
	      }
	    }
	    return "Success"_;
          },
	  [&colNames](Symbol const &symbol) -> dads::Expression {
	    colNames.insert(symbol);
            return "Success"_;
          },
          [](auto const &arg) -> dads::Expression {
            return "Success"_;
          }),
      e);
  }

  RemoteTableInfo extractRemoteTableInfo(dads::ComplexExpression &&e) {
    RemoteTableInfo res;
    auto [head, unused_, dynamics, spans] =
      std::move(e).decompose();

    if (head == "RemoteTable"_ ||
	head == "RangedRemoteTable"_ ||
	head == "RemoteTableWithIndices"_ ||
	head == "RangedRemoteTableWithIndices"_) {
      res.url = std::get<std::string>(dynamics[0]);

      if (head == "RangeRemoteTable"_ || head == "RangedRemoteTableWithIndices"_) {
	res.rangeStart = std::get<dads::Symbol>(dynamics[1]);
	res.rangeEnd = std::get<dads::Symbol>(dynamics[2]);
	res.hasRangeIndex = true;
      }

      if (head == "RemoteTableWithIndices"_ || head == "RangedRemoteTableWithIndices"_) {
	res.indices = std::move(dynamics[dynamics.size() - 1]);
	res.hasIndices = true;
      }
    }
    
    return res;
  }

  dads::Expression createGatherExpression(RemoteTableInfo &remoteTabInfo, dads::ComplexExpression &&colNames) {
    dads::Symbol gatherHead = "Gather"_;
    dads::ExpressionArguments gatherArgs;
    gatherArgs.push_back(remoteTabInfo.url);
    gatherArgs.push_back(RBL_PATH);

    if (remoteTabInfo.hasRangeIndex && remoteTabInfo.hasIndices) {
      gatherHead = "GatherRanges"_;
      gatherArgs.push_back(remoteTabInfo.rangeStart);
      gatherArgs.push_back(remoteTabInfo.rangeEnd);
    } else if (remoteTabInfo.hasRangeIndex && !remoteTabInfo.hasIndices) {
      auto [listHead, unused_, listDynamics, listSpans] =
                std::move(colNames).decompose();
      listDynamics.push_back(remoteTabInfo.rangeStart);
      listDynamics.push_back(remoteTabInfo.rangeEnd);
      colNames = dads::ComplexExpression(std::move(listHead), {}, std::move(listDynamics), std::move(listSpans));
    }
    
    if (remoteTabInfo.hasIndices) {
      gatherArgs.push_back(std::move(remoteTabInfo.indices));
      remoteTabInfo.hasIndices = false;
      remoteTabInfo.indices = "None"_;
    } else {
      gatherArgs.push_back("List"_("List"_()));
    }
    
    gatherArgs.push_back(std::move(colNames));

    return dads::ComplexExpression(std::move(gatherHead), {}, std::move(gatherArgs), {});
  }

  dads::Expression createRemoteTableWithIndicesExpression(RemoteTableInfo &remoteTabInfo, dads::Expression &&dataTable) {
    dads::Symbol remoteTabHead = "RemoteTableWithIndices"_;
    dads::ExpressionArguments remoteTabArgs;
    remoteTabArgs.push_back(remoteTabInfo.url);
    remoteTabArgs.push_back(std::move(dataTable));
    if (remoteTabInfo.hasRangeIndex) {
      remoteTabHead = "RangedRemoteTableWithIndices"_;
      remoteTabArgs.push_back(remoteTabInfo.rangeStart);
      remoteTabArgs.push_back(remoteTabInfo.rangeEnd);
    }

    return dads::ComplexExpression(std::move(remoteTabHead), {}, std::move(remoteTabArgs), {});
  }


  dads::Expression Engine::evaluateCycle(Expression &&e) {
  return std::visit(
      dads::utilities::overload(
          [&, this](ComplexExpression &&expression) -> dads::Expression {
	    
#ifdef DEBUG	    
            std::cout << "\n\nSTAGE: " << stage << std::endl;
            std::cout << "START EXPR: " << expression << std::endl;
#endif
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Table"_) {
	      return std::move(dads::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));
	    }

	    int32_t &condEng = engineNames["Conditional"_];
	    int32_t &veloxEng = engineNames["Velox"_];
	    int32_t &dictEncEng = engineNames["DictionaryEncoder"_];
	    int32_t &fileFormatLoaderEng = engineNames["FileFormatLoader"_];

	    EvalFunction &condEvalFunc = evalFuncs[condEng];
	    EvalFunction &veloxEvalFunc = evalFuncs[veloxEng];
	    EvalFunction &dictEncEvalFunc = evalFuncs[dictEncEng];
	    EvalFunction &fileFormatLoaderEvalFunc = evalFuncs[fileFormatLoaderEng];
	    
	    Expression curr = std::move(dads::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));

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
// 	    curr = applyEngine(std::move(curr), dictEncEvalFunc);	    	    
// #ifdef DEBUG	    
//             std::cout << "\nAFTER DICT 1: " << curr << std::endl;
// #endif
	    curr = applyEngine(std::move(curr), fileFormatLoaderEvalFunc);	    
#ifdef DEBUG	    
            std::cout << "\nAFTER FILE FORMAT LOADER: " << curr << std::endl;
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
          [this](Symbol &&symbol) -> dads::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> dads::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

  dads::Expression Engine::inputStageValue(Expression &&e) {
    return std::visit(
      dads::utilities::overload(
          [this](ComplexExpression &&expression) -> dads::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
	    
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return inputStageValue(std::forward<decltype(arg)>(arg));
                           });
            return dads::ComplexExpression(
					   std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> dads::Expression {
	    if (symbol == "STAGE"_) {
	      return stage;
	    }
            return std::move(symbol);
          },
          [](auto &&arg) -> dads::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
  }

  dads::Expression Engine::inputStageSymbol(Expression &&e) {
    return std::visit(
      dads::utilities::overload(
          [this](ComplexExpression &&expression) -> dads::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Equals"_) {
	      dynamics[0] = "STAGE"_;
	      return dads::ComplexExpression(
					     std::move(head), {}, std::move(dynamics), std::move(spans));
	    }
	    
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return inputStageSymbol(std::forward<decltype(arg)>(arg));
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
            if (head == "EvaluateInEngines"_) {

              // SETUP
              auto &evalFuncAddrs = std::get<dads::Span<int64_t>>(spans.at(0));
              for (auto [it, i] = std::tuple{evalFuncAddrs.begin(), 0};
                   it != evalFuncAddrs.end(); ++it, ++i) {
#ifdef DEBUG
                std::cout << "ENGINE: " << i << std::endl;
#endif
                auto evalFunc = reinterpret_cast<EvalFunction>(*it);
                EngineCapabilities engineCaps;
                evalFuncs[i] = evalFunc;

                auto engineCap =
                    applyEngine(dads::ComplexExpression(
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
		} else if (std::find(engineCaps.begin(), engineCaps.end(), "GatherRanges"_) != engineCaps.end() || std::find(engineCaps.begin(), engineCaps.end(), "GetColumnsFromParquet"_) != engineCaps.end()) {
		  engineNames["FileFormatLoader"_] = i;
		}

              }

	      if (std::holds_alternative<dads::Symbol>(dynamics[0]) && std::get<dads::Symbol>(dynamics[0]) == "NonLECycle"_) {
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
          [this](Symbol &&symbol) -> dads::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> dads::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}
} // namespace dads::engines::LazyLoadingCoordinator

static auto &enginePtr(bool initialise = true) {
  static auto engine =
      std::unique_ptr<dads::engines::LazyLoadingCoordinator::Engine>();
  if (!engine && initialise) {
    engine.reset(new dads::engines::LazyLoadingCoordinator::Engine());
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
