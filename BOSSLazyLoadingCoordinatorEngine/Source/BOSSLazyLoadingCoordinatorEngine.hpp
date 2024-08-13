#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>

namespace boss::engines::LazyLoadingCoordinator {

using EvalFunction = BOSSExpression *(*)(BOSSExpression *);
using EngineCapabilities = std::vector<Symbol>;

class Engine {

public:
  Engine(Engine &) = delete;

  Engine &operator=(Engine &) = delete;

  Engine(Engine &&) = default;

  Engine &operator=(Engine &&) = delete;

  Engine() = default;

  ~Engine() = default;

  boss::Expression evaluate(boss::Expression &&e);

private:

  int32_t stage;
  std::unordered_map<Symbol, int32_t> engineNames;
  std::unordered_map<int32_t, EvalFunction> evalFuncs;
  std::unordered_map<int32_t, EngineCapabilities> engineCapsMap;
  std::unordered_map<Symbol, int32_t> capToEngineMap;

  boss::Expression inputStageValue(boss::Expression &&e);
  boss::Expression inputStageSymbol(boss::Expression &&e);
  boss::Expression evaluateCycle(boss::Expression &&e);
  boss::Expression depthFirstEvaluate(boss::Expression &&e);
  boss::Expression depthFirstEvaluate(boss::Expression &&e, bool isNesting);
  boss::Expression lazyLoadEvaluate(boss::Expression &&e);
  boss::Expression lazyLoadApplySelectionHeuristic(boss::Expression &&e);
  bool hasDependents(boss::ComplexExpression const &e);
};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
} // namespace boss::engines::LazyLoadingCoordinator
