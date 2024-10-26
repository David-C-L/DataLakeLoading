#include <DADS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>

namespace dads::engines::LazyLoadingCoordinator {

using EvalFunction = DADSExpression *(*)(DADSExpression *);
using EngineCapabilities = std::vector<Symbol>;

class Engine {

public:
  Engine(Engine &) = delete;

  Engine &operator=(Engine &) = delete;

  Engine(Engine &&) = default;

  Engine &operator=(Engine &&) = delete;

  Engine() = default;

  ~Engine() = default;

  dads::Expression evaluate(dads::Expression &&e);

private:

  int32_t stage;
  std::unordered_map<Symbol, int32_t> engineNames;
  std::unordered_map<int32_t, EvalFunction> evalFuncs;
  std::unordered_map<int32_t, EngineCapabilities> engineCapsMap;
  std::unordered_map<Symbol, int32_t> capToEngineMap;

  dads::Expression inputStageValue(dads::Expression &&e);
  dads::Expression inputStageSymbol(dads::Expression &&e);
  dads::Expression fullyWrapInConditionals(dads::Expression &&e);
  dads::Expression evaluateCycle(dads::Expression &&e);
  dads::Expression depthFirstEvaluate(dads::Expression &&e);
  dads::Expression depthFirstEvaluate(dads::Expression &&e, bool isNesting);
  dads::Expression lazyLoadEvaluate(dads::Expression &&e);
  dads::Expression lazyLoadApplySelectionHeuristic(dads::Expression &&e);
  bool hasDependents(dads::ComplexExpression const &e);
};

extern "C" DADSExpression *evaluate(DADSExpression *e);
} // namespace dads::engines::LazyLoadingCoordinator
