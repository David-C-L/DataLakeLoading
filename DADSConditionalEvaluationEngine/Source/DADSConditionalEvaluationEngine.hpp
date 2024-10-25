#include <DADS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>

namespace dads::engines::ConditionalEvaluation {

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
  std::unordered_map<Symbol, dads::Expression> vars;

  dads::Expression evaluateArgs(dads::Expression &&e);
  dads::Expression toggleExpressionMangling(dads::Expression &&e, bool mangle);
  bool evaluateCondition(dads::Expression &&e);
  dads::Expression evaluateSetOperand(dads::Expression &&e);
  int64_t evaluateIncrementOperand(dads::Expression &&e);
};

extern "C" DADSExpression *evaluate(DADSExpression *e);
} // namespace dads::engines::ConditionalEvaluation
