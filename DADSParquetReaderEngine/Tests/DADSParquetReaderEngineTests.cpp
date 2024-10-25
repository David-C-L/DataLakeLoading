#include <string_view>
#define CATCH_CONFIG_RUNNER
#include "../Source/DADSParquetReaderEngine.hpp"
#include <DADS.hpp>
#include <ExpressionUtilities.hpp>
#include <array>
#include <catch2/catch.hpp>
#include <numeric>
#include <typeinfo>
#include <variant>
using dads::Expression;
using std::string;
using std::literals::string_literals::
operator""s;                        // NOLINT(misc-unused-using-decls)
                                    // clang-tidy bug
using dads::utilities::operator""_; // NOLINT(misc-unused-using-decls)
                                    // clang-tidy bug
using Catch::Generators::random;
using Catch::Generators::take;
using Catch::Generators::values;
using std::vector;
using namespace Catch::Matchers;
using dads::expressions::CloneReason;
using dads::expressions::ComplexExpression;
using dads::expressions::generic::get;
using dads::expressions::generic::get_if;
using dads::expressions::generic::holds_alternative;
namespace dads {
using dads::expressions::atoms::Span;
};
using std::int64_t;

namespace {
std::vector<string>
    librariesToTest{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
}

// NOLINTBEGIN(readability-magic-numbers)
// NOLINTBEGIN(bugprone-exception-escape)
// NOLINTBEGIN(readability-function-cognitive-complexity)
TEST_CASE("Subspans work correctly", "[spans]") {
  auto input = dads::Span<int64_t>{std::vector<int64_t>{1, 2, 4, 3}};
  auto subrange = std::move(input).subspan(1, 3);
  CHECK(subrange.size() == 3);
  CHECK(subrange[0] == 2);
  CHECK(subrange[1] == 4);
  CHECK(subrange[2] == 3);
  auto subrange2 =
      dads::Span<int64_t>{std::vector<int64_t>{1, 2, 3, 2}}.subspan(2);
  CHECK(subrange2[0] == 3);
  CHECK(subrange2[1] == 2);
}

// Test each functionality the engine provides
TEST_CASE("Enter Functionality", "[basics]") { // NOLINT
  dads::engines::ParquetReader::Engine engine;
  auto col1 = dads::Span<int32_t>{std::vector<int32_t>{1, 2, 4, 3}};
  auto col2 =
      dads::Span<std::string>{std::vector<std::string>{"a", "b", "c", "d"}};
  auto col3 =
      dads::Span<std::string>{std::vector<std::string>{"d", "d", "a", "d"}};
  auto table = "Table"_("col1"_("List"_(std::move(col1))),
                        "col2"_("List"_(std::move(col2))),
                        "col3"_("List"_(std::move(col3))));

  dads::Symbol expectedResult = "SomeEvaluationResult"_;

  auto res = engine.evaluate("EnterFunctionality"_(std::move(table)));

  CHECK(res == expectedResult);
}

int main(int argc, char *argv[]) {
  Catch::Session session;
  session.cli(session.cli() |
              Catch::clara::Opt(librariesToTest, "library")["--library"]);
  auto const returnCode = session.applyCommandLine(argc, argv);
  if (returnCode != 0) {
    return returnCode;
  }
  return session.run();
}