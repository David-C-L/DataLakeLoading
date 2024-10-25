#include <string_view>
#define CATCH_CONFIG_RUNNER
#include "../Source/DADSDictionaryEncoderEngine.hpp"
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

TEST_CASE("Encode Table", "[basics]") { // NOLINT
  dads::engines::DictionaryEncoder::Engine engine;
  auto col1 = dads::Span<int32_t>{std::vector<int32_t>{1, 2, 4, 3}};
  auto col2 =
      dads::Span<std::string>{std::vector<std::string>{"a", "b", "c", "d"}};
  auto col3 =
      dads::Span<std::string>{std::vector<std::string>{"d", "d", "a", "d"}};
  auto table = "Table"_("col1"_("List"_(std::move(col1))),
                        "col2"_("List"_(std::move(col2))),
                        "col3"_("List"_(std::move(col3))));

  auto expectedCol1 = dads::Span<int32_t>{std::vector<int32_t>{1, 2, 4, 3}};
  auto expectedCol2 = dads::Span<int32_t>{std::vector<int32_t>{0, 1, 2, 3}};
  auto expectedCol3 = dads::Span<int32_t>{std::vector<int32_t>{0, 0, 1, 0}};
  auto expectedTable = "Table"_("col1"_("List"_(std::move(expectedCol1))),
                                "col2"_("List"_(std::move(expectedCol2))),
                                "col3"_("List"_(std::move(expectedCol3))));

  auto res = engine.evaluate("EncodeTable"_(std::move(table)));

  CHECK(res == expectedTable);
}

TEST_CASE("Get Encoding", "[basics]") { // NOLINT
  dads::engines::DictionaryEncoder::Engine engine;
  auto col1 = dads::Span<int32_t>{std::vector<int32_t>{1, 2, 4, 3}};
  auto col2 =
      dads::Span<std::string>{std::vector<std::string>{"a", "b", "c", "d"}};
  auto col3 =
      dads::Span<std::string>{std::vector<std::string>{"d", "d", "a", "d"}};
  auto table = "Table"_("col1"_("List"_(std::move(col1))),
                        "col2"_("List"_(std::move(col2))),
                        "col3"_("List"_(std::move(col3))));

  auto expectedCol1 = dads::Span<int32_t>{std::vector<int32_t>{1, 2, 4, 3}};
  auto expectedCol2 = dads::Span<int32_t>{std::vector<int32_t>{0, 1, 2, 3}};
  auto expectedCol3 = dads::Span<int32_t>{std::vector<int32_t>{0, 0, 1, 0}};
  auto expectedTable = "Table"_("col1"_("List"_(std::move(expectedCol1))),
                                "col2"_("List"_(std::move(expectedCol2))),
                                "col3"_("List"_(std::move(expectedCol3))));
  engine.evaluate("EncodeTable"_(std::move(table)));

  auto res = engine.evaluate("GetEncodingFor"_("a", "col2"_));

  CHECK(res == 0);
}

TEST_CASE("Decode Table", "[basics]") { // NOLINT
  dads::engines::DictionaryEncoder::Engine engine;
  auto col1 = dads::Span<int32_t>{std::vector<int32_t>{1, 2, 4, 3}};
  auto col2 =
      dads::Span<std::string>{std::vector<std::string>{"a", "b", "c", "d"}};
  auto col3 =
      dads::Span<std::string>{std::vector<std::string>{"d", "d", "a", "d"}};
  auto table = "Table"_("col1"_("List"_(std::move(col1))),
                        "col2"_("List"_(std::move(col2))),
                        "col3"_("List"_(std::move(col3))));
  
  auto colCopy1 = dads::Span<int32_t>{std::vector<int32_t>{1, 2, 4, 3}};
  auto colCopy2 =
      dads::Span<std::string>{std::vector<std::string>{"a", "b", "c", "d"}};
  auto colCopy3 =
      dads::Span<std::string>{std::vector<std::string>{"d", "d", "a", "d"}};
  auto tableCopy = "Table"_("col1"_("List"_(std::move(colCopy1))),
                        "col2"_("List"_(std::move(colCopy2))),
                        "col3"_("List"_(std::move(colCopy3))));

  auto expectedCol1 = dads::Span<int32_t>{std::vector<int32_t>{1, 2, 4, 3}};
  auto expectedCol2 = dads::Span<int32_t>{std::vector<int32_t>{0, 1, 2, 3}};
  auto expectedCol3 = dads::Span<int32_t>{std::vector<int32_t>{0, 0, 1, 0}};
  auto expectedTable = "Table"_("col1"_("List"_(std::move(expectedCol1))),
                                "col2"_("List"_(std::move(expectedCol2))),
                                "col3"_("List"_(std::move(expectedCol3))));

  auto res = engine.evaluate("EncodeTable"_(std::move(table)));

  CHECK(res == expectedTable);

  auto res2 = engine.evaluate("DecodeTable"_(std::move(res)));

  CHECK(res2 == tableCopy);
}

TEST_CASE("Decode Table with f_ c_ col name", "[basics]") { // NOLINT
  dads::engines::DictionaryEncoder::Engine engine;
  auto col1 = dads::Span<int32_t>{std::vector<int32_t>{1, 2, 4, 3}};
  auto col2 =
      dads::Span<std::string>{std::vector<std::string>{"a", "b", "c", "d"}};
  auto col3 =
      dads::Span<std::string>{std::vector<std::string>{"d", "d", "a", "d"}};
  auto table = "Table"_("col1"_("List"_(std::move(col1))),
                        "c_col2"_("List"_(std::move(col2))),
                        "f_col3"_("List"_(std::move(col3))));
  
  auto colCopy1 = dads::Span<int32_t>{std::vector<int32_t>{1, 2, 4, 3}};
  auto colCopy2 =
      dads::Span<std::string>{std::vector<std::string>{"a", "b", "c", "d"}};
  auto colCopy3 =
      dads::Span<std::string>{std::vector<std::string>{"d", "d", "a", "d"}};
  auto tableCopy = "Table"_("col1"_("List"_(std::move(colCopy1))),
                        "c_col2"_("List"_(std::move(colCopy2))),
                        "f_col3"_("List"_(std::move(colCopy3))));

  auto expectedCol1 = dads::Span<int32_t>{std::vector<int32_t>{1, 2, 4, 3}};
  auto expectedCol2 = dads::Span<int32_t>{std::vector<int32_t>{0, 1, 2, 3}};
  auto expectedCol3 = dads::Span<int32_t>{std::vector<int32_t>{0, 0, 1, 0}};
  auto expectedTable = "Table"_("col1"_("List"_(std::move(expectedCol1))),
                                "c_col2"_("List"_(std::move(expectedCol2))),
                                "f_col3"_("List"_(std::move(expectedCol3))));

  auto res = engine.evaluate("EncodeTable"_(std::move(table)));

  CHECK(res == expectedTable);

  auto res2 = engine.evaluate("DecodeTable"_(std::move(res)));

  CHECK(res2 == tableCopy);
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
