#define CATCH_CONFIG_RUNNER

#include "../Source/DADS.hpp"
#include "../Source/BootstrapEngine.hpp"
#include "../Source/ExpressionUtilities.hpp"
#include <catch2/catch.hpp>
#include <numeric>
#include <variant>

#define USE_NEW_TABLE_FORMAT
#define VERBOSE_OUTPUT

using dads::Expression;
using std::string;
using std::literals::string_literals::operator""s;
using dads::utilities::operator""_;
using Catch::Generators::random;
using Catch::Generators::take;
using Catch::Generators::values;
using std::vector;
using namespace Catch::Matchers;
using dads::expressions::CloneReason;
using dads::expressions::generic::get;
using dads::expressions::generic::get_if;
using dads::expressions::generic::holds_alternative;
namespace dads {
using dads::expressions::atoms::Span;
};

using intType = std::int32_t;

static std::vector<string>
    librariesToTest{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto createSpansInt = [](auto... values) {
  using SpanArguments = dads::expressions::ExpressionSpanArguments;
  std::vector<intType> v1 = {values...};
  auto s1 = dads::Span<intType>(std::move(v1));
  SpanArguments args;
  args.emplace_back(std::move(s1));
  return dads::expressions::ComplexExpression("List"_, {}, {}, std::move(args));
};

auto createSpansFloat = [](auto... values) {
  using SpanArguments = dads::expressions::ExpressionSpanArguments;
  std::vector<double_t> v1 = {values...};
  auto s1 = dads::Span<double_t>(std::move(v1));
  SpanArguments args;
  args.emplace_back(std::move(s1));
  return dads::expressions::ComplexExpression("List"_, {}, {}, std::move(args));
};

// TEST_CASE("TPCH", "[basics]") { // NOLINT
//   auto engine = dads::engines::BootstrapEngine();
//   REQUIRE(!librariesToTest.empty());
//   auto eval = [&engine](dads::Expression&& expression) mutable {
//     return engine.evaluate("EvaluateInEngines"_("List"_(GENERATE(from_range(librariesToTest))),
//                                                 std::move(expression)));
//   };

// #ifdef USE_NEW_TABLE_FORMAT
//   auto lineitem = "Table"_(
//       "l_orderkey"_(createSpansInt(1, 1, 2, 3)),
//       "l_partkey"_(createSpansInt(1, 2, 3, 4)),
//       "l_suppkey"_(createSpansInt(1, 2, 3, 4)),
//       "l_returnflag"_(createSpansInt('N', 'N', 'A', 'A')),
//       "l_linestatus"_(createSpansInt('O', 'O', 'F', 'F')),
//       "l_quantity"_(createSpansInt(17, 21, 8, 5)),
//       "l_extendedprice"_(createSpansFloat(17954.55, 34850.16, 7712.48, 25284.00)),
//       "l_discount"_(createSpansFloat(0.10, 0.05, 0.06, 0.06)),
//       "l_tax"_(createSpansFloat(0.02, 0.06, 0.02, 0.06)),
//       "l_shipdate"_(createSpansInt(1992, 1994, 1996, 1994)));
// #else
//   auto lineitem = "Table"_(
//       "Column"_("l_orderkey"_, createSpansInt(1, 2, 3, 4)),
//       "Column"_("l_partkey"_, createSpansInt(1, 2, 3, 4)),
//       "Column"_("l_suppkey"_, createSpansInt(1, 2, 3, 4)),
//       "Column"_("l_quantity"_, createSpansInt(17, 21, 8, 5)),
//       "Column"_("l_extendedprice"_, createSpansFloat(17954.55, 34850.16, 7712.48, 25284.00)),
//       "Column"_("l_discount"_, createSpansFloat(0.10, 0.05, 0.06, 0.06)),
//       "Column"_("l_tax"_, createSpansFloat(0.02, 0.06, 0.02, 0.06)),
//       "Column"_("l_returnflag"_, createSpansInt('N', 'N', 'A', 'A')),
//       "Column"_("l_linestatus"_, createSpansInt('O', 'O', 'F', 'F')),
//       "Column"_("l_shipdate"_, createSpansInt(1992, 1994, 1996, 1994)));
// #endif

//   SECTION("q6") {
//     auto const& result = eval("Group"_(
//         "Project"_(
//             "Select"_(
//                 "Project"_(std::move(lineitem), "As"_("l_quantity"_, "l_quantity"_, "l_discount"_,
//                                                       "l_discount"_, "l_shipdate"_, "l_shipdate"_,
//                                                       "l_extendedprice"_, "l_extendedprice"_)),
//                 "Where"_("And"_("Greater"_(24, "l_quantity"_), "Greater"_("l_discount"_, 0.0499),
//                                 "Greater"_(0.07001, "l_discount"_), "Greater"_(1995, "l_shipdate"_),
//                                 "Greater"_("l_shipdate"_, 1993)))),
//             "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))),
//         "Sum"_("revenue"_)));

//     CHECK(result == "List"_("List"_(34850.16 * 0.05 + 25284.00 * 0.06))); // NOLINT
//   }
// }

// TEST_CASE("SELECT", "[basics]") { // NOLINT
//   auto engine = dads::engines::BootstrapEngine();
//   REQUIRE(!librariesToTest.empty());
//   auto eval = [&engine](dads::Expression&& expression) mutable {
//     return engine.evaluate("EvaluateInEngines"_("List"_(GENERATE(from_range(librariesToTest))),
//                                                 std::move(expression)));
//   };

// #ifdef USE_NEW_TABLE_FORMAT
//   auto table1 = "Table"_(
//       "key"_(createSpansInt(1, 2, 3)),
//       "payload"_(createSpansInt(4, 5, 6)));
// #else
//   auto table1 = "Table"_(
//       "Column"_("key"_, createSpansInt(1, 2, 3)),
//       "Column"_("payload"_, createSpansInt(4, 5, 6)));
// #endif

//   SECTION("Simple_select") {
//     auto const& result = eval("Select"_(
//         "Project"_(std::move(table1), "As"_("key"_, "key"_, "payload"_, "payload"_)),
//                                         "Where"_("Greater"_("key"_, 2))));

//     CHECK(result == "List"_("List"_(3,6))); // NOLINT
//   }

// #ifdef USE_NEW_TABLE_FORMAT
//   auto table2 = "Table"_(
//       "key"_(createSpansInt(1, 2, 3)),
//       "payload"_(createSpansInt(4, 5, 6)));
// #else
//   auto table2 = "Table"_(
//       "Column"_("key"_, createSpansInt(1, 2, 3)),
//       "Column"_("payload"_, createSpansInt(4, 5, 6)));
// #endif

//   SECTION("Simple_select") {
//     auto const& result = eval("Select"_(
//         "Project"_(std::move(table2), "As"_("key"_, "key"_, "payload"_, "payload"_)),
//         "Where"_("Greater"_(2, "key"_))));

//     CHECK(result == "List"_("List"_(1, 4))); // NOLINT
//   }
// }

// TEST_CASE("Gather", "[basics]") { // NOLINT
//   auto engine = dads::engines::BootstrapEngine();
//   REQUIRE(!librariesToTest.empty());
//   auto eval = [&engine](dads::Expression&& expression) mutable {
//     return engine.evaluate("EvaluateInEngines"_("List"_(GENERATE(from_range(librariesToTest))),
//                                                 std::move(expression)));
//   };

// #ifdef USE_NEW_TABLE_FORMAT
//   auto table1 = "Table"_(
//       "key"_(createSpansInt(1, 2, 3, 4)),
//       "payload"_(createSpansInt(5, 6, 7, 8)));
// #else
//   auto table1 = "Table"_(
//       "Column"_("key"_, createSpansInt(1, 2, 3, 4)),
//       "Column"_("payload"_, createSpansInt(5, 6, 7, 8)));
// #endif

//   SECTION("Simple_gather") {
//     std::vector<int32_t> indexes = {0,2,3};
//     dads::expressions::ExpressionSpanArguments args;
//     args.emplace_back(dads::Span<int32_t>(std::move(std::vector(indexes))));
//     auto projectExpression = dads::expressions::ComplexExpression("Project"_(std::move(table1),
//                                                                              "As"_("key"_, "key"_,
//                                                                                    "payload"_, "payload"_)));
//     dads::expressions::ExpressionArguments subExpressions;
//     subExpressions.emplace_back(std::move(projectExpression));
//     auto gatherExpression =
//         dads::expressions::ComplexExpression("Gather"_, {}, std::move(subExpressions), std::move(args));

//     auto const& result = eval(std::move(gatherExpression));

//     CHECK(result == "List"_("List"_(1, 3, 4, 5, 7, 8))); // NOLINT
//   }
// }

TEST_CASE("INDEXSELECT", "[basics]") { // NOLINT
  auto engine = dads::engines::BootstrapEngine();
  REQUIRE(!librariesToTest.empty());
  auto eval = [&engine](dads::Expression&& expression) mutable {
    return engine.evaluate("EvaluateInEngines"_("List"_(GENERATE(from_range(librariesToTest))),
                                                std::move(expression)));
  };

#ifdef USE_NEW_TABLE_FORMAT
  auto table1 = "Table"_(
			 "key"_(createSpansInt(1, 2, 3, 7, 0, 10)),
			 "payload"_(createSpansInt(5, 10, 6, 8, 3, 2)));
#else
  auto table1 = "Table"_(
      "Column"_("key"_, createSpansInt(1, 2, 3, 4)),
      "Column"_("payload"_, createSpansInt(5, 6, 7, 8)));
#endif
  SECTION("Simple_select") {
    auto const& result = eval("Select"_(
					"Project"_("AddIndices"_(std::move(table1), "__internal_indices_"_), "As"_(
          "key"_, "key"_, 
          "payload"_, "payload"_,
          "index"_, "__internal_indices_"_
          )
		   ),"Where"_("Greater"_("key"_, 2))));


    auto expected =
      "Table"_(
	       "key"_(createSpansInt(3,7,10)),
	       "payload"_(createSpansInt(6,8,2)),
	       "index"_(createSpansInt(2,3,5))
	       );

    
    CHECK(result == expected); // NOLINT
  }
}

int main(int argc, char* argv[]) {
  Catch::Session session;
#ifdef VERBOSE_OUTPUT
  session.configData().showSuccessfulTests = true;
#endif
  session.cli(session.cli() | Catch::clara::Opt(librariesToTest, "library")["--library"]);
  int returnCode = session.applyCommandLine(argc, argv);
  if(returnCode != 0) {
    return returnCode;
  }
  return session.run();
}
