#include <string_view>
#define CATCH_CONFIG_RUNNER
#include "../Source/BOSSWisentDeserialiserEngine.hpp"
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <array>
#include <catch2/catch.hpp>
#include <fstream>
#include <filesystem>
#include <numeric>
#include <typeinfo>
#include <variant>
using boss::Expression;
using std::string;
using std::literals::string_literals::
operator""s;                        // NOLINT(misc-unused-using-decls)
                                    // clang-tidy bug
using boss::utilities::operator""_; // NOLINT(misc-unused-using-decls)
                                    // clang-tidy bug
using Catch::Generators::random;
using Catch::Generators::take;
using Catch::Generators::values;
using std::vector;
using namespace Catch::Matchers;
using boss::expressions::CloneReason;
using boss::expressions::ComplexExpression;
using boss::expressions::generic::get;
using boss::expressions::generic::get_if;
using boss::expressions::generic::holds_alternative;
namespace boss {
using boss::expressions::atoms::Span;
};
using boss::serialization::Argument;
using boss::serialization::ArgumentType;
using boss::serialization::RootExpression;
using boss::serialization::SerializedExpression;
using std::int64_t;
using SerializationExpression = boss::serialization::Expression;

namespace {
std::vector<string>
    librariesToTest{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
}

std::vector<int8_t> createByteVector(boss::ComplexExpression &&e) {

  std::vector<int8_t> zeroedInt64_t = {0, 0, 0, 0, 0, 0, 0, 0};
  RootExpression *root = SerializedExpression(std::move(e)).extractRoot();
  size_t bufferSize = sizeof(Argument) * root->argumentCount +
                      sizeof(ArgumentType) * root->argumentCount +
                      sizeof(SerializationExpression) * root->expressionCount +
                      root->stringArgumentsFillIndex;

  std::vector<int8_t> bytes;
  bytes.insert(bytes.end(),
               reinterpret_cast<const int8_t *>(&(root->argumentCount)),
               reinterpret_cast<const int8_t *>(&(root->argumentCount)) +
                   sizeof(root->argumentCount));
  bytes.insert(bytes.end(),
               reinterpret_cast<const int8_t *>(&(root->expressionCount)),
               reinterpret_cast<const int8_t *>(&(root->expressionCount)) +
                   sizeof(root->expressionCount));
  bytes.insert(bytes.end(), zeroedInt64_t.begin(), zeroedInt64_t.end());
  bytes.insert(
      bytes.end(),
      reinterpret_cast<const int8_t *>(&(root->stringArgumentsFillIndex)),
      reinterpret_cast<const int8_t *>(&(root->stringArgumentsFillIndex)) +
          sizeof(root->stringArgumentsFillIndex));
  bytes.insert(
      bytes.end(), reinterpret_cast<const int8_t *>(&(root->arguments)),
      reinterpret_cast<const int8_t *>(&(root->arguments)) + bufferSize);

  std::free(root);

  return bytes;
}

bool writeBytesToFile(boss::ComplexExpression &&e, const std::string& filename) {
    std::vector<int8_t> zeroedInt64_t = {0, 0, 0, 0, 0, 0, 0, 0};
  RootExpression *root = SerializedExpression(std::move(e)).extractRoot();
  size_t bufferSize = sizeof(Argument) * root->argumentCount +
                      sizeof(ArgumentType) * root->argumentCount +
                      sizeof(SerializationExpression) * root->expressionCount +
                      root->stringArgumentsFillIndex;
    // if (!root) return false;  // Ensure root is valid

    std::ofstream fileStream(filename, std::ios::binary);
    if (!fileStream.is_open()) {
        std::free(root);
        return false;  // File couldn't be opened
    }

    // Write various fields to file
    fileStream.write(reinterpret_cast<const char*>(&root->argumentCount), sizeof(root->argumentCount));
    fileStream.write(reinterpret_cast<const char*>(&root->expressionCount), sizeof(root->expressionCount));
    fileStream.write(reinterpret_cast<const char*>(zeroedInt64_t.data()), zeroedInt64_t.size());
    fileStream.write(reinterpret_cast<const char*>(&root->stringArgumentsFillIndex), sizeof(root->stringArgumentsFillIndex));
    fileStream.write(reinterpret_cast<const char*>(&root->arguments), bufferSize);

    fileStream.close();
    std::free(root);

    return true;  // Return success
}

boss::Expression writeExpressionToWisentFile(boss::ComplexExpression &&e,
                                             const std::string &filePath) {

  auto bytes = createByteVector(std::move(e));
  std::cout << "GOT BYTES" << std::endl;
  std::ofstream file(filePath, std::ios::binary);
  if (!file) {
    throw std::runtime_error("Error opening file");
  }
  std::cout << "WRITING BYTES" << std::endl;
  file.write(reinterpret_cast<const char *>(bytes.data()),
             bytes.size() * sizeof(int8_t));
  file.close();
  std::cout << "WRITTEN BYTES" << std::endl;

  return "SuccessfulWriteTo"_(filePath);
}
// NOLINTBEGIN(readability-magic-numbers)
// NOLINTBEGIN(bugprone-exception-escape)
// NOLINTBEGIN(readability-function-cognitive-complexity)
TEST_CASE("Subspans work correctly", "[spans]") {
  auto input = boss::Span<int64_t>{std::vector<int64_t>{1, 2, 4, 3}};
  auto subrange = std::move(input).subspan(1, 3);
  CHECK(subrange.size() == 3);
  CHECK(subrange[0] == 2);
  CHECK(subrange[1] == 4);
  CHECK(subrange[2] == 3);
  auto subrange2 =
      boss::Span<int64_t>{std::vector<int64_t>{1, 2, 3, 2}}.subspan(2);
  CHECK(subrange2[0] == 3);
  CHECK(subrange2[1] == 2);
}

TEST_CASE("Basics", "[basics]") { // NOLINT

  auto const dataSetSize = 10;
  std::vector<int64_t> vec1(dataSetSize);
  std::vector<int64_t> vec2(dataSetSize);
  // std::vector<int64_t> vec2(dataSetSize);
  // std::vector<int64_t> vec3(dataSetSize);
  boss::engines::WisentDeserialiser::Engine engine;
  std::vector<int64_t> sfs = {1};

  for (auto sf : sfs) {
    std::iota(vec1.begin(), vec1.end(), 0);
    std::iota(vec2.begin(), vec2.end(), 0);
    // std::iota(vec1.begin(), vec1.end(), dataSetSize * sf);
    // std::iota(vec2.begin(), vec2.end(), dataSetSize * (sf + 1));
    // std::iota(vec3.begin(), vec3.end(), dataSetSize * (sf + 2));
    // std::vector<int8_t> bytes =
    // createByteVector(std::move("Table"_("value1"_("List"_(boss::Span<int64_t>(vector(vec1)))),
    // "value2"_("List"_(boss::Span<int64_t>(vector(vec2)))),
    // "value3"_("List"_(boss::Span<int64_t>(vector(vec3)))))));
    
    auto worked = writeBytesToFile(std::move(
					     "Table"_("value1"_("List"_(boss::Span<int64_t>(vector(vec1)), boss::Span<int64_t>(vector(vec2)))))), "/home/david/Documents/PhD/datasets/tpc_h_wisent_no_dict_enc/sf1000/test_small.bin");

    // std::ofstream file("overhead_table.bin", std::ios::binary);
    // if (!file) {
    //   throw std::runtime_error("Error opening file");
    // }
    // file.write(reinterpret_cast<const char*>(bytes.data()), bytes.size() *
    // 	       sizeof(int8_t)); file.close();
    // auto result = engine.evaluate("Parse"_(
    //     "ByteSequence"_(boss::Span<int8_t>(std::move(vector(bytes))))));
    // auto expected =
    //     "Table"_("value1"_("List"_(boss::Span<int64_t>(vector(vec1)))));
    // REQUIRE(result == expected);
  }
}

TEST_CASE("Large Table Gather", "[]") { // NOLINT

  boss::engines::WisentDeserialiser::Engine engine;

  std::vector<int64_t> expectedValues = {10000, 10002, 10004, 10006, 10008};

  std::vector<int64_t> indices = {0, 2, 4, 6, 8};
  boss::ExpressionArguments args;
  args.push_back("https://www.doc.ic.ac.uk/~dcl19/largeWisentTable.bin");
  args.push_back(
      "/home/david/Documents/PhD/symbol-store/BOSSRemoteBinaryLoaderEngine/"
      "releaseBuild/libBOSSRemoteBinaryLoaderEngine.so");
  args.push_back(
      "List"_("List"_(boss::Span<int64_t const>(std::move(vector(indices))))));
  args.push_back("List"_());
  auto gatherExpr = boss::ComplexExpression{"Gather"_, {}, std::move(args), {}};
  auto result = engine.evaluate(std::move(gatherExpr));
  auto expected =
      "Table"_("List"_(boss::Span<int64_t>(vector(expectedValues))));
  REQUIRE(result == expected);
}

TEST_CASE("Large Table Gather Ranges", "[]") { // NOLINT

  boss::engines::WisentDeserialiser::Engine engine;

  std::vector<int64_t> expectedValues = {10000, 10001, 10002, 10003, 10004,
                                         10010, 10011, 10012, 10013, 10014};

  std::vector<int64_t> starts = {0, 10};
  std::vector<int64_t> ends = {5, 15};
  boss::ExpressionArguments args;
  args.push_back("https://www.doc.ic.ac.uk/~dcl19/largeWisentTable.bin");
  args.push_back(
      "/home/david/Documents/PhD/symbol-store/BOSSRemoteBinaryLoaderEngine/"
      "releaseBuild/libBOSSRemoteBinaryLoaderEngine.so");
  args.push_back("starts"_);
  args.push_back("ends"_);
  args.push_back("Table"_(
      "starts"_("List"_(boss::Span<int64_t const>(std::move(vector(starts))))),
      "ends"_("List"_(boss::Span<int64_t const>(std::move(vector(ends)))))));
  args.push_back("List"_());
  auto gatherExpr =
      boss::ComplexExpression{"GatherRanges"_, {}, std::move(args), {}};
  auto result = engine.evaluate(std::move(gatherExpr));
  auto expected =
      "Table"_("List"_(boss::Span<int64_t>(vector(expectedValues))));
  REQUIRE(result == expected);
}

// TEST_CASE("Large Table Gather Selected Columns", "[]") { // NOLINT

//   boss::engines::WisentDeserialiser::Engine engine;

//   std::vector<int64_t> expectedValues1 = {10000, 10002, 10004, 10006, 10008};
//   std::vector<int64_t> expectedValues3 = {30000, 30002, 30004, 30006, 30008};

//   std::vector<int64_t> indices = {0, 2, 4, 6, 8};
//   boss::ExpressionArguments args;
//   args.push_back(
//       "https://www.doc.ic.ac.uk/~dcl19/largeMultiColWisentTable.bin");
//   args.push_back(
//       "/home/david/Documents/PhD/symbol-store/BOSSRemoteBinaryLoaderEngine/"
//       "releaseBuild/libBOSSRemoteBinaryLoaderEngine.so");
//   args.push_back(
//       "List"_("List"_(boss::Span<int64_t const>(std::move(vector(indices))))));
//   args.push_back("List"_("value1"_, "value3"_));
//   auto gatherExpr = boss::ComplexExpression{"Gather"_, {}, std::move(args), {}};
//   auto result = engine.evaluate(std::move(gatherExpr));
//   auto expected = "Table"_(
//       "value1"_("List"_(boss::Span<int64_t>(vector(expectedValues1)))),
//       "value3"_("List"_(boss::Span<int64_t>(vector(expectedValues3)))));
//   REQUIRE(result == expected);
// }

// TEST_CASE("Large Table Gather Ranges Selected Columns", "[]") { // NOLINT

//   boss::engines::WisentDeserialiser::Engine engine;

//   std::vector<int64_t> expectedValues1 = {10000, 10001, 10002, 10003, 10004,
//                                           10010, 10011, 10012, 10013, 10014};
//   std::vector<int64_t> expectedValues3 = {30000, 30001, 30002, 30003, 30004,
//                                           30010, 30011, 30012, 30013, 30014};

//   std::vector<int64_t> starts = {0, 10};
//   std::vector<int64_t> ends = {5, 15};
//   boss::ExpressionArguments args;
//   args.push_back(
//       "https://www.doc.ic.ac.uk/~dcl19/largeMultiColWisentTable.bin");
//   args.push_back(
//       "/home/david/Documents/PhD/symbol-store/BOSSRemoteBinaryLoaderEngine/"
//       "releaseBuild/libBOSSRemoteBinaryLoaderEngine.so");
//   args.push_back("starts"_);
//   args.push_back("ends"_);
//   args.push_back("Table"_(
//       "starts"_("List"_(boss::Span<int64_t const>(std::move(vector(starts))))),
//       "ends"_("List"_(boss::Span<int64_t const>(std::move(vector(ends)))))));
//   args.push_back("List"_("value1"_, "value3"_));
//   auto gatherExpr =
//       boss::ComplexExpression{"GatherRanges"_, {}, std::move(args), {}};
//   auto result = engine.evaluate(std::move(gatherExpr));
//   auto expected = "Table"_(
//       "value1"_("List"_(boss::Span<int64_t>(vector(expectedValues1)))),
//       "value3"_("List"_(boss::Span<int64_t>(vector(expectedValues3)))));
//   REQUIRE(result == expected);
// }

// TEST_CASE("Many Column Table Gather", "[]") { // NOLINT

//   boss::engines::WisentDeserialiser::Engine engine;

//   std::vector<int64_t> expectedValues = {10000, 10002, 10004, 10006, 10008};

//   std::vector<int64_t> indices = {0, 2, 4, 6, 8};
//   boss::ExpressionArguments args;
//   // boss::expressions::ExpressionSpanArguments spanArgs;
//   args.push_back("https://www.doc.ic.ac.uk/~dcl19/files_table2.bin");
//   args.push_back(
//       "/home/david/Documents/PhD/symbol-store/BOSSRemoteBinaryLoaderEngine/"
//       "releaseBuild/libBOSSRemoteBinaryLoaderEngine.so");
//   args.push_back("List"_("List"_()));
//   args.push_back("List"_("file_location"_));
//   // spanArgs.push_back(boss::Span<int64_t>(std::move(vector(indices))));
//   auto gatherExpr = boss::ComplexExpression{"Gather"_, {}, std::move(args),
//   {}}; auto result = engine.evaluate(std::move(gatherExpr)); std::cout <<
//   result << std::endl;
//   // auto expected =
//   //     "Table"_("List"_(boss::Span<int64_t>(vector(expectedValues))));
//   // REQUIRE(result == expected);
// }

// TEST_CASE("Parse Tables", "[]") { // NOLINT

//   boss::engines::WisentDeserialiser::Engine engine;
  
//   std::string SF_1_CUSTOMER_TABLE_URL = "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_supplier.bin";
//   std::vector<std::string> customerURLs = {SF_1_CUSTOMER_TABLE_URL};
//   auto eagerLoadCustomer =
//     "ParseTables"_("/home/david/Documents/PhD/symbol-store/BOSSRemoteBinaryLoaderEngine/releaseBuild/libBOSSRemoteBinaryLoaderEngine.so", "List"_("List"_(std::move(boss::Span<std::string const>(std::move(vector(customerURLs)))))));
//   auto result = engine.evaluate(std::move(eagerLoadCustomer));
//   std::cout << result << std::endl;
// }

// TEST_CASE("Parse Tables 3", "[]") { // NOLINT

//   boss::engines::WisentDeserialiser::Engine engine;
  
//   std::string SF_1000_ORDERS_TABLE_URL = "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_orders.bin";
//   std::vector<std::string> customerURLs = {SF_1000_ORDERS_TABLE_URL};
//   auto eagerLoadCustomer =
//     "ParseTables"_("/home/david/Documents/PhD/symbol-store/BOSSRemoteBinaryLoaderEngine/releaseBuild/libBOSSRemoteBinaryLoaderEngine.so", "List"_("List"_(std::move(boss::Span<std::string const>(std::move(vector(customerURLs)))))));
//   auto result = engine.evaluate(std::move(eagerLoadCustomer));
//   // auto newRes = writeExpressionToWisentFile(std::get<boss::ComplexExpression>(std::move(result)), "/home/david/Documents/PhD/datasets/tpc_h_wisent_no_dict_enc/tpch_20000MB_orders.bin");
//   // std::cout << result << std::endl;
// }

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
