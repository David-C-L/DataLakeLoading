#include "config.hpp"
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <iostream>
#include "utilities.hpp"

#pragma once

namespace boss::benchmarks::LazyLoading::TPCHQueries {

using namespace std;
using namespace boss;
using utilities::operator""_; // NOLINT(misc-unused-using-decls) clang-tidy bug
using boss::benchmarks::LazyLoading::utilities::wrapEval;
using boss::benchmarks::LazyLoading::config::paths::RBL_PATH;

enum TPCH_SF {
  TPCH_SF_1 = 0,
  TPCH_SF_10 = 1,
  TPCH_SF_100 = 2,
  TPCH_SF_1000 = 3,
  TPCH_SF_10000 = 4,
  TPCH_SF_20000 = 5
};

enum TPCH_TABLE {
  TPCH_CUSTOMER = 0,
  TPCH_LINEITEM = 1,
  TPCH_NATION = 2,
  TPCH_ORDERS = 3,
  TPCH_PART = 4,
  TPCH_PARTSUPP = 5,
  TPCH_REGION = 6,
  TPCH_SUPPLIER = 7
};

std::string SF_1_CUSTOMER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_customer.bin";
std::string SF_1_LINEITEM_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_lineitem.bin";
std::string SF_1_NATION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_nation.bin";
std::string SF_1_ORDERS_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_orders.bin";
std::string SF_1_PART_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_part.bin";
std::string SF_1_PARTSUPP_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_partsupp.bin";
std::string SF_1_REGION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_region.bin";
std::string SF_1_SUPPLIER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_supplier.bin";

std::string SF_10_CUSTOMER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_customer.bin";
std::string SF_10_LINEITEM_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_lineitem.bin";
std::string SF_10_NATION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_nation.bin";
std::string SF_10_ORDERS_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_orders.bin";
std::string SF_10_PART_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_part.bin";
std::string SF_10_PARTSUPP_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_partsupp.bin";
std::string SF_10_REGION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_region.bin";
std::string SF_10_SUPPLIER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_supplier.bin";

std::string SF_100_CUSTOMER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_customer.bin";
std::string SF_100_LINEITEM_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_lineitem.bin";
std::string SF_100_NATION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_nation.bin";
std::string SF_100_ORDERS_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_orders.bin";
std::string SF_100_PART_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_part.bin";
std::string SF_100_PARTSUPP_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_partsupp.bin";
std::string SF_100_REGION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_region.bin";
std::string SF_100_SUPPLIER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_supplier.bin";

std::string SF_1000_CUSTOMER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_customer.bin";
std::string SF_1000_LINEITEM_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_lineitem.bin";
std::string SF_1000_NATION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_nation.bin";
std::string SF_1000_ORDERS_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_orders.bin";
std::string SF_1000_PART_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_part.bin";
std::string SF_1000_PARTSUPP_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_partsupp.bin";
std::string SF_1000_REGION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_region.bin";
std::string SF_1000_SUPPLIER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_supplier.bin";

std::string SF_10000_CUSTOMER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_customer.bin";
std::string SF_10000_LINEITEM_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_lineitem.bin";
std::string SF_10000_NATION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_nation.bin";
std::string SF_10000_ORDERS_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_orders.bin";
std::string SF_10000_PART_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_part.bin";
std::string SF_10000_PARTSUPP_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_partsupp.bin";
std::string SF_10000_REGION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_region.bin";
std::string SF_10000_SUPPLIER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_supplier.bin";

std::string SF_20000_CUSTOMER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_customer.bin";
std::string SF_20000_LINEITEM_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_lineitem.bin";
std::string SF_20000_NATION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_nation.bin";
std::string SF_20000_ORDERS_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_orders.bin";
std::string SF_20000_PART_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_part.bin";
std::string SF_20000_PARTSUPP_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_partsupp.bin";
std::string SF_20000_REGION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_region.bin";
std::string SF_20000_SUPPLIER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_supplier.bin";

std::unordered_map<TPCH_TABLE, std::unordered_map<TPCH_SF, std::string>>
    tableURLs{{TPCH_CUSTOMER,
               {
                   {TPCH_SF_1, SF_1_CUSTOMER_TABLE_URL},
                   {TPCH_SF_10, SF_10_CUSTOMER_TABLE_URL},
                   {TPCH_SF_100, SF_100_CUSTOMER_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_CUSTOMER_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_CUSTOMER_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_CUSTOMER_TABLE_URL},
               }},
              {TPCH_LINEITEM,
               {
                   {TPCH_SF_1, SF_1_LINEITEM_TABLE_URL},
                   {TPCH_SF_10, SF_10_LINEITEM_TABLE_URL},
                   {TPCH_SF_100, SF_100_LINEITEM_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_LINEITEM_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_LINEITEM_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_LINEITEM_TABLE_URL},
               }},
              {TPCH_NATION,
               {
                   {TPCH_SF_1, SF_1_NATION_TABLE_URL},
                   {TPCH_SF_10, SF_10_NATION_TABLE_URL},
                   {TPCH_SF_100, SF_100_NATION_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_NATION_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_NATION_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_NATION_TABLE_URL},
               }},
              {TPCH_ORDERS,
               {
                   {TPCH_SF_1, SF_1_ORDERS_TABLE_URL},
                   {TPCH_SF_10, SF_10_ORDERS_TABLE_URL},
                   {TPCH_SF_100, SF_100_ORDERS_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_ORDERS_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_ORDERS_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_ORDERS_TABLE_URL},
               }},
              {TPCH_PART,
               {
                   {TPCH_SF_1, SF_1_PART_TABLE_URL},
                   {TPCH_SF_10, SF_10_PART_TABLE_URL},
                   {TPCH_SF_100, SF_100_PART_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_PART_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_PART_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_PART_TABLE_URL},
               }},
              {TPCH_PARTSUPP,
               {
                   {TPCH_SF_1, SF_1_PARTSUPP_TABLE_URL},
                   {TPCH_SF_10, SF_10_PARTSUPP_TABLE_URL},
                   {TPCH_SF_100, SF_100_PARTSUPP_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_PARTSUPP_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_PARTSUPP_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_PARTSUPP_TABLE_URL},
               }},
              {TPCH_REGION,
               {
                   {TPCH_SF_1, SF_1_REGION_TABLE_URL},
                   {TPCH_SF_10, SF_10_REGION_TABLE_URL},
                   {TPCH_SF_100, SF_100_REGION_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_REGION_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_REGION_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_REGION_TABLE_URL},
               }},
              {TPCH_SUPPLIER,
               {
                   {TPCH_SF_1, SF_1_SUPPLIER_TABLE_URL},
                   {TPCH_SF_10, SF_10_SUPPLIER_TABLE_URL},
                   {TPCH_SF_100, SF_100_SUPPLIER_TABLE_URL},
                   {TPCH_SF_1000, SF_1000_SUPPLIER_TABLE_URL},
                   {TPCH_SF_10000, SF_10000_SUPPLIER_TABLE_URL},
                   {TPCH_SF_20000, SF_20000_SUPPLIER_TABLE_URL},
               }}};

inline boss::Expression TPCH_Q1_DV(TPCH_SF sf) {
  std::vector<std::string> lineitemURLs = {tableURLs[TPCH_LINEITEM][sf]};

  auto eagerLoadLineitem = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(lineitemURLs)))))));
  auto encodedLineitem = "EncodeTable"_(std::move(eagerLoadLineitem));

  auto select = 
    "Select"_(
	      "Project"_(std::move(encodedLineitem),
			 "As"_("l_quantity"_, "l_quantity"_,
			       "l_discount"_, "l_discount"_,
			       "l_shipdate"_, "l_shipdate"_,
			       "l_extendedprice"_,
			       "l_extendedprice"_,
			       "l_returnflag"_, "l_returnflag"_,
			       "l_linestatus"_, "l_linestatus"_,
			       "l_tax"_, "l_tax"_)),
	      "Where"_("Greater"_("DateObject"_("1998-08-31"),
				  "l_shipdate"_)));

  auto query = "Order"_(
      "Project"_(
          "Group"_(
              "Project"_(
                  "Project"_(
			     "Project"_(std::move(select),
                          "As"_("l_returnflag"_, "l_returnflag"_,
                                "l_linestatus"_, "l_linestatus"_, "l_quantity"_,
                                "l_quantity"_, "l_extendedprice"_,
                                "l_extendedprice"_, "l_discount"_,
                                "l_discount"_, "calc1"_,
                                "Minus"_(1.0, "l_discount"_), "calc2"_,
                                "Plus"_("l_tax"_, 1.0))),
                      "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                            "l_linestatus"_, "l_quantity"_, "l_quantity"_,
                            "l_extendedprice"_, "l_extendedprice"_,
                            "l_discount"_, "l_discount"_, "disc_price"_,
                            "Times"_("l_extendedprice"_, "calc1"_), "calc2"_,
                            "calc2"_)),
                  "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                        "l_linestatus"_, "l_quantity"_, "l_quantity"_,
                        "l_extendedprice"_, "l_extendedprice"_, "l_discount"_,
                        "l_discount"_, "disc_price"_, "disc_price"_, "calc"_,
                        "Times"_("disc_price"_, "calc2"_))),
              "By"_("l_returnflag"_, "l_linestatus"_),
              "As"_("sum_qty"_, "Sum"_("l_quantity"_), "sum_base_price"_,
                    "Sum"_("l_extendedprice"_), "sum_disc_price"_,
                    "Sum"_("disc_price"_), "sum_charges"_, "Sum"_("calc"_),
                    "sum_disc"_, "Sum"_("l_discount"_), "count_order"_,
                    "Count"_("*"_))),
          "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                "l_linestatus"_, "sum_qty"_, "sum_qty"_, "sum_base_price"_,
                "sum_base_price"_, "sum_disc_price"_, "sum_disc_price"_,
                "sum_charges"_, "sum_charges"_, "avg_qty"_,
                "Divide"_("sum_qty"_, "count_order"_), "avg_price"_,
                "Divide"_("sum_base_price"_, "count_order"_), "avg_disc"_,
                "Divide"_("sum_disc"_, "count_order"_), "count_order"_,
                "count_order"_)),
      "By"_("l_returnflag"_, "l_linestatus"_));
  
  auto decodedResult = "DecodeTable"_(std::move(query));

  return std::move(decodedResult);
}

inline boss::Expression TPCH_Q3_DV(TPCH_SF sf) {
  std::vector<std::string> customerURLs = {tableURLs[TPCH_CUSTOMER][sf]};
  std::vector<std::string> ordersURLs = {tableURLs[TPCH_ORDERS][sf]};
  std::vector<std::string> lineitemURLs = {tableURLs[TPCH_LINEITEM][sf]};

  auto eagerLoadCustomer = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(customerURLs)))))));
  auto eagerLoadOrders = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(ordersURLs)))))));
  auto eagerLoadLineitem = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(lineitemURLs)))))));
  
  auto encodedCustomer = "EncodeTable"_(std::move(eagerLoadCustomer));
  auto encodedOrders = "EncodeTable"_(std::move(eagerLoadOrders));
  auto encodedLineitem = "EncodeTable"_(std::move(eagerLoadLineitem));

  auto query = "Top"_(
      "Group"_(
          "Project"_(
              "Join"_(
                  "Project"_(
                      "Join"_(
                          "Select"_(
                              "Project"_(std::move(encodedOrders),
                                         "As"_("o_orderkey"_, "o_orderkey"_,
                                               "o_orderdate"_, "o_orderdate"_,
                                               "o_custkey"_, "o_custkey"_,
                                               "o_shippriority"_,
                                               "o_shippriority"_)),
                              "Where"_("Greater"_("DateObject"_("1995-03-15"),
                                                  "o_orderdate"_))),
                          "Project"_(
                              "Select"_(
                                  "Project"_(std::move(encodedCustomer),
                                             "As"_("c_custkey"_, "c_custkey"_,
                                                   "c_mktsegment"_,
                                                   "c_mktsegment"_)),
                                  "Where"_("Equal"_("c_mktsegment"_,
                                                              "GetEncodingFor"_("BUILDING", "c_mktsegment"_)))),
                              "As"_("c_custkey"_, "c_custkey"_, "c_mktsegment"_,
                                    "c_mktsegment"_)),
                          "Where"_("Equal"_("o_custkey"_, "c_custkey"_))),
                      "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                            "o_orderdate"_, "o_custkey"_, "o_custkey"_,
                            "o_shippriority"_, "o_shippriority"_)),
                  "Project"_(
                      "Select"_(
                          "Project"_(
                              std::move(encodedLineitem),
                              "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_,
                                    "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                    "l_extendedprice"_, "l_extendedprice"_)),
                          "Where"_("Greater"_("l_shipdate"_,
                                              "DateObject"_("1993-03-15")))),
                      "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_,
                            "l_discount"_, "l_extendedprice"_,
                            "l_extendedprice"_)),
                  "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
              "As"_("expr1009"_,
                    "Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
                    "l_extendedprice"_, "l_extendedprice"_, "l_orderkey"_,
                    "l_orderkey"_, "o_orderdate"_, "o_orderdate"_,
                    "o_shippriority"_, "o_shippriority"_)),
          "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
          "As"_("revenue"_, "Sum"_("expr1009"_))),
      "By"_("revenue"_, "desc"_, "o_orderdate"_), 10);

  auto decodedResult = "DecodeTable"_(std::move(query));

  return std::move(decodedResult);
}

inline boss::Expression TPCH_Q6_DV(TPCH_SF sf) {
  std::vector<std::string> lineitemURLs = {tableURLs[TPCH_LINEITEM][sf]};

  auto eagerLoadLineitem = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(lineitemURLs)))))));

  auto encodedLineitem = "EncodeTable"_(std::move(eagerLoadLineitem));

  auto query = "Group"_(
      "Project"_(
          "Select"_(
              "Project"_(std::move(encodedLineitem),
                         "As"_("l_quantity"_, "l_quantity"_, "l_discount"_,
                               "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                               "l_extendedprice"_, "l_extendedprice"_)),
              "Where"_("And"_(
                  "Greater"_(24, "l_quantity"_),      // NOLINT
                  "Greater"_("l_discount"_, 0.0499),  // NOLINT
                  "Greater"_(0.07001, "l_discount"_), // NOLINT
                  "Greater"_("DateObject"_("1995-01-01"), "l_shipdate"_),
                  "Greater"_("l_shipdate"_, "DateObject"_("1993-12-31"))))),
          "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))),
      "Sum"_("revenue"_));

  auto decodedResult = "DecodeTable"_(std::move(query));

  return std::move(decodedResult);
}

inline boss::Expression TPCH_Q9_DV(TPCH_SF sf) {
  std::vector<std::string> partURLs = {tableURLs[TPCH_PART][sf]};
  std::vector<std::string> ordersURLs = {tableURLs[TPCH_ORDERS][sf]};
  std::vector<std::string> nationURLs = {tableURLs[TPCH_NATION][sf]};
  std::vector<std::string> supplierURLs = {tableURLs[TPCH_SUPPLIER][sf]};
  std::vector<std::string> partsuppURLs = {tableURLs[TPCH_PARTSUPP][sf]};
  std::vector<std::string> lineitemURLs = {tableURLs[TPCH_LINEITEM][sf]};

  auto eagerLoadPart = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(partURLs)))))));
  auto eagerLoadOrders = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(ordersURLs)))))));
  auto eagerLoadNation = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(nationURLs)))))));
  auto eagerLoadSupplier = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(supplierURLs)))))));
  auto eagerLoadPartsupp = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(partsuppURLs)))))));
  auto eagerLoadLineitem = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(lineitemURLs)))))));

  
  auto encodedPart = "EncodeTable"_(std::move(eagerLoadPart));
  auto encodedOrders = "EncodeTable"_(std::move(eagerLoadOrders));
  auto encodedNation = "EncodeTable"_(std::move(eagerLoadNation));
  auto encodedSupplier = "EncodeTable"_(std::move(eagerLoadSupplier));
  auto encodedPartsupp = "EncodeTable"_(std::move(eagerLoadPartsupp));
  auto encodedLineitem = "EncodeTable"_(std::move(eagerLoadLineitem));

  auto query = "Order"_(
      "Group"_(
          "Project"_(
              "Join"_(
                  "Project"_(std::move(encodedOrders),
                             "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                   "o_orderdate"_)),
                  "Project"_(
                      "Join"_(
                          "Project"_(
                              "Join"_(
                                  "Project"_(
                                      "Select"_(
                                          "Project"_(std::move(encodedPart),
                                                     "As"_("p_partkey"_,
                                                           "p_partkey"_,
                                                           "p_retailprice"_,
                                                           "p_retailprice"_)),
                                          "Where"_("And"_(
                                              "Greater"_("p_retailprice"_,
                                                         1006.05), // NOLINT
                                              "Greater"_(1080.1,   // NOLINT
                                                         "p_retailprice"_)))),
                                      "As"_("p_partkey"_, "p_partkey"_,
                                            "p_retailprice"_,
                                            "p_retailprice"_)),
                                  "Project"_(
                                      "Join"_(
                                          "Project"_(
                                              "Join"_(
                                                  "Project"_(
                                                      std::move(
                                                          encodedNation),
                                                      "As"_("n_name"_,
                                                            "n_name"_,
                                                            "n_nationkey"_,
                                                            "n_nationkey"_)),
                                                  "Project"_(
                                                      std::move(
                                                          encodedSupplier),
                                                      "As"_("s_suppkey"_,
                                                            "s_suppkey"_,
                                                            "s_nationkey"_,
                                                            "s_nationkey"_)),
                                                  "Where"_("Equal"_(
                                                      "n_nationkey"_,
                                                      "s_nationkey"_))),
                                              "As"_("n_name"_, "n_name"_,
                                                    "s_suppkey"_,
                                                    "s_suppkey"_)),
                                          "Project"_(
                                              std::move(encodedPartsupp),
                                              "As"_(
                                                  "ps_partkey"_, "ps_partkey"_,
                                                  "ps_suppkey"_, "ps_suppkey"_,
                                                  "ps_supplycost"_,
                                                  "ps_supplycost"_)),
                                          "Where"_("Equal"_("s_suppkey"_,
                                                            "ps_suppkey"_))),
                                      "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                            "ps_partkey"_, "ps_suppkey"_,
                                            "ps_suppkey"_, "ps_supplycost"_,
                                            "ps_supplycost"_)),
                                  "Where"_(
                                      "Equal"_("p_partkey"_, "ps_partkey"_))),
                              "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                    "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_,
                                    "ps_supplycost"_, "ps_supplycost"_)),
                          "Project"_(
                              std::move(encodedLineitem),
                              "As"_("l_partkey"_, "l_partkey"_, "l_suppkey"_,
                                    "l_suppkey"_, "l_orderkey"_, "l_orderkey"_,
                                    "l_extendedprice"_, "l_extendedprice"_,
                                    "l_discount"_, "l_discount"_, "l_quantity"_,
                                    "l_quantity"_)),
                          "Where"_(
                              "Equal"_("List"_("ps_partkey"_, "ps_suppkey"_),
                                       "List"_("l_partkey"_, "l_suppkey"_)))),
                      "As"_("n_name"_, "n_name"_, "ps_supplycost"_,
                            "ps_supplycost"_, "l_orderkey"_, "l_orderkey"_,
                            "l_extendedprice"_, "l_extendedprice"_,
                            "l_discount"_, "l_discount"_, "l_quantity"_,
                            "l_quantity"_)),
                  "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
              "As"_("nation"_, "n_name"_, "o_year"_, "Year"_("o_orderdate"_),
                    "amount"_,
                    "Minus"_("Times"_("l_extendedprice"_,
                                      "Minus"_(1.0, "l_discount"_)),
                             "Times"_("ps_supplycost"_, "l_quantity"_)))),
          "By"_("nation"_, "o_year"_), "Sum"_("amount"_)),
      "By"_("nation"_, "o_year"_, "desc"_));

  auto decodedResult = "DecodeTable"_(std::move(query));

  return std::move(decodedResult);
}

inline boss::Expression TPCH_Q18_DV(TPCH_SF sf) {
  std::vector<std::string> customerURLs = {tableURLs[TPCH_CUSTOMER][sf]};
  std::vector<std::string> ordersURLs = {tableURLs[TPCH_ORDERS][sf]};
  std::vector<std::string> lineitemURLs = {tableURLs[TPCH_LINEITEM][sf]};

  auto eagerLoadCustomer = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(customerURLs)))))));
  auto eagerLoadOrders = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(ordersURLs)))))));
  auto eagerLoadLineitem = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(boss::Span<std::string const>(
                    std::move(vector(lineitemURLs)))))));

  
  auto encodedCustomer = "EncodeTable"_(std::move(eagerLoadCustomer));
  auto encodedOrders = "EncodeTable"_(std::move(eagerLoadOrders));
  auto encodedLineitem = "EncodeTable"_(std::move(eagerLoadLineitem));

  auto query = "Top"_(
      "Group"_(
          "Project"_(
              "Join"_(
                  "Select"_(
                      "Group"_("Project"_(std::move(encodedLineitem),
                                          "As"_("l_orderkey"_, "l_orderkey"_,
                                                "l_quantity"_, "l_quantity"_)),
                               "By"_("l_orderkey"_),
                               "As"_("sum_l_quantity"_, "Sum"_("l_quantity"_))),
                      "Where"_("Greater"_("sum_l_quantity"_, 300))), // NOLINT
                  "Project"_(
                      "Join"_(
                          "Project"_(std::move(encodedCustomer),
                                     "As"_("c_custkey"_, "c_custkey"_)),
                          "Project"_(std::move(encodedOrders),
                                     "As"_("o_orderkey"_, "o_orderkey"_,
                                           "o_custkey"_, "o_custkey"_,
                                           "o_orderdate"_, "o_orderdate"_,
                                           "o_totalprice"_, "o_totalprice"_)),
                          "Where"_("Equal"_("c_custkey"_, "o_custkey"_))),
                      "As"_("o_orderkey"_, "o_orderkey"_, "o_custkey"_,
                            "o_custkey"_, "o_orderdate"_, "o_orderdate"_,
                            "o_totalprice"_, "o_totalprice"_)),
                  "Where"_("Equal"_("l_orderkey"_, "o_orderkey"_))),
              "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                    "o_orderdate"_, "o_totalprice"_, "o_totalprice"_,
                    "o_custkey"_, "o_custkey"_, "sum_l_quantity"_,
                    "sum_l_quantity"_)),
          "By"_("o_custkey"_, "o_orderkey"_, "o_orderdate"_, "o_totalprice"_),
          "Sum"_("sum_l_quantity"_)),
      "By"_("o_totalprice"_, "desc"_, "o_orderdate"_), 100);

  auto decodedResult = "DecodeTable"_(std::move(query));

  return std::move(decodedResult);
}

inline boss::Expression createSingleList(std::vector<boss::Symbol> symbols) {
  boss::ExpressionArguments args;
  for (boss::Symbol symbol : symbols) {
    args.push_back(symbol);
  }
  auto list = boss::ComplexExpression{"List"_, {}, std::move(args), {}};
  return std::move(list);
}

inline boss::Expression
createIndicesAsNoRename(std::vector<boss::Symbol> symbols) {
  boss::ExpressionArguments args;
  args.push_back("__internal_indices_"_);
  args.push_back("__internal_indices_"_);
  for (boss::Symbol symbol : symbols) {
    args.push_back(symbol);
    args.push_back(symbol);
  }
  auto as = boss::ComplexExpression{"As"_, {}, std::move(args), {}};
  return std::move(as);
}

inline boss::Expression
getGatherSelectGather(std::string url, boss::Expression &&gatherIndices1,
                      std::vector<boss::Symbol> gatherColumns1,
                      boss::Expression &&where,
                      std::vector<boss::Symbol> gatherColumns2, bool encode1, bool encode2) {
  auto const gather1 = "Gather"_(url, RBL_PATH, std::move(gatherIndices1),
                           std::move(createSingleList(gatherColumns1)));
  auto encoded1 = std::move(gather1.clone(expressions::CloneReason::FOR_TESTING));
  if (encode1) {
    encoded1 = "EncodeTable"_(std::move(gather1.clone(expressions::CloneReason::FOR_TESTING)));
  }
  auto project = "Project"_("AddIndices"_(std::move(encoded1), "__internal_indices_"_),
                            std::move(createIndicesAsNoRename(gatherColumns1)));
  auto indices = "Project"_("Select"_(std::move(project), std::move(where)),
                            std::move(createIndicesAsNoRename({})));
  auto gather2 = "Gather"_(url, RBL_PATH, std::move(indices),
                           std::move(createSingleList(gatherColumns2)));
  if (encode2) {
    auto encoded2 = "EncodeTable"_(std::move(gather2));
    return std::move(encoded2);
  }

  return std::move(gather2);
}
  
inline boss::Expression
getSelectGather(std::string url, boss::Expression &&table,
                      std::vector<boss::Symbol> tableColumns,
                      boss::Expression &&where,
                      std::vector<boss::Symbol> gatherColumns2, bool encode1, bool encode2) {
  auto project = "Project"_(std::move(table),
                            std::move(createIndicesAsNoRename(tableColumns)));
  auto indices = "Project"_("Select"_(std::move(project), std::move(where)),
                            std::move(createIndicesAsNoRename({})));
  auto gather2 = "Gather"_(url, RBL_PATH, std::move(indices),
                           std::move(createSingleList(gatherColumns2)));
  if (encode2) {
    auto encoded2 = "EncodeTable"_(std::move(gather2));
    return std::move(encoded2);
  }

  return std::move(gather2);
}

inline boss::Expression
getGatherSelectGatherWrap(std::string url, boss::Expression &&gatherIndices1,
                      std::vector<boss::Symbol> gatherColumns1,
                      boss::Expression &&where,
			  std::vector<boss::Symbol> gatherColumns2, bool encode1, bool encode2, int32_t s1, int32_t s2) {
  auto const gather1 = wrapEval("Gather"_(url, RBL_PATH, std::move(gatherIndices1),
					  std::move(createSingleList(gatherColumns1))), s1);
  auto encoded1 = std::move(gather1.clone(expressions::CloneReason::FOR_TESTING));
  if (encode1) {
    encoded1 = wrapEval("EncodeTable"_(std::move(gather1.clone(expressions::CloneReason::FOR_TESTING))), s1);
  }
  auto project = wrapEval("Project"_("AddIndices"_(std::move(encoded1), "__internal_indices_"_),
				     std::move(createIndicesAsNoRename(gatherColumns1))), s1);
  auto indices = wrapEval("Project"_(wrapEval("Select"_(std::move(project), std::move(where)), s1),
				     std::move(createIndicesAsNoRename({}))), s1);
  auto gather2 = wrapEval("Gather"_(url, RBL_PATH, std::move(indices),
				    std::move(createSingleList(gatherColumns2))), s2);
  if (encode2) {
    auto encoded2 = wrapEval("EncodeTable"_(std::move(gather2)), s2);
    return std::move(encoded2);
  }

  return std::move(gather2);
}
  
inline boss::Expression
getSelectGatherWrap(std::string url, boss::Expression &&table,
                      std::vector<boss::Symbol> tableColumns,
                      boss::Expression &&where,
		    std::vector<boss::Symbol> gatherColumns2, bool encode1, bool encode2, int32_t s1, int32_t s2) {
  auto project = wrapEval("Project"_(std::move(table),
				     std::move(createIndicesAsNoRename(tableColumns))), s1);
  auto indices = wrapEval("Project"_(wrapEval("Select"_(std::move(project), std::move(where)), s1),
				     std::move(createIndicesAsNoRename({}))), s1);
  auto gather2 = wrapEval("Gather"_(url, RBL_PATH, std::move(indices),
				    std::move(createSingleList(gatherColumns2))), s2);
  if (encode2) {
    auto encoded2 = wrapEval("EncodeTable"_(std::move(gather2)), s2);
    return std::move(encoded2);
  }

  return std::move(gather2);
}

inline boss::Expression TPCH_Q1_BOSS_CYCLE(TPCH_SF sf) {
  auto lineitemDateGatherIndices = "List"_("List"_());
  std::vector<boss::Symbol> lineitemDateGatherColumns = {"l_shipdate"_};
  auto lineitemDateWhere =
      "Where"_("Greater"_("DateObject"_("1998-08-31"), "l_shipdate"_));
  std::vector<boss::Symbol> lineitemFinalGatherColumns = {
      "l_returnflag"_,    "l_quantity"_,   "l_discount"_,
      "l_extendedprice"_, "l_linestatus"_, "l_tax"_};

  auto lineitemFinalGather = getGatherSelectGatherWrap(
      tableURLs[TPCH_LINEITEM][sf], std::move(lineitemDateGatherIndices),
      lineitemDateGatherColumns, std::move(lineitemDateWhere),
      lineitemFinalGatherColumns, false, true, 0, 1);

  auto query = wrapEval("Order"_(
      wrapEval("Project"_(
          wrapEval("Group"_(
              wrapEval("Project"_(
                  wrapEval("Project"_(
                      wrapEval("Project"_(std::move(lineitemFinalGather),
                                 "As"_("l_returnflag"_, "l_returnflag"_,
                                       "l_linestatus"_, "l_linestatus"_,
                                       "l_quantity"_, "l_quantity"_,
                                       "l_extendedprice"_, "l_extendedprice"_,
                                       "l_discount"_, "l_discount"_, "calc1"_,
                                       "Minus"_(1.0, "l_discount"_), "calc2"_,
                                       "Plus"_("l_tax"_, 1.0))), 1),
                      "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                            "l_linestatus"_, "l_quantity"_, "l_quantity"_,
                            "l_extendedprice"_, "l_extendedprice"_,
                            "l_discount"_, "l_discount"_, "disc_price"_,
                            "Times"_("l_extendedprice"_, "calc1"_), "calc2"_,
                            "calc2"_)), 1),
                  "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                        "l_linestatus"_, "l_quantity"_, "l_quantity"_,
                        "l_extendedprice"_, "l_extendedprice"_, "l_discount"_,
                        "l_discount"_, "disc_price"_, "disc_price"_, "calc"_,
                        "Times"_("disc_price"_, "calc2"_))), 1),
              "By"_("l_returnflag"_, "l_linestatus"_),
              "As"_("sum_qty"_, "Sum"_("l_quantity"_), "sum_base_price"_,
                    "Sum"_("l_extendedprice"_), "sum_disc_price"_,
                    "Sum"_("disc_price"_), "sum_charges"_, "Sum"_("calc"_),
                    "sum_disc"_, "Sum"_("l_discount"_), "count_order"_,
                    "Count"_("*"_))), 1),
          "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                "l_linestatus"_, "sum_qty"_, "sum_qty"_, "sum_base_price"_,
                "sum_base_price"_, "sum_disc_price"_, "sum_disc_price"_,
                "sum_charges"_, "sum_charges"_, "avg_qty"_,
                "Divide"_("sum_qty"_, "count_order"_), "avg_price"_,
                "Divide"_("sum_base_price"_, "count_order"_), "avg_disc"_,
                "Divide"_("sum_disc"_, "count_order"_), "count_order"_,
                "count_order"_)), 1),
      "By"_("l_returnflag"_, "l_linestatus"_)), 1);

  return std::move(query);
}

inline boss::Expression TPCH_Q3_BOSS_CYCLE(TPCH_SF sf) {
  auto customerMktSegmentGatherIndices = "List"_("List"_());
  std::vector<boss::Symbol> customerMktSegmentGatherColumns = {"c_mktsegment"_};
  auto customerMktSegmentWhere =
    "Where"_("Equal"_("c_mktsegment"_, "GetEncodingFor"_("BUILDING", "c_mktsegment"_)));
  std::vector<boss::Symbol> customerPreJoinGatherColumns = {"c_custkey"_};
  auto customerPreJoinGather = getGatherSelectGatherWrap(
      tableURLs[TPCH_CUSTOMER][sf], std::move(customerMktSegmentGatherIndices),
      customerMktSegmentGatherColumns, std::move(customerMktSegmentWhere),
      customerPreJoinGatherColumns, true, false, 0, 1);

  auto ordersDateGatherIndices = "List"_("List"_());
  std::vector<boss::Symbol> ordersDateGatherColumns = {"o_orderdate"_};
  auto ordersDateWhere =
      "Where"_("Greater"_("DateObject"_("1995-03-15"), "o_orderdate"_));
  std::vector<boss::Symbol> ordersPreJoinGatherColumns = {
      "o_custkey"_, "o_orderkey"_, "o_orderdate"_, "o_shippriority"_};
  auto ordersPreJoinGather = getGatherSelectGatherWrap(
      tableURLs[TPCH_ORDERS][sf], std::move(ordersDateGatherIndices),
      ordersDateGatherColumns, std::move(ordersDateWhere),
      ordersPreJoinGatherColumns, false, false, 2, 3);

  auto lineitemDateGatherIndices = "List"_("List"_());
  std::vector<boss::Symbol> lineitemDateGatherColumns = {"l_shipdate"_};
  auto lineitemDateWhere =
      "Where"_("Greater"_("DateObject"_("1995-03-15"), "l_shipdate"_));
  std::vector<boss::Symbol> lineitemPreJoinGatherColumns = {
      "l_orderkey"_, "l_extendedprice"_, "l_discount"_};
  auto lineitemPreJoinGather = getGatherSelectGatherWrap(
      tableURLs[TPCH_LINEITEM][sf], std::move(lineitemDateGatherIndices),
      lineitemDateGatherColumns, std::move(lineitemDateWhere),
      lineitemPreJoinGatherColumns, false, false, 4, 5);

  auto ordersCustomerJoin = wrapEval("Project"_(
      wrapEval("Join"_(std::move(ordersPreJoinGather), std::move(customerPreJoinGather),
		       "Where"_("Equal"_("o_custkey"_, "c_custkey"_))), 3),
      "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_,
            "o_shippriority"_, "o_shippriority"_)), 3);

  auto ordersLineitemJoin = wrapEval("Project"_(
      wrapEval("Join"_(std::move(ordersCustomerJoin), std::move(lineitemPreJoinGather),
		       "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))), 5),
      "As"_("l_orderkey"_, "l_orderkey"_, "l_extendedprice"_,
            "l_extendedprice"_, "l_discount"_, "l_discount"_, "o_orderdate"_,
            "o_orderdate"_, "o_shippriority"_, "o_shippriority"_)), 5);
  
  auto query =
      wrapEval("Top"_(wrapEval("Group"_(wrapEval("Project"_(std::move(ordersLineitemJoin),
                                 "As"_("expr1009"_,
                                       "Times"_("l_extendedprice"_,
                                                "Minus"_(1.0, "l_discount"_)),
                                       "l_extendedprice"_, "l_extendedprice"_,
                                       "l_orderkey"_, "l_orderkey"_,
                                       "o_orderdate"_, "o_orderdate"_,
                                       "o_shippriority"_, "o_shippriority"_)), 5),
                      "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
					"As"_("revenue"_, "Sum"_("expr1009"_))), 5),
		      "By"_("revenue"_, "desc"_, "o_orderdate"_), 10), 5);

  return std::move(query);
}

inline boss::Expression TPCH_Q6_BOSS_CYCLE(TPCH_SF sf) {
  auto lineitemDateGatherIndices = "List"_("List"_());
  std::vector<boss::Symbol> lineitemDateGatherColumns = {"l_shipdate"_,
                                                         "l_quantity"_};
  auto lineitemDateWhere =
      "Where"_("And"_("Greater"_(24, "l_quantity"_), // NOLINT
                      "Greater"_("DateObject"_("1995-01-01"), "l_shipdate"_),
                      "Greater"_("l_shipdate"_, "DateObject"_("1993-12-31"))));
  std::vector<boss::Symbol> lineitemFirstGatherColumns = {"l_discount"_, "l_extendedprice"_};
  auto lineitemFirstGather = getGatherSelectGatherWrap(
      tableURLs[TPCH_LINEITEM][sf], std::move(lineitemDateGatherIndices),
      lineitemDateGatherColumns, std::move(lineitemDateWhere),
      lineitemFirstGatherColumns, false, false, 0, 1);
  
  auto query = wrapEval("Group"_(
      wrapEval("Project"_(
          wrapEval("Select"_(wrapEval("Project"_(std::move(lineitemFirstGather),
                               "As"_("l_discount"_, "l_discount"_,
                                     "l_extendedprice"_, "l_extendedprice"_)), 1),
                    "Where"_("And"_("Greater"_("l_discount"_, 0.0499), // NOLINT
                                    "Greater"_(0.07001, "l_discount"_)))), 1),
          "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))), 1),
      "Sum"_("revenue"_)), 1);

  return std::move(query);
}

inline boss::Expression TPCH_Q9_BOSS_CYCLE(TPCH_SF sf) {
  auto partRetailPriceGatherIndices = "List"_("List"_());
  std::vector<boss::Symbol> partRetailPriceGatherColumns = {"p_retailprice"_};
  auto partRetailPriceWhere = "Where"_("And"_("Greater"_("p_retailprice"_,
                                                         1006.05), // NOLINT
                                              "Greater"_(1080.1,   // NOLINT
                                                         "p_retailprice"_)));
  std::vector<boss::Symbol> partFinalGatherColumns = {"p_partkey"_};
  auto partFinalGather = getGatherSelectGatherWrap(
      tableURLs[TPCH_PART][sf], std::move(partRetailPriceGatherIndices),
      partRetailPriceGatherColumns, std::move(partRetailPriceWhere),
      partFinalGatherColumns, false, false, 0, 1);

  auto orderGather =
      wrapEval("Gather"_(tableURLs[TPCH_ORDERS][sf], RBL_PATH, "List"_("List"_()),
			 "List"_("o_orderkey"_, "o_orderdate"_)), 1);
  auto nationGather =
      wrapEval("Gather"_(tableURLs[TPCH_NATION][sf], RBL_PATH, "List"_("List"_()),
                "List"_("n_name"_, "n_nationkey"_)), 1);
  auto supplierGather =
      wrapEval("Gather"_(tableURLs[TPCH_SUPPLIER][sf], RBL_PATH, "List"_("List"_()),
			 "List"_("s_suppkey"_, "s_nationkey"_)), 1);
  auto partsuppGather =
      wrapEval("Gather"_(tableURLs[TPCH_PARTSUPP][sf], RBL_PATH, "List"_("List"_()),
			 "List"_("ps_partkey"_, "ps_suppkey"_, "ps_supplycost"_)), 1);
  auto lineitemGather =
      wrapEval("Gather"_(tableURLs[TPCH_LINEITEM][sf], RBL_PATH, "List"_("List"_()),
                "List"_("l_partkey"_, "l_suppkey"_, "l_orderkey"_,
                        "l_extendedprice"_, "l_discount"_, "l_quantity"_)), 1);

  auto nationEncoded = wrapEval("EncodeTable"_(std::move(nationGather)), 1);

  auto query = wrapEval("Order"_(
      wrapEval("Group"_(
          wrapEval("Project"_(
              wrapEval("Join"_(
                  wrapEval("Project"_(std::move(orderGather),
                             "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                   "o_orderdate"_)), 1),
                  wrapEval("Project"_(
                      wrapEval("Join"_(
                          wrapEval("Project"_(
                              wrapEval("Join"_(
                                  wrapEval("Project"_(std::move(partFinalGather),
						      "As"_("p_partkey"_, "p_partkey"_)), 1),
                                  wrapEval("Project"_(
                                      wrapEval("Join"_(
                                          wrapEval("Project"_(
                                              wrapEval("Join"_(
                                                  wrapEval("Project"_(
                                                      std::move(nationEncoded),
                                                      "As"_("n_name"_,
                                                            "n_name"_,
                                                            "n_nationkey"_,
                                                            "n_nationkey"_)), 1),
                                                  wrapEval("Project"_(
                                                      std::move(supplierGather),
                                                      "As"_("s_suppkey"_,
                                                            "s_suppkey"_,
                                                            "s_nationkey"_,
                                                            "s_nationkey"_)), 1),
                                                  "Where"_("Equal"_(
                                                      "n_nationkey"_,
                                                      "s_nationkey"_))), 1),
                                              "As"_("n_name"_, "n_name"_,
                                                    "s_suppkey"_,
                                                    "s_suppkey"_)), 1),
                                          wrapEval("Project"_(std::move(partsuppGather),
                                                     "As"_("ps_partkey"_,
                                                           "ps_partkey"_,
                                                           "ps_suppkey"_,
                                                           "ps_suppkey"_,
                                                           "ps_supplycost"_,
                                                           "ps_supplycost"_)), 1),
                                          "Where"_("Equal"_("s_suppkey"_,
                                                            "ps_suppkey"_))), 1),
                                      "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                            "ps_partkey"_, "ps_suppkey"_,
                                            "ps_suppkey"_, "ps_supplycost"_,
                                            "ps_supplycost"_)), 1),
                                  "Where"_(
					   "Equal"_("p_partkey"_, "ps_partkey"_))), 1),
                              "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                    "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_,
                                    "ps_supplycost"_, "ps_supplycost"_)), 1),
                          wrapEval("Project"_(
                              std::move(lineitemGather),
                              "As"_("l_partkey"_, "l_partkey"_, "l_suppkey"_,
                                    "l_suppkey"_, "l_orderkey"_, "l_orderkey"_,
                                    "l_extendedprice"_, "l_extendedprice"_,
                                    "l_discount"_, "l_discount"_, "l_quantity"_,
                                    "l_quantity"_)), 1),
                          "Where"_(
                              "Equal"_("List"_("ps_partkey"_, "ps_suppkey"_),
                                       "List"_("l_partkey"_, "l_suppkey"_)))), 1),
                      "As"_("n_name"_, "n_name"_, "ps_supplycost"_,
                            "ps_supplycost"_, "l_orderkey"_, "l_orderkey"_,
                            "l_extendedprice"_, "l_extendedprice"_,
                            "l_discount"_, "l_discount"_, "l_quantity"_,
                            "l_quantity"_)), 1),
                  "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))), 1),
              "As"_("nation"_, "n_name"_, "o_year"_, "Year"_("o_orderdate"_),
                    "amount"_,
                    "Minus"_("Times"_("l_extendedprice"_,
                                      "Minus"_(1.0, "l_discount"_)),
                             "Times"_("ps_supplycost"_, "l_quantity"_)))), 1),
          "By"_("nation"_, "o_year"_), "Sum"_("amount"_)), 1),
      "By"_("nation"_, "o_year"_, "desc"_)), 1);

  return std::move(query);
}

inline boss::Expression TPCH_Q18_BOSS_CYCLE(TPCH_SF sf) {

  auto customerGather = wrapEval("Gather"_(tableURLs[TPCH_CUSTOMER][sf], RBL_PATH,
					   "List"_("List"_()), "List"_("c_custkey"_)), 0);
  auto lineitemGather =
      wrapEval("Gather"_(tableURLs[TPCH_LINEITEM][sf], RBL_PATH, "List"_("List"_()),
			 "List"_("l_orderkey"_, "l_quantity"_)), 0);
  auto orderGather = wrapEval("Gather"_(
      tableURLs[TPCH_ORDERS][sf], RBL_PATH, "List"_("List"_()),
      "List"_("o_custkey"_, "o_orderkey"_, "o_totalprice"_, "o_orderdate"_)), 0);

  auto query = wrapEval("Top"_(
      wrapEval("Group"_(
          wrapEval("Project"_(
              wrapEval("Join"_(
                  wrapEval("Select"_(
                      wrapEval("Group"_(wrapEval("Project"_(std::move(lineitemGather),
                                          "As"_("l_orderkey"_, "l_orderkey"_,
                                                "l_quantity"_, "l_quantity"_)), 0),
					"By"_("l_orderkey"_),
					"As"_("sum_l_quantity"_, "Sum"_("l_quantity"_))), 0),
                      "Where"_("Greater"_("sum_l_quantity"_, 300))), 0), // NOLINT
                  wrapEval("Project"_(
                      wrapEval("Join"_(
                          wrapEval("Project"_(std::move(customerGather),
					      "As"_("c_custkey"_, "c_custkey"_)), 0),
                          wrapEval("Project"_(std::move(orderGather),
                                     "As"_("o_orderkey"_, "o_orderkey"_,
                                           "o_custkey"_, "o_custkey"_,
                                           "o_orderdate"_, "o_orderdate"_,
                                           "o_totalprice"_, "o_totalprice"_)), 0),
                          "Where"_("Equal"_("c_custkey"_, "o_custkey"_))), 0),
                      "As"_("o_orderkey"_, "o_orderkey"_, "o_custkey"_,
                            "o_custkey"_, "o_orderdate"_, "o_orderdate"_,
                            "o_totalprice"_, "o_totalprice"_)), 0),
                  "Where"_("Equal"_("l_orderkey"_, "o_orderkey"_))), 0),
              "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                    "o_orderdate"_, "o_totalprice"_, "o_totalprice"_,
                    "o_custkey"_, "o_custkey"_, "sum_l_quantity"_,
                    "sum_l_quantity"_)), 0),
          "By"_("o_custkey"_, "o_orderkey"_, "o_orderdate"_, "o_totalprice"_),
          "Sum"_("sum_l_quantity"_)), 0),
      "By"_("o_totalprice"_, "desc"_, "o_orderdate"_), 100), 0);

  return std::move(query);
}

inline std::vector<std::function<boss::Expression(TPCH_SF)>>
    dataVaultsQueriesTPCH{TPCH_Q1_DV, TPCH_Q3_DV, TPCH_Q6_DV, TPCH_Q9_DV,
                          TPCH_Q18_DV};

inline std::vector<std::function<boss::Expression(TPCH_SF)>> bossCycleQueriesTPCH{
    TPCH_Q1_BOSS_CYCLE, TPCH_Q3_BOSS_CYCLE, TPCH_Q6_BOSS_CYCLE, TPCH_Q9_BOSS_CYCLE, TPCH_Q18_BOSS_CYCLE};
} // namespace boss::benchmarks::LazyLoading::TPCHQueries
