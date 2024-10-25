#include "config.hpp"
#include <DADS.hpp>
#include <ExpressionUtilities.hpp>
#include <iostream>
#include "utilities.hpp"

#pragma once

namespace dads::benchmarks::LazyLoading::TPCHQueries {

using namespace std;
using namespace dads;
using utilities::operator""_; // NOLINT(misc-unused-using-decls) clang-tidy bug
using dads::benchmarks::LazyLoading::utilities::wrapEval;
using dads::benchmarks::LazyLoading::config::paths::RBL_PATH;


  extern int64_t NUM_THREADS;
  int64_t NUM_THREADS = 40;

  extern int64_t NUM_RANGES;
  int64_t NUM_RANGES = 255;
  
  
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
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_orders.bin";
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

inline dads::Expression TPCH_Q1_DV(TPCH_SF sf) {
  std::vector<std::string> lineitemURLs = {tableURLs[TPCH_LINEITEM][sf]};

  auto eagerLoadLineitem = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
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

inline dads::Expression TPCH_Q3_DV(TPCH_SF sf) {
  std::vector<std::string> customerURLs = {tableURLs[TPCH_CUSTOMER][sf]};
  std::vector<std::string> ordersURLs = {tableURLs[TPCH_ORDERS][sf]};
  std::vector<std::string> lineitemURLs = {tableURLs[TPCH_LINEITEM][sf]};

  auto eagerLoadCustomer = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
                    std::move(vector(customerURLs)))))));
  auto eagerLoadOrders = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
                    std::move(vector(ordersURLs)))))));
  auto eagerLoadLineitem = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
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

inline dads::Expression TPCH_Q6_DV(TPCH_SF sf) {
  std::vector<std::string> lineitemURLs = {tableURLs[TPCH_LINEITEM][sf]};

  auto eagerLoadLineitem = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
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

inline dads::Expression TPCH_Q9_DV(TPCH_SF sf) {
  std::vector<std::string> partURLs = {tableURLs[TPCH_PART][sf]};
  std::vector<std::string> ordersURLs = {tableURLs[TPCH_ORDERS][sf]};
  std::vector<std::string> nationURLs = {tableURLs[TPCH_NATION][sf]};
  std::vector<std::string> supplierURLs = {tableURLs[TPCH_SUPPLIER][sf]};
  std::vector<std::string> partsuppURLs = {tableURLs[TPCH_PARTSUPP][sf]};
  std::vector<std::string> lineitemURLs = {tableURLs[TPCH_LINEITEM][sf]};

  auto eagerLoadPart = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
                    std::move(vector(partURLs)))))));
  auto eagerLoadOrders = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
                    std::move(vector(ordersURLs)))))));
  auto eagerLoadNation = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
                    std::move(vector(nationURLs)))))));
  auto eagerLoadSupplier = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
                    std::move(vector(supplierURLs)))))));
  auto eagerLoadPartsupp = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
                    std::move(vector(partsuppURLs)))))));
  auto eagerLoadLineitem = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
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

inline dads::Expression TPCH_Q18_DV(TPCH_SF sf) {
  std::vector<std::string> customerURLs = {tableURLs[TPCH_CUSTOMER][sf]};
  std::vector<std::string> ordersURLs = {tableURLs[TPCH_ORDERS][sf]};
  std::vector<std::string> lineitemURLs = {tableURLs[TPCH_LINEITEM][sf]};

  auto eagerLoadCustomer = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
                    std::move(vector(customerURLs)))))));
  auto eagerLoadOrders = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
                    std::move(vector(ordersURLs)))))));
  auto eagerLoadLineitem = "ParseTables"_(
      RBL_PATH, "List"_("List"_(std::move(dads::Span<std::string const>(
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

inline dads::Expression createSingleList(std::vector<dads::Symbol> symbols) {
  dads::ExpressionArguments args;
  for (dads::Symbol symbol : symbols) {
    args.push_back(symbol);
  }
  auto list = dads::ComplexExpression{"List"_, {}, std::move(args), {}};
  return std::move(list);
}

inline dads::Expression
createIndicesAsNoRename(std::vector<dads::Symbol> symbols, dads::Symbol indexColName = "__internal_indices_"_) {
  dads::ExpressionArguments args;
  args.push_back(indexColName);
  args.push_back(indexColName);
  for (dads::Symbol symbol : symbols) {
    args.push_back(symbol);
    args.push_back(symbol);
  }
  auto as = dads::ComplexExpression{"As"_, {}, std::move(args), {}};
  return std::move(as);
}

inline dads::Expression
getGatherSelectGather(std::string url, dads::Expression &&gatherIndices1,
                      std::vector<dads::Symbol> gatherColumns1,
                      dads::Expression &&where,
                      std::vector<dads::Symbol> gatherColumns2, bool encode1, bool encode2) {
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
  
inline dads::Expression
getSelectGather(std::string url, dads::Expression &&table,
                      std::vector<dads::Symbol> tableColumns,
                      dads::Expression &&where,
                      std::vector<dads::Symbol> gatherColumns2, bool encode1, bool encode2) {
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

inline dads::Expression
getGatherSelectGatherWrap(std::string url, dads::Expression &&gatherIndices1,
                      std::vector<dads::Symbol> gatherColumns1,
                      dads::Expression &&where,
			  std::vector<dads::Symbol> gatherColumns2, bool encode1, bool encode2, int32_t s1, int32_t s2, bool addIndices = true, dads::Symbol indexColName = "__internal_indices_"_) {
  auto const gather1 = wrapEval("Gather"_(url, RBL_PATH, std::move(gatherIndices1),
					  std::move(createSingleList(gatherColumns1)), NUM_RANGES, indexColName, NUM_THREADS), s1);
  auto encoded1 = std::move(gather1.clone(expressions::CloneReason::FOR_TESTING));
  if (encode1) {
    encoded1 = wrapEval("EncodeTable"_(std::move(gather1.clone(expressions::CloneReason::FOR_TESTING))), s1);
  }
  auto indicesEncoded1 = std::move(encoded1);
  if (addIndices) {
    indicesEncoded1 = "AddIndices"_(std::move(indicesEncoded1), indexColName);
  }
  auto project = wrapEval("Project"_(std::move(indicesEncoded1),
				     std::move(createIndicesAsNoRename(gatherColumns1, indexColName))), s1);
  auto indices = wrapEval("Project"_(wrapEval("Select"_(std::move(project), std::move(where)), s1),
				     std::move(createIndicesAsNoRename({}, indexColName))), s1);
  auto gather2 = wrapEval("Gather"_(url, RBL_PATH, std::move(indices),
				    std::move(createSingleList(gatherColumns2)), (int64_t) NUM_RANGES, indexColName, NUM_THREADS), s2);
  if (encode2) {
    auto encoded2 = wrapEval("EncodeTable"_(std::move(gather2)), s2);
    return std::move(encoded2);
  }

  return std::move(gather2);
}
  
inline dads::Expression
getSelectGatherWrap(std::string url, dads::Expression &&table,
                      std::vector<dads::Symbol> tableColumns,
                      dads::Expression &&where,
		    std::vector<dads::Symbol> gatherColumns2, bool encode1, bool encode2, int32_t s1, int32_t s2, dads::Symbol indexColName = "__internal_indices_"_) {
  auto project = wrapEval("Project"_(std::move(table),
				     std::move(createIndicesAsNoRename(tableColumns))), s1);
  auto indices = wrapEval("Project"_(wrapEval("Select"_(std::move(project), std::move(where)), s1),
				     std::move(createIndicesAsNoRename({}))), s1);
  auto gather2 = wrapEval("Gather"_(url, RBL_PATH, std::move(indices),
				    std::move(createSingleList(gatherColumns2)), (int64_t) NUM_RANGES, indexColName, NUM_THREADS), s2);
  if (encode2) {
    auto encoded2 = wrapEval("EncodeTable"_(std::move(gather2)), s2);
    return std::move(encoded2);
  }

  return std::move(gather2);
}
  
inline dads::Expression
getGatherWrap(std::string url, dads::Expression &&table,
	      std::vector<dads::Symbol> gatherColumns1, bool encode1, int32_t s1, dads::Symbol indexColName = "__internal_indices_"_) {
  auto gather1 = wrapEval("Gather"_(url, RBL_PATH, std::move(table),
				    std::move(createSingleList(gatherColumns1)), (int64_t) NUM_RANGES, indexColName, NUM_THREADS), s1);
  if (encode1) {
    auto encoded1 = wrapEval("EncodeTable"_(std::move(gather1)), s1);
    return std::move(encoded1);
  }

  return std::move(gather1);
}

inline dads::Expression TPCH_Q1_DADS(TPCH_SF sf) {
  auto lineitemDateGatherIndices = "List"_("List"_());
  std::vector<dads::Symbol> lineitemDateGatherColumns = {"l_shipdate"_};
  auto lineitemDateWhere =
      "Where"_("Greater"_("DateObject"_("1998-08-31"), "l_shipdate"_));
  std::vector<dads::Symbol> lineitemFinalGatherColumns = {
      "l_returnflag"_,    "l_quantity"_,   "l_discount"_,
      "l_extendedprice"_, "l_linestatus"_, "l_tax"_};

  auto lineitemFinalGather = getGatherSelectGather(
      tableURLs[TPCH_LINEITEM][sf], std::move(lineitemDateGatherIndices),
      lineitemDateGatherColumns, std::move(lineitemDateWhere),
      lineitemFinalGatherColumns, false, true);

  auto query = "Order"_(
      "Project"_(
          "Group"_(
              "Project"_(
                  "Project"_(
                      "Project"_(std::move(lineitemFinalGather),
                                 "As"_("l_returnflag"_, "l_returnflag"_,
                                       "l_linestatus"_, "l_linestatus"_,
                                       "l_quantity"_, "l_quantity"_,
                                       "l_extendedprice"_, "l_extendedprice"_,
                                       "l_discount"_, "l_discount"_, "calc1"_,
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

  return std::move(query);
}

inline dads::Expression TPCH_Q3_DADS(TPCH_SF sf) {
  auto customerMktSegmentGatherIndices = "List"_("List"_());
  std::vector<dads::Symbol> customerMktSegmentGatherColumns = {"c_mktsegment"_};
  auto customerMktSegmentWhere =
    "Where"_("Equal"_("c_mktsegment"_, "GetEncodingFor"_("BUILDING", "c_mktsegment"_)));
  std::vector<dads::Symbol> customerPreJoinGatherColumns = {"c_custkey"_};
  auto customerPreJoinGather = getGatherSelectGather(
      tableURLs[TPCH_CUSTOMER][sf], std::move(customerMktSegmentGatherIndices),
      customerMktSegmentGatherColumns, std::move(customerMktSegmentWhere),
      customerPreJoinGatherColumns, true, false);

  auto ordersDateGatherIndices = "List"_("List"_());
  std::vector<dads::Symbol> ordersDateGatherColumns = {"o_orderdate"_};
  auto ordersDateWhere =
      "Where"_("Greater"_("DateObject"_("1995-03-15"), "o_orderdate"_));
  std::vector<dads::Symbol> ordersPreJoinGatherColumns = {
      "o_custkey"_, "o_orderkey"_, "o_orderdate"_, "o_shippriority"_};
  auto ordersPreJoinGather = getGatherSelectGather(
      tableURLs[TPCH_ORDERS][sf], std::move(ordersDateGatherIndices),
      ordersDateGatherColumns, std::move(ordersDateWhere),
      ordersPreJoinGatherColumns, false, false);

  auto lineitemDateGatherIndices = "List"_("List"_());
  std::vector<dads::Symbol> lineitemDateGatherColumns = {"l_shipdate"_};
  auto lineitemDateWhere =
      "Where"_("Greater"_("DateObject"_("1995-03-15"), "l_shipdate"_));
  std::vector<dads::Symbol> lineitemPreJoinGatherColumns = {
      "l_orderkey"_, "l_extendedprice"_, "l_discount"_};
  auto lineitemPreJoinGather = getGatherSelectGather(
      tableURLs[TPCH_LINEITEM][sf], std::move(lineitemDateGatherIndices),
      lineitemDateGatherColumns, std::move(lineitemDateWhere),
      lineitemPreJoinGatherColumns, false, false);

  auto ordersCustomerJoin = "Project"_(
      "Join"_(std::move(ordersPreJoinGather), std::move(customerPreJoinGather),
              "Where"_("Equal"_("o_custkey"_, "c_custkey"_))),
      "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_,
            "o_shippriority"_, "o_shippriority"_));

  auto ordersLineitemJoin = "Project"_(
      "Join"_(std::move(ordersCustomerJoin), std::move(lineitemPreJoinGather),
              "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
      "As"_("l_orderkey"_, "l_orderkey"_, "l_extendedprice"_,
            "l_extendedprice"_, "l_discount"_, "l_discount"_, "o_orderdate"_,
            "o_orderdate"_, "o_shippriority"_, "o_shippriority"_));
  
  auto query =
      "Top"_("Group"_("Project"_(std::move(ordersLineitemJoin),
                                 "As"_("expr1009"_,
                                       "Times"_("l_extendedprice"_,
                                                "Minus"_(1.0, "l_discount"_)),
                                       "l_extendedprice"_, "l_extendedprice"_,
                                       "l_orderkey"_, "l_orderkey"_,
                                       "o_orderdate"_, "o_orderdate"_,
                                       "o_shippriority"_, "o_shippriority"_)),
                      "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
                      "As"_("revenue"_, "Sum"_("expr1009"_))),
             "By"_("revenue"_, "desc"_, "o_orderdate"_), 10);

  return std::move(query);
}

inline dads::Expression TPCH_Q6_DADS(TPCH_SF sf) {
  auto lineitemDateGatherIndices = "List"_("List"_());
  std::vector<dads::Symbol> lineitemDateGatherColumns = {"l_shipdate"_,
                                                         "l_quantity"_};
  auto lineitemDateWhere =
      "Where"_("And"_("Greater"_(24, "l_quantity"_), // NOLINT
                      "Greater"_("DateObject"_("1995-01-01"), "l_shipdate"_),
                      "Greater"_("l_shipdate"_, "DateObject"_("1993-12-31"))));
  std::vector<dads::Symbol> lineitemFirstGatherColumns = {"l_discount"_, "l_extendedprice"_};
  auto lineitemFirstGather = getGatherSelectGather(
      tableURLs[TPCH_LINEITEM][sf], std::move(lineitemDateGatherIndices),
      lineitemDateGatherColumns, std::move(lineitemDateWhere),
      lineitemFirstGatherColumns, false, false);
  
  auto query = "Group"_(
      "Project"_(
          "Select"_("Project"_(std::move(lineitemFirstGather),
                               "As"_("l_discount"_, "l_discount"_,
                                     "l_extendedprice"_, "l_extendedprice"_)),
                    "Where"_("And"_("Greater"_("l_discount"_, 0.0499), // NOLINT
                                    "Greater"_(0.07001, "l_discount"_)))),
          "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))),
      "Sum"_("revenue"_));

  return std::move(query);
}

inline dads::Expression TPCH_Q9_DADS(TPCH_SF sf) {
  auto partRetailPriceGatherIndices = "List"_("List"_());
  std::vector<dads::Symbol> partRetailPriceGatherColumns = {"p_retailprice"_};
  auto partRetailPriceWhere = "Where"_("And"_("Greater"_("p_retailprice"_,
                                                         1006.05), // NOLINT
                                              "Greater"_(1080.1,   // NOLINT
                                                         "p_retailprice"_)));
  std::vector<dads::Symbol> partFinalGatherColumns = {"p_partkey"_};
  auto partFinalGather = getGatherSelectGather(
      tableURLs[TPCH_PART][sf], std::move(partRetailPriceGatherIndices),
      partRetailPriceGatherColumns, std::move(partRetailPriceWhere),
      partFinalGatherColumns, false, false);

  auto orderGather =
      "Gather"_(tableURLs[TPCH_ORDERS][sf], RBL_PATH, "List"_("List"_()),
                "List"_("o_orderkey"_, "o_orderdate"_));
  auto nationGather =
      "Gather"_(tableURLs[TPCH_NATION][sf], RBL_PATH, "List"_("List"_()),
                "List"_("n_name"_, "n_nationkey"_));
  auto supplierGather =
      "Gather"_(tableURLs[TPCH_SUPPLIER][sf], RBL_PATH, "List"_("List"_()),
                "List"_("s_suppkey"_, "s_nationkey"_));
  auto partsuppGather =
      "Gather"_(tableURLs[TPCH_PARTSUPP][sf], RBL_PATH, "List"_("List"_()),
                "List"_("ps_partkey"_, "ps_suppkey"_, "ps_supplycost"_));
  auto lineitemGather =
      "Gather"_(tableURLs[TPCH_LINEITEM][sf], RBL_PATH, "List"_("List"_()),
                "List"_("l_partkey"_, "l_suppkey"_, "l_orderkey"_,
                        "l_extendedprice"_, "l_discount"_, "l_quantity"_));

  auto nationEncoded = "EncodeTable"_(std::move(nationGather));

  auto query = "Order"_(
      "Group"_(
          "Project"_(
              "Join"_(
                  "Project"_(std::move(orderGather),
                             "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                   "o_orderdate"_)),
                  "Project"_(
                      "Join"_(
                          "Project"_(
                              "Join"_(
                                  "Project"_(std::move(partFinalGather),
                                             "As"_("p_partkey"_, "p_partkey"_)),
                                  "Project"_(
                                      "Join"_(
                                          "Project"_(
                                              "Join"_(
                                                  "Project"_(
                                                      std::move(nationEncoded),
                                                      "As"_("n_name"_,
                                                            "n_name"_,
                                                            "n_nationkey"_,
                                                            "n_nationkey"_)),
                                                  "Project"_(
                                                      std::move(supplierGather),
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
                                          "Project"_(std::move(partsuppGather),
                                                     "As"_("ps_partkey"_,
                                                           "ps_partkey"_,
                                                           "ps_suppkey"_,
                                                           "ps_suppkey"_,
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
                              std::move(lineitemGather),
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

  return std::move(query);
}

inline dads::Expression TPCH_Q18_DADS(TPCH_SF sf) {

  auto customerGather = "Gather"_(tableURLs[TPCH_CUSTOMER][sf], RBL_PATH,
                                  "List"_("List"_()), "List"_("c_custkey"_));
  auto lineitemGather =
      "Gather"_(tableURLs[TPCH_LINEITEM][sf], RBL_PATH, "List"_("List"_()),
                "List"_("l_orderkey"_, "l_quantity"_));
  auto orderGather = "Gather"_(
      tableURLs[TPCH_ORDERS][sf], RBL_PATH, "List"_("List"_()),
      "List"_("o_custkey"_, "o_orderkey"_, "o_totalprice"_, "o_orderdate"_));

  auto query = "Top"_(
      "Group"_(
          "Project"_(
              "Join"_(
                  "Select"_(
                      "Group"_("Project"_(std::move(lineitemGather),
                                          "As"_("l_orderkey"_, "l_orderkey"_,
                                                "l_quantity"_, "l_quantity"_)),
                               "By"_("l_orderkey"_),
                               "As"_("sum_l_quantity"_, "Sum"_("l_quantity"_))),
                      "Where"_("Greater"_("sum_l_quantity"_, 300))), // NOLINT
                  "Project"_(
                      "Join"_(
                          "Project"_(std::move(customerGather),
                                     "As"_("c_custkey"_, "c_custkey"_)),
                          "Project"_(std::move(orderGather),
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

  return std::move(query);
}
  
inline dads::Expression TPCH_Q1_DADS_CYCLE(TPCH_SF sf) {
  auto lineitemIndices1 = "List"_("List"_());
  std::vector<dads::Symbol> lineitemSelectionColumns1 = {"l_shipdate"_};
  std::vector<dads::Symbol> lineitemFinalColumns = {
      "l_returnflag"_,    "l_quantity"_,   "l_discount"_,
      "l_extendedprice"_, "l_linestatus"_, "l_tax"_};
  
  auto lineitemSelectionWhere1 =
      "Where"_("Greater"_("DateObject"_("1998-08-31"), "l_shipdate"_));

  auto lineitemGather1 = getGatherSelectGatherWrap(tableURLs[TPCH_LINEITEM][sf], std::move(lineitemIndices1),
						   lineitemSelectionColumns1, std::move(lineitemSelectionWhere1),
						   lineitemFinalColumns, false, true, 0, 1);

  // auto lineitemDateGatherIndices = "List"_("List"_());
  // std::vector<dads::Symbol> lineitemDateGatherColumns = {"l_shipdate"_};
  // auto lineitemDateWhere =
  //     "Where"_("Greater"_("DateObject"_("1998-08-31"), "l_shipdate"_));
  // std::vector<dads::Symbol> lineitemFinalGatherColumns = {
  //     "l_returnflag"_,    "l_quantity"_,   "l_discount"_,
  //     "l_extendedprice"_, "l_linestatus"_, "l_tax"_};

  // auto lineitemFinalGather = getGatherSelectGatherWrap(
  //     tableURLs[TPCH_LINEITEM][sf], std::move(lineitemDateGatherIndices),
  //     lineitemDateGatherColumns, std::move(lineitemDateWhere),
  //     lineitemFinalGatherColumns, false, true, 0, 1);

  auto query = wrapEval("Order"_(
      wrapEval("Project"_(
          wrapEval("Group"_(
              wrapEval("Project"_(
                  wrapEval("Project"_(
                      wrapEval("Project"_(std::move(lineitemGather1),
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

inline dads::Expression TPCH_Q3_DADS_CYCLE(TPCH_SF sf) {
  // Customer
  auto customersIndexColName = "c__internal_indices_"_;
  auto customerIndices1 = "List"_("List"_());
  std::vector<dads::Symbol> customerColumns1 = {"c_mktsegment"_};
  std::vector<dads::Symbol> customerColumns2 = {"c_custkey"_};

  auto customerSelectionWhere1 = "Where"_("Equal"_("c_mktsegment"_, "GetEncodingFor"_("BUILDING", "c_mktsegment"_)));

  auto customerGather1 = getGatherSelectGatherWrap(
      tableURLs[TPCH_CUSTOMER][sf], std::move(customerIndices1),
      customerColumns1, std::move(customerSelectionWhere1),
      customerColumns2, true, false, 0, 1, true, customersIndexColName);

  // Orders
  auto ordersIndexColName = "o__internal_indices_"_;
  auto ordersIndices1 = "List"_("List"_());
  std::vector<dads::Symbol> ordersColumns1 = {"o_orderdate"_};
  std::vector<dads::Symbol> ordersColumns2 = {"o_custkey"_};

  auto ordersSelectionWhere1 = "Where"_("Greater"_("DateObject"_("1995-03-15"), "o_orderdate"_));

  auto ordersGather1 = getGatherSelectGatherWrap(
      tableURLs[TPCH_ORDERS][sf], std::move(ordersIndices1),
      ordersColumns1, std::move(ordersSelectionWhere1),
      ordersColumns2, false, false, 2, 3, true, ordersIndexColName);
  
  // Lineitem
  auto lineitemIndexColName = "l__internal_indices_"_;
  auto lineitemIndices1 = "List"_("List"_());
  std::vector<dads::Symbol> lineitemColumns1 = {"l_shipdate"_};
  std::vector<dads::Symbol> lineitemColumns2 = {"l_orderkey"_, "l_extendedprice"_, "l_discount"_};
  
  auto lineitemSelectionWhere1 = "Where"_("Greater"_("DateObject"_("1995-03-15"), "l_shipdate"_));
  
  auto lineitemGather1 = getGatherSelectGatherWrap(
      tableURLs[TPCH_LINEITEM][sf], std::move(lineitemIndices1),
      lineitemColumns1, std::move(lineitemSelectionWhere1),
      lineitemColumns2, false, false, 4, 5, true, lineitemIndexColName);

  // Customer & Orders
  auto ordersCustomerJoin1 = wrapEval("Project"_(
      wrapEval("Join"_(std::move(ordersGather1), std::move(customerGather1),
		       "Where"_("Equal"_("o_custkey"_, "c_custkey"_))), 3),
      "As"_(ordersIndexColName, ordersIndexColName)), 3);

  // auto saveOrderCustomerJoinIndices = wrapEval("SaveTable"_(std::move(ordersCustomerJoin1), "OrderIndices"_), 4);

  // Orders
  std::vector<dads::Symbol> ordersColumns3 = {"o_orderkey"_, "o_shippriority"_, "o_orderdate"_};
  
  auto ordersGather2 = getGatherWrap(tableURLs[TPCH_ORDERS][sf], std::move(ordersCustomerJoin1),
				     ordersColumns3, false, 5, ordersIndexColName);
  // auto ordersGather2 = getGatherWrap(tableURLs[TPCH_ORDERS][sf], std::move(saveOrderCustomerJoinIndices),
  // 				     ordersColumns3, false, 6, ordersIndexColName);

  // Lineitem & Orders
  // auto ordersLineitemJoin1 = wrapEval("Project"_(
  // 		       wrapEval("Join"_(std::move(lineitemGather1), std::move(ordersGather2),
  // 		       "Where"_("Equal"_("l_orderkey"_, "o_orderkey"_))), 6),
  //     "As"_(lineitemIndexColName, lineitemIndexColName)), 6);

  // Orders
  // auto orderIndices2 = wrapEval("GetTable"_("OrderIndices"_), 6);
  // std::vector<dads::Symbol> ordersColumns4 = {"o_shippriority"_, "o_orderdate"_, "o_orderkey"_};
  
  // auto ordersGather3 = getGatherWrap(tableURLs[TPCH_ORDERS][sf], std::move(orderIndices2),
  // 				     ordersColumns4, false, 7, ordersIndexColName);

  // Lineitem
  // std::vector<dads::Symbol> lineitemColumns3 = {"l_orderkey"_, "l_extendedprice"_, "l_discount"_};
  
  // auto lineitemGather2 = getGatherWrap(tableURLs[TPCH_LINEITEM][sf], std::move(lineitemGather1),
  // 				       lineitemColumns3, false, 5, lineitemIndexColName);
  // auto lineitemGather2 = getGatherWrap(tableURLs[TPCH_LINEITEM][sf], std::move(ordersLineitemJoin1),
  // 				       lineitemColumns3, false, 8, lineitemIndexColName);

  // Lineitem & Orders
  auto ordersLineitemJoin2 = wrapEval("Project"_(
      wrapEval("Join"_(std::move(ordersGather2), std::move(lineitemGather1),
		       "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))), 5),
      "As"_("l_orderkey"_, "l_orderkey"_, "l_extendedprice"_, "l_extendedprice"_,
	    "l_discount"_, "l_discount"_, "o_orderdate"_, "o_orderdate"_,
	    "o_shippriority"_, "o_shippriority"_)), 5);

  
  auto query =
      wrapEval("Top"_(wrapEval("Group"_(wrapEval("Project"_(std::move(ordersLineitemJoin2),
                                 "As"_("expr1009"_,
                                       "Times"_("l_extendedprice"_,
                                                "Minus"_(1.0, "l_discount"_)),
                                       "l_extendedprice"_, "l_extendedprice"_,
                                       "l_orderkey"_, "l_orderkey"_,
                                       "o_orderdate"_, "o_orderdate"_,
                                       "o_shippriority"_, "o_shippriority"_)), 8),
                      "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
					"As"_("revenue"_, "Sum"_("expr1009"_))), 8),
		      "By"_("revenue"_, "desc"_, "o_orderdate"_), 10), 8);
  
  // auto customerMktSegmentGatherIndices = "List"_("List"_());
  // std::vector<dads::Symbol> customerMktSegmentGatherColumns = {"c_mktsegment"_};
  // auto customerMktSegmentWhere =
  //   "Where"_("Equal"_("c_mktsegment"_, "GetEncodingFor"_("BUILDING", "c_mktsegment"_)));
  // std::vector<dads::Symbol> customerPreJoinGatherColumns = {"c_custkey"_};
  // auto customerPreJoinGather = getGatherSelectGatherWrap(
  //     tableURLs[TPCH_CUSTOMER][sf], std::move(customerMktSegmentGatherIndices),
  //     customerMktSegmentGatherColumns, std::move(customerMktSegmentWhere),
  //     customerPreJoinGatherColumns, true, false, 0, 1);

  // auto ordersDateGatherIndices = "List"_("List"_());
  // std::vector<dads::Symbol> ordersDateGatherColumns = {"o_orderdate"_};
  // auto ordersDateWhere =
  //     "Where"_("Greater"_("DateObject"_("1995-03-15"), "o_orderdate"_));
  // std::vector<dads::Symbol> ordersPreJoinGatherColumns = {
  //     "o_custkey"_, "o_orderkey"_, "o_orderdate"_, "o_shippriority"_};
  // auto ordersPreJoinGather = getGatherSelectGatherWrap(
  //     tableURLs[TPCH_ORDERS][sf], std::move(ordersDateGatherIndices),
  //     ordersDateGatherColumns, std::move(ordersDateWhere),
  //     ordersPreJoinGatherColumns, false, false, 2, 3);

  // auto lineitemDateGatherIndices = "List"_("List"_());
  // std::vector<dads::Symbol> lineitemDateGatherColumns = {"l_shipdate"_};
  // auto lineitemDateWhere =
  //     "Where"_("Greater"_("DateObject"_("1995-03-15"), "l_shipdate"_));
  // std::vector<dads::Symbol> lineitemPreJoinGatherColumns = {
  //     "l_orderkey"_, "l_extendedprice"_, "l_discount"_};
  // auto lineitemPreJoinGather = getGatherSelectGatherWrap(
  //     tableURLs[TPCH_LINEITEM][sf], std::move(lineitemDateGatherIndices),
  //     lineitemDateGatherColumns, std::move(lineitemDateWhere),
  //     lineitemPreJoinGatherColumns, false, false, 4, 5);

  // auto ordersCustomerJoin = wrapEval("Project"_(
  //     wrapEval("Join"_(std::move(ordersPreJoinGather), std::move(customerPreJoinGather),
  // 		       "Where"_("Equal"_("o_custkey"_, "c_custkey"_))), 3),
  //     "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_,
  //           "o_shippriority"_, "o_shippriority"_)), 3);

  // auto ordersLineitemJoin = wrapEval("Project"_(
  //     wrapEval("Join"_(std::move(ordersCustomerJoin), std::move(lineitemPreJoinGather),
  // 		       "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))), 5),
  //     "As"_("l_orderkey"_, "l_orderkey"_, "l_extendedprice"_,
  //           "l_extendedprice"_, "l_discount"_, "l_discount"_, "o_orderdate"_,
  //           "o_orderdate"_, "o_shippriority"_, "o_shippriority"_)), 5);
  
  // auto query =
  //     wrapEval("Top"_(wrapEval("Group"_(wrapEval("Project"_(std::move(ordersLineitemJoin),
  //                                "As"_("expr1009"_,
  //                                      "Times"_("l_extendedprice"_,
  //                                               "Minus"_(1.0, "l_discount"_)),
  //                                      "l_extendedprice"_, "l_extendedprice"_,
  //                                      "l_orderkey"_, "l_orderkey"_,
  //                                      "o_orderdate"_, "o_orderdate"_,
  //                                      "o_shippriority"_, "o_shippriority"_)), 5),
  //                     "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
  // 					"As"_("revenue"_, "Sum"_("expr1009"_))), 5),
  // 		      "By"_("revenue"_, "desc"_, "o_orderdate"_), 10), 5);

  return std::move(query);
}

inline dads::Expression TPCH_Q6_DADS_CYCLE(TPCH_SF sf) {
  auto lineitemIndices1 = "List"_("List"_());
  std::vector<dads::Symbol> lineitemSelectionColumns1 = {"l_shipdate"_};
  std::vector<dads::Symbol> lineitemSelectionColumns2 = {"l_quantity"_};
  std::vector<dads::Symbol> lineitemSelectionColumns3 = {"l_discount"_};
  std::vector<dads::Symbol> lineitemFinalColumns = {"l_discount"_, "l_extendedprice"_};

  auto lineitemSelectionWhere1 =
    "Where"_("And"_("Greater"_("DateObject"_("1995-01-01"), "l_shipdate"_),
		    "Greater"_("l_shipdate"_, "DateObject"_("1993-12-31"))));
  auto lineitemSelectionWhere2 =
    "Where"_("Greater"_(24, "l_quantity"_));
  auto lineitemSelectionWhere3 =
    "Where"_("And"_("Greater"_("l_discount"_, 0.0499),
		    "Greater"_(0.07001, "l_discount"_)));

  auto lineitemGather1 = getGatherSelectGatherWrap(
      tableURLs[TPCH_LINEITEM][sf], std::move(lineitemIndices1),
      lineitemSelectionColumns1, std::move(lineitemSelectionWhere1),
      lineitemSelectionColumns2, false, false, 0, 1);
  auto lineitemGather2 = getSelectGatherWrap(
      tableURLs[TPCH_LINEITEM][sf], std::move(lineitemGather1),
      lineitemSelectionColumns2, std::move(lineitemSelectionWhere2),
      lineitemSelectionColumns3, false, false, 1, 2);
  auto lineitemGather3 = getSelectGatherWrap(
      tableURLs[TPCH_LINEITEM][sf], std::move(lineitemGather2),
      lineitemSelectionColumns3, std::move(lineitemSelectionWhere3),
      lineitemFinalColumns, false, false, 2, 3);

  auto query = wrapEval("Group"_(
      wrapEval("Project"_(
          wrapEval("Project"_(std::move(lineitemGather3),
                               "As"_("l_discount"_, "l_discount"_,
                                     "l_extendedprice"_, "l_extendedprice"_)), 3),
          "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))), 3),
      "Sum"_("revenue"_)), 3);

  return std::move(query);
}

inline dads::Expression TPCH_Q9_DADS_CYCLE(TPCH_SF sf) {

  // Part

  // auto partIndicesTmp = "List"_("List"_());  
  // std::vector<dads::Symbol> tmpCols = {"p_partkey"_};

  // auto tmpGather = getGatherWrap(tableURLs[TPCH_PART][sf], std::move(partIndicesTmp),
  // 				       tmpCols, false, 0);
  // return std::move(tmpGather);
  
  auto partIndices1 = "List"_("List"_());
  std::vector<dads::Symbol> partColumns1 = {"p_retailprice"_};
  std::vector<dads::Symbol> partColumns2 = {"p_partkey"_};

  auto partSelectionWhere1 = "Where"_("And"_("Greater"_("p_retailprice"_,
                                                         1006.05), // NOLINT
                                              "Greater"_(1080.1,   // NOLINT
                                                         "p_retailprice"_)));

  auto partGather1 = getGatherSelectGatherWrap(
      tableURLs[TPCH_PART][sf], std::move(partIndices1),
      partColumns1, std::move(partSelectionWhere1),
      partColumns2, false, false, 0, 1);

  auto savePartSelection = wrapEval("SaveTable"_(std::move(partGather1), "PartSelection"_), 1);

  // Lineitem
  auto lineitemIndexColName = "l__internal_indices_"_;
  auto lineitemIndices1 = "List"_("List"_());
  std::vector<dads::Symbol> lineitemColumns1 = {"l_partkey"_};

  auto lineitemGather1 =
    wrapEval("Project"_("AddIndices"_(std::move(getGatherWrap(tableURLs[TPCH_LINEITEM][sf], std::move(lineitemIndices1),
							      lineitemColumns1, false, 2, lineitemIndexColName)), lineitemIndexColName),
			"As"_(lineitemIndexColName, lineitemIndexColName, "l_partkey"_, "l_partkey"_)), 2);

  // Part & Lineitem
  auto partLineitemJoin1 = wrapEval("Project"_(
      wrapEval("Join"_(std::move(savePartSelection), std::move(lineitemGather1),
		       "Where"_("Equal"_("p_partkey"_, "l_partkey"_))), 2),
      "As"_(lineitemIndexColName, lineitemIndexColName)), 2);

  // Lineitem
  std::vector<dads::Symbol> lineitemColumns2 = {"l_suppkey"_};

  auto lineitemGather2 = getGatherWrap(tableURLs[TPCH_LINEITEM][sf], std::move(partLineitemJoin1),
				       lineitemColumns2, false, 5, lineitemIndexColName);

  // Supplier
  auto supplierIndices1 = "List"_("List"_());
  std::vector<dads::Symbol> supplierColumns1 = {"s_suppkey"_, "s_nationkey"_};

  auto supplierGather1 = getGatherWrap(tableURLs[TPCH_SUPPLIER][sf], std::move(supplierIndices1),
				       supplierColumns1, false, 3);
  
  // Nation
  auto nationIndices1 = "List"_("List"_());
  std::vector<dads::Symbol> nationColumns1 = {"n_nationkey"_};

  auto nationGather1 = getGatherWrap(tableURLs[TPCH_NATION][sf], std::move(nationIndices1),
				       nationColumns1, false, 4);

  // Supplier & Nation
  auto supplierNationJoin1 = wrapEval("Project"_(
      wrapEval("Join"_(std::move(supplierGather1), std::move(nationGather1),
		       "Where"_("Equal"_("s_nationkey"_, "n_nationkey"_))), 4),
      "As"_("s_suppkey"_, "s_suppkey"_)), 4);

  // Lineitem & Supplier
  auto lineitemSupplierJoin1 = wrapEval("Project"_(
      wrapEval("Join"_(std::move(lineitemGather2), std::move(supplierNationJoin1),
		       "Where"_("Equal"_("l_suppkey"_, "s_suppkey"_))), 5),
      "As"_(lineitemIndexColName, lineitemIndexColName)), 5);

  // Lineitem
  std::vector<dads::Symbol> lineitemColumns3 = {"l_suppkey"_, "l_partkey"_, "l_orderkey"_, "l_discount"_, "l_extendedprice"_, "l_quantity"_};
 
  auto lineitemGather3 = getGatherWrap(tableURLs[TPCH_LINEITEM][sf], std::move(lineitemSupplierJoin1),
				       lineitemColumns3, false, 6, lineitemIndexColName);
  
  // Orders
  auto ordersIndices1 = "List"_("List"_());
  std::vector<dads::Symbol> ordersColumns1 = {"o_orderkey"_, "o_orderdate"_};

  auto ordersGather1 = getGatherWrap(tableURLs[TPCH_ORDERS][sf], std::move(ordersIndices1),
				       ordersColumns1, false, 6);
  
  // Partsupp
  auto partsuppIndices1 = "List"_("List"_());
  std::vector<dads::Symbol> partsuppColumns1 = {"ps_partkey"_, "ps_suppkey"_, "ps_supplycost"_};

  auto partsuppGather1 = getGatherWrap(tableURLs[TPCH_PARTSUPP][sf], std::move(partsuppIndices1),
				       partsuppColumns1, false, 6);

  // Supplier
  auto supplierIndices2 = "List"_("List"_());
  std::vector<dads::Symbol> supplierColumns2 = {"s_suppkey"_, "s_nationkey"_};

  auto supplierGather2 = getGatherWrap(tableURLs[TPCH_SUPPLIER][sf], std::move(supplierIndices2),
				       supplierColumns2, false, 6);
  
  // Nation
  auto nationIndices2 = "List"_("List"_());
  std::vector<dads::Symbol> nationColumns2 = {"n_nationkey"_, "n_name"_};

  auto nationGather2 = getGatherWrap(tableURLs[TPCH_NATION][sf], std::move(nationIndices2),
				       nationColumns2, true, 6);

  // Part
  auto partSelection = wrapEval("GetTable"_("PartSelection"_), 5);

  auto query = wrapEval("Order"_(
      wrapEval("Group"_(
          wrapEval("Project"_(
              wrapEval("Join"_(
                  wrapEval("Project"_(std::move(ordersGather1),
                             "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                   "o_orderdate"_)), 6),
                  wrapEval("Project"_(
                      wrapEval("Join"_(
                          wrapEval("Project"_(
                              wrapEval("Join"_(
                                  wrapEval("Project"_(std::move(partSelection),
						      "As"_("p_partkey"_, "p_partkey"_)), 6),
                                  wrapEval("Project"_(
                                      wrapEval("Join"_(
                                          wrapEval("Project"_(
                                              wrapEval("Join"_(
                                                  wrapEval("Project"_(
                                                      std::move(nationGather2),
                                                      "As"_("n_name"_,
                                                            "n_name"_,
                                                            "n_nationkey"_,
                                                            "n_nationkey"_)), 6),
                                                  wrapEval("Project"_(
                                                      std::move(supplierGather2),
                                                      "As"_("s_suppkey"_,
                                                            "s_suppkey"_,
                                                            "s_nationkey"_,
                                                            "s_nationkey"_)), 6),
                                                  "Where"_("Equal"_(
                                                      "n_nationkey"_,
                                                      "s_nationkey"_))), 6),
                                              "As"_("n_name"_, "n_name"_,
                                                    "s_suppkey"_,
                                                    "s_suppkey"_)), 6),
                                          wrapEval("Project"_(std::move(partsuppGather1),
                                                     "As"_("ps_partkey"_,
                                                           "ps_partkey"_,
                                                           "ps_suppkey"_,
                                                           "ps_suppkey"_,
                                                           "ps_supplycost"_,
                                                           "ps_supplycost"_)), 6),
                                          "Where"_("Equal"_("s_suppkey"_,
                                                            "ps_suppkey"_))), 6),
                                      "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                            "ps_partkey"_, "ps_suppkey"_,
                                            "ps_suppkey"_, "ps_supplycost"_,
                                            "ps_supplycost"_)), 6),
                                  "Where"_(
					   "Equal"_("p_partkey"_, "ps_partkey"_))), 6),
                              "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                    "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_,
                                    "ps_supplycost"_, "ps_supplycost"_)), 6),
                          wrapEval("Project"_(
                              std::move(lineitemGather3),
                              "As"_("l_partkey"_, "l_partkey"_, "l_suppkey"_,
                                    "l_suppkey"_, "l_orderkey"_, "l_orderkey"_,
                                    "l_extendedprice"_, "l_extendedprice"_,
                                    "l_discount"_, "l_discount"_, "l_quantity"_,
                                    "l_quantity"_)), 6),
                          "Where"_(
                              "Equal"_("List"_("ps_partkey"_, "ps_suppkey"_),
                                       "List"_("l_partkey"_, "l_suppkey"_)))), 6),
                      "As"_("n_name"_, "n_name"_, "ps_supplycost"_,
                            "ps_supplycost"_, "l_orderkey"_, "l_orderkey"_,
                            "l_extendedprice"_, "l_extendedprice"_,
                            "l_discount"_, "l_discount"_, "l_quantity"_,
                            "l_quantity"_)), 6),
                  "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))), 6),
              "As"_("nation"_, "n_name"_, "o_year"_, "Year"_("o_orderdate"_),
                    "amount"_,
                    "Minus"_("Times"_("l_extendedprice"_,
                                      "Minus"_(1.0, "l_discount"_)),
                             "Times"_("ps_supplycost"_, "l_quantity"_)))), 6),
          "By"_("nation"_, "o_year"_), "Sum"_("amount"_)), 6),
      "By"_("nation"_, "o_year"_, "desc"_)), 6);

  // auto partRetailPriceGatherIndices = "List"_("List"_());
  // std::vector<dads::Symbol> partRetailPriceGatherColumns = {"p_retailprice"_};
  // auto partRetailPriceWhere = "Where"_("And"_("Greater"_("p_retailprice"_,
  //                                                        1006.05), // NOLINT
  //                                             "Greater"_(1080.1,   // NOLINT
  //                                                        "p_retailprice"_)));
  // std::vector<dads::Symbol> partFinalGatherColumns = {"p_partkey"_};
  // auto partFinalGather = getGatherSelectGatherWrap(
  //     tableURLs[TPCH_PART][sf], std::move(partRetailPriceGatherIndices),
  //     partRetailPriceGatherColumns, std::move(partRetailPriceWhere),
  //     partFinalGatherColumns, false, false, 0, 1);

  // auto orderGather =
  //     wrapEval("Gather"_(tableURLs[TPCH_ORDERS][sf], RBL_PATH, "List"_("List"_()),
  // 			 "List"_("o_orderkey"_, "o_orderdate"_)), 1);
  // auto nationGather =
  //     wrapEval("Gather"_(tableURLs[TPCH_NATION][sf], RBL_PATH, "List"_("List"_()),
  //               "List"_("n_name"_, "n_nationkey"_)), 1);
  // auto supplierGather =
  //     wrapEval("Gather"_(tableURLs[TPCH_SUPPLIER][sf], RBL_PATH, "List"_("List"_()),
  // 			 "List"_("s_suppkey"_, "s_nationkey"_)), 1);
  // auto partsuppGather =
  //     wrapEval("Gather"_(tableURLs[TPCH_PARTSUPP][sf], RBL_PATH, "List"_("List"_()),
  // 			 "List"_("ps_partkey"_, "ps_suppkey"_, "ps_supplycost"_)), 1);
  // auto lineitemGather =
  //     wrapEval("Gather"_(tableURLs[TPCH_LINEITEM][sf], RBL_PATH, "List"_("List"_()),
  //               "List"_("l_partkey"_, "l_suppkey"_, "l_orderkey"_,
  //                       "l_extendedprice"_, "l_discount"_, "l_quantity"_)), 1);

  // auto nationEncoded = wrapEval("EncodeTable"_(std::move(nationGather)), 1);

  // auto query = wrapEval("Order"_(
  //     wrapEval("Group"_(
  //         wrapEval("Project"_(
  //             wrapEval("Join"_(
  //                 wrapEval("Project"_(std::move(orderGather),
  //                            "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
  //                                  "o_orderdate"_)), 1),
  //                 wrapEval("Project"_(
  //                     wrapEval("Join"_(
  //                         wrapEval("Project"_(
  //                             wrapEval("Join"_(
  //                                 wrapEval("Project"_(std::move(partFinalGather),
  // 						      "As"_("p_partkey"_, "p_partkey"_)), 1),
  //                                 wrapEval("Project"_(
  //                                     wrapEval("Join"_(
  //                                         wrapEval("Project"_(
  //                                             wrapEval("Join"_(
  //                                                 wrapEval("Project"_(
  //                                                     std::move(nationEncoded),
  //                                                     "As"_("n_name"_,
  //                                                           "n_name"_,
  //                                                           "n_nationkey"_,
  //                                                           "n_nationkey"_)), 1),
  //                                                 wrapEval("Project"_(
  //                                                     std::move(supplierGather),
  //                                                     "As"_("s_suppkey"_,
  //                                                           "s_suppkey"_,
  //                                                           "s_nationkey"_,
  //                                                           "s_nationkey"_)), 1),
  //                                                 "Where"_("Equal"_(
  //                                                     "n_nationkey"_,
  //                                                     "s_nationkey"_))), 1),
  //                                             "As"_("n_name"_, "n_name"_,
  //                                                   "s_suppkey"_,
  //                                                   "s_suppkey"_)), 1),
  //                                         wrapEval("Project"_(std::move(partsuppGather),
  //                                                    "As"_("ps_partkey"_,
  //                                                          "ps_partkey"_,
  //                                                          "ps_suppkey"_,
  //                                                          "ps_suppkey"_,
  //                                                          "ps_supplycost"_,
  //                                                          "ps_supplycost"_)), 1),
  //                                         "Where"_("Equal"_("s_suppkey"_,
  //                                                           "ps_suppkey"_))), 1),
  //                                     "As"_("n_name"_, "n_name"_, "ps_partkey"_,
  //                                           "ps_partkey"_, "ps_suppkey"_,
  //                                           "ps_suppkey"_, "ps_supplycost"_,
  //                                           "ps_supplycost"_)), 1),
  //                                 "Where"_(
  // 					   "Equal"_("p_partkey"_, "ps_partkey"_))), 1),
  //                             "As"_("n_name"_, "n_name"_, "ps_partkey"_,
  //                                   "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_,
  //                                   "ps_supplycost"_, "ps_supplycost"_)), 1),
  //                         wrapEval("Project"_(
  //                             std::move(lineitemGather),
  //                             "As"_("l_partkey"_, "l_partkey"_, "l_suppkey"_,
  //                                   "l_suppkey"_, "l_orderkey"_, "l_orderkey"_,
  //                                   "l_extendedprice"_, "l_extendedprice"_,
  //                                   "l_discount"_, "l_discount"_, "l_quantity"_,
  //                                   "l_quantity"_)), 1),
  //                         "Where"_(
  //                             "Equal"_("List"_("ps_partkey"_, "ps_suppkey"_),
  //                                      "List"_("l_partkey"_, "l_suppkey"_)))), 1),
  //                     "As"_("n_name"_, "n_name"_, "ps_supplycost"_,
  //                           "ps_supplycost"_, "l_orderkey"_, "l_orderkey"_,
  //                           "l_extendedprice"_, "l_extendedprice"_,
  //                           "l_discount"_, "l_discount"_, "l_quantity"_,
  //                           "l_quantity"_)), 1),
  //                 "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))), 1),
  //             "As"_("nation"_, "n_name"_, "o_year"_, "Year"_("o_orderdate"_),
  //                   "amount"_,
  //                   "Minus"_("Times"_("l_extendedprice"_,
  //                                     "Minus"_(1.0, "l_discount"_)),
  //                            "Times"_("ps_supplycost"_, "l_quantity"_)))), 1),
  //         "By"_("nation"_, "o_year"_), "Sum"_("amount"_)), 1),
  //     "By"_("nation"_, "o_year"_, "desc"_)), 1);

  return std::move(query);
}

inline dads::Expression TPCH_Q18_DADS_CYCLE(TPCH_SF sf) {

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

inline std::vector<std::function<dads::Expression(TPCH_SF)>>
    dataVaultsQueriesTPCH{TPCH_Q1_DV, TPCH_Q3_DV, TPCH_Q6_DV, TPCH_Q9_DV,
                          TPCH_Q18_DV};
inline std::vector<std::function<dads::Expression(TPCH_SF)>> dadsQueriesTPCH{
    TPCH_Q1_DADS, TPCH_Q3_DADS, TPCH_Q6_DADS, TPCH_Q9_DADS, TPCH_Q18_DADS};

inline std::vector<std::function<dads::Expression(TPCH_SF)>> dadsCycleQueriesTPCH{
    TPCH_Q1_DADS_CYCLE, TPCH_Q3_DADS_CYCLE, TPCH_Q6_DADS_CYCLE, TPCH_Q9_DADS_CYCLE, TPCH_Q18_DADS_CYCLE};
} // namespace dads::benchmarks::LazyLoading::TPCHQueries
