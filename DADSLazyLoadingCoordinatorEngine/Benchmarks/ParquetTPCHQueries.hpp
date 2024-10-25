#include "config.hpp"
#include <DADS.hpp>
#include <ExpressionUtilities.hpp>
#include <iostream>
#include <limits>
#include "utilities.hpp"

#pragma once

namespace dads::benchmarks::LazyLoading::ParquetTPCHQueries {

using namespace std;
using namespace dads;
using utilities::operator""_; // NOLINT(misc-unused-using-decls) clang-tidy bug
using dads::benchmarks::LazyLoading::utilities::wrapEval;
using dads::benchmarks::LazyLoading::config::paths::RBL_PATH;
using dads::benchmarks::LazyLoading::TPCHQueries::TPCH_SF;
using dads::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_1;
using dads::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_10;
using dads::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_100;
using dads::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_1000;
using dads::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_10000;
using dads::benchmarks::LazyLoading::TPCHQueries::TPCH_SF_20000;

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
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_customer.parquet";
std::string SF_1_LINEITEM_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_lineitem.parquet";
std::string SF_1_NATION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_nation.parquet";
std::string SF_1_ORDERS_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_orders.parquet";
std::string SF_1_PART_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_part.parquet";
std::string SF_1_PARTSUPP_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_partsupp.parquet";
std::string SF_1_REGION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_region.parquet";
std::string SF_1_SUPPLIER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1MB_supplier.parquet";

std::string SF_10_CUSTOMER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_customer.parquet";
std::string SF_10_LINEITEM_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_lineitem.parquet";
std::string SF_10_NATION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_nation.parquet";
std::string SF_10_ORDERS_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_orders.parquet";
std::string SF_10_PART_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_part.parquet";
std::string SF_10_PARTSUPP_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_partsupp.parquet";
std::string SF_10_REGION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_region.parquet";
std::string SF_10_SUPPLIER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10MB_supplier.parquet";

std::string SF_100_CUSTOMER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_customer.parquet";
std::string SF_100_LINEITEM_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_lineitem.parquet";
std::string SF_100_NATION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_nation.parquet";
std::string SF_100_ORDERS_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_orders.parquet";
std::string SF_100_PART_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_part.parquet";
std::string SF_100_PARTSUPP_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_partsupp.parquet";
std::string SF_100_REGION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_region.parquet";
std::string SF_100_SUPPLIER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_100MB_supplier.parquet";

std::string SF_1000_CUSTOMER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_customer.parquet";
std::string SF_1000_LINEITEM_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_lineitem.parquet";
std::string SF_1000_NATION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_nation.parquet";
std::string SF_1000_ORDERS_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_orders.parquet";
std::string SF_1000_PART_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_part.parquet";
std::string SF_1000_PARTSUPP_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_partsupp.parquet";
std::string SF_1000_REGION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_region.parquet";
std::string SF_1000_SUPPLIER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_1000MB_supplier.parquet";

std::string SF_10000_CUSTOMER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_customer.parquet";
std::string SF_10000_LINEITEM_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_lineitem.parquet";
std::string SF_10000_NATION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_nation.parquet";
std::string SF_10000_ORDERS_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_orders.parquet";
std::string SF_10000_PART_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_part.parquet";
std::string SF_10000_PARTSUPP_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_partsupp.parquet";
std::string SF_10000_REGION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_region.parquet";
std::string SF_10000_SUPPLIER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_10000MB_supplier.parquet";

std::string SF_20000_CUSTOMER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_customer.parquet";
std::string SF_20000_LINEITEM_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_lineitem.parquet";
std::string SF_20000_NATION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_nation.parquet";
std::string SF_20000_ORDERS_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_orders.parquet";
std::string SF_20000_PART_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_part.parquet";
std::string SF_20000_PARTSUPP_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_partsupp.parquet";
std::string SF_20000_REGION_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_region.parquet";
std::string SF_20000_SUPPLIER_TABLE_URL =
    "https://www.doc.ic.ac.uk/~dcl19/tpch_20000MB_supplier.parquet";

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

inline dads::Expression createSingleList(std::vector<dads::Symbol> symbols) {
  dads::ExpressionArguments args;
  for (dads::Symbol symbol : symbols) {
    args.push_back(symbol);
  }
  auto list = dads::ComplexExpression{"List"_, {}, std::move(args), {}};
  return std::move(list);
}

inline dads::Expression
createIndicesAsNoRename(std::vector<dads::Symbol> symbols) {
  dads::ExpressionArguments args;
  args.push_back("__internal_indices_"_);
  args.push_back("__internal_indices_"_);
  for (dads::Symbol symbol : symbols) {
    args.push_back(symbol);
    args.push_back(symbol);
  }
  auto as = dads::ComplexExpression{"As"_, {}, std::move(args), {}};
  return std::move(as);
}

inline dads::Expression
getGatherSelectGatherWrap(std::string url, dads::Expression &&gatherIndices1,
                      std::vector<dads::Symbol> gatherColumns1,
                      dads::Expression &&where,
			  std::vector<dads::Symbol> gatherColumns2, bool encode1, bool encode2, int32_t s1, int32_t s2) {
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
  
inline dads::Expression
getSelectGatherWrap(std::string url, dads::Expression &&table,
                      std::vector<dads::Symbol> tableColumns,
                      dads::Expression &&where,
		    std::vector<dads::Symbol> gatherColumns2, bool encode1, bool encode2, int32_t s1, int32_t s2) {
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
  
inline dads::Expression TPCH_Q1_DADS_CYCLE_PARQUET(TPCH_SF sf) {
  auto url = tableURLs[TPCH_LINEITEM][sf];
  auto getParquetColsExpr = wrapEval("GetColumnsFromParquet"_(RBL_PATH, url, "List"_("l_shipdate"_("ALL"_()), "l_returnflag"_("ALL"_()), "l_linestatus"_("ALL"_()), "l_tax"_("ALL"_()), "l_discount"_("ALL"_()), "l_extendedprice"_("ALL"_()), "l_quantity"_("ALL"_()))), 0);
  auto encodedParquetCols = wrapEval("EncodeTable"_(std::move(getParquetColsExpr)), 0);
  
  auto select = 
    wrapEval("Select"_(
		       wrapEval("Project"_(std::move(encodedParquetCols),
			 "As"_("l_quantity"_, "l_quantity"_,
			       "l_discount"_, "l_discount"_,
			       "l_shipdate"_, "l_shipdate"_,
			       "l_extendedprice"_,
			       "l_extendedprice"_,
			       "l_returnflag"_, "l_returnflag"_,
			       "l_linestatus"_, "l_linestatus"_,
			       "l_tax"_, "l_tax"_)), 0),
	      "Where"_("Greater"_("DateObject"_("1998-08-31"),
				  "l_shipdate"_))), 0);

  auto query = wrapEval("Order"_(
      wrapEval("Project"_(
          wrapEval("Group"_(
              wrapEval("Project"_(
                  wrapEval("Project"_(
                      wrapEval("Project"_(std::move(select),
                                 "As"_("l_returnflag"_, "l_returnflag"_,
                                       "l_linestatus"_, "l_linestatus"_,
                                       "l_quantity"_, "l_quantity"_,
                                       "l_extendedprice"_, "l_extendedprice"_,
                                       "l_discount"_, "l_discount"_, "calc1"_,
                                       "Minus"_(1.0, "l_discount"_), "calc2"_,
                                       "Plus"_("l_tax"_, 1.0))), 0),
                      "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                            "l_linestatus"_, "l_quantity"_, "l_quantity"_,
                            "l_extendedprice"_, "l_extendedprice"_,
                            "l_discount"_, "l_discount"_, "disc_price"_,
                            "Times"_("l_extendedprice"_, "calc1"_), "calc2"_,
                            "calc2"_)), 0),
                  "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                        "l_linestatus"_, "l_quantity"_, "l_quantity"_,
                        "l_extendedprice"_, "l_extendedprice"_, "l_discount"_,
                        "l_discount"_, "disc_price"_, "disc_price"_, "calc"_,
                        "Times"_("disc_price"_, "calc2"_))), 0),
              "By"_("l_returnflag"_, "l_linestatus"_),
              "As"_("sum_qty"_, "Sum"_("l_quantity"_), "sum_base_price"_,
                    "Sum"_("l_extendedprice"_), "sum_disc_price"_,
                    "Sum"_("disc_price"_), "sum_charges"_, "Sum"_("calc"_),
                    "sum_disc"_, "Sum"_("l_discount"_), "count_order"_,
                    "Count"_("*"_))), 0),
          "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                "l_linestatus"_, "sum_qty"_, "sum_qty"_, "sum_base_price"_,
                "sum_base_price"_, "sum_disc_price"_, "sum_disc_price"_,
                "sum_charges"_, "sum_charges"_, "avg_qty"_,
                "Divide"_("sum_qty"_, "count_order"_), "avg_price"_,
                "Divide"_("sum_base_price"_, "count_order"_), "avg_disc"_,
                "Divide"_("sum_disc"_, "count_order"_), "count_order"_,
                "count_order"_)), 0),
      "By"_("l_returnflag"_, "l_linestatus"_)), 0);

  return std::move(query);
}

inline dads::Expression TPCH_Q3_DADS_CYCLE_PARQUET(TPCH_SF sf) {
   auto customerUrl = tableURLs[TPCH_CUSTOMER][sf];
   auto customerParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, customerUrl, "List"_("c_mktsegment"_("ALL"_()), "c_custkey"_("ALL"_()))), 0);
   auto encodeCustomerParquetCols = wrapEval("EncodeTable"_(std::move(customerParquetCols)), 0);
   
   auto ordersUrl = tableURLs[TPCH_ORDERS][sf];
   auto ordersParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, ordersUrl, "List"_("o_orderkey"_("ALL"_()), "o_custkey"_("ALL"_()), "o_orderdate"_("ALL"_()), "o_shippriority"_("ALL"_()))), 0);
   
   auto lineitemUrl = tableURLs[TPCH_LINEITEM][sf];
   auto lineitemParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, lineitemUrl, "List"_("l_shipdate"_("ALL"_()), "l_orderkey"_("ALL"_()), "l_extendedprice"_("ALL"_()), "l_discount"_("ALL"_()))), 0);

   auto orderSelect =
     wrapEval("Select"_(
			wrapEval("Project"_(std::move(ordersParquetCols),
					    "As"_("o_orderkey"_, "o_orderkey"_,
						  "o_orderdate"_, "o_orderdate"_,
						  "o_custkey"_, "o_custkey"_,
						  "o_shippriority"_,
						  "o_shippriority"_)), 0),
			"Where"_("Greater"_("DateObject"_("1995-03-15"),
					    "o_orderdate"_))), 0);

   auto customerSelect =
     wrapEval("Project"_(
			 wrapEval("Select"_(
					    wrapEval("Project"_(std::move(encodeCustomerParquetCols),
								"As"_("c_custkey"_, "c_custkey"_,
								      "c_mktsegment"_,
								      "c_mktsegment"_)), 0),
					    "Where"_("Equal"_("c_mktsegment"_,
							      "GetEncodingFor"_("BUILDING", "c_mktsegment"_)))), 0),
			 "As"_("c_custkey"_, "c_custkey"_, "c_mktsegment"_,
			       "c_mktsegment"_)), 0);

   auto lineitemSelect =
     wrapEval("Project"_(
			 wrapEval("Select"_(
					    wrapEval("Project"_(
								std::move(lineitemParquetCols),
								"As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_,
								      "l_discount"_, "l_shipdate"_, "l_shipdate"_,
								      "l_extendedprice"_, "l_extendedprice"_)), 0),
						     "Where"_("Greater"_("l_shipdate"_,
									 "DateObject"_("1993-03-15")))), 0),
					    "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_,
						  "l_discount"_, "l_extendedprice"_,
						  "l_extendedprice"_)), 0);
   
  auto ordersCustomerJoin = wrapEval("Project"_(
      wrapEval("Join"_(std::move(orderSelect), std::move(customerSelect),
		       "Where"_("Equal"_("o_custkey"_, "c_custkey"_))), 0),
      "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_,
            "o_shippriority"_, "o_shippriority"_)), 0);

  auto ordersLineitemJoin = wrapEval("Project"_(
      wrapEval("Join"_(std::move(ordersCustomerJoin), std::move(lineitemSelect),
		       "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))), 0),
      "As"_("l_orderkey"_, "l_orderkey"_, "l_extendedprice"_,
            "l_extendedprice"_, "l_discount"_, "l_discount"_, "o_orderdate"_,
            "o_orderdate"_, "o_shippriority"_, "o_shippriority"_)), 0);
  
  auto query =
      wrapEval("Top"_(wrapEval("Group"_(wrapEval("Project"_(std::move(ordersLineitemJoin),
                                 "As"_("expr1009"_,
                                       "Times"_("l_extendedprice"_,
                                                "Minus"_(1.0, "l_discount"_)),
                                       "l_extendedprice"_, "l_extendedprice"_,
                                       "l_orderkey"_, "l_orderkey"_,
                                       "o_orderdate"_, "o_orderdate"_,
                                       "o_shippriority"_, "o_shippriority"_)), 0),
                      "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
					"As"_("revenue"_, "Sum"_("expr1009"_))), 0),
		      "By"_("revenue"_, "desc"_, "o_orderdate"_), 10), 0);

  return std::move(query);
}

inline dads::Expression TPCH_Q6_DADS_CYCLE_PARQUET(TPCH_SF sf) {
  auto url = tableURLs[TPCH_LINEITEM][sf];
  auto getParquetColsExpr = wrapEval("GetColumnsFromParquet"_(RBL_PATH, url, "List"_("l_shipdate"_("ALL"_()), "l_discount"_("List"_((double_t) 0.0499, (double_t) 0.07001)), "l_extendedprice"_("ALL"_()), "l_quantity"_("List"_(std::numeric_limits<double_t>::min(), (double_t) 23)))), 0);
  
  auto query = wrapEval("Group"_(
      wrapEval("Project"_(
          wrapEval("Select"_(wrapEval("Project"_(std::move(getParquetColsExpr),
                               "As"_("l_discount"_, "l_discount"_,
                                     "l_extendedprice"_, "l_extendedprice"_,
                                     "l_quantity"_, "l_quantity"_,
                                     "l_shipdate"_, "l_shipdate"_)), 0),
                    "Where"_("And"_("Greater"_("l_discount"_, 0.0499), // NOLINT
                                    "Greater"_(0.07001, "l_discount"_), // NOLINT
                                    "Greater"_((double_t) 24, "l_quantity"_), // NOLINT
				    "Greater"_("DateObject"_("1995-01-01"), "l_shipdate"_), // NOLINT
					       "Greater"_("l_shipdate", "DateObject"_("1993-12-31"))))), 0),
          "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))), 0),
      "Sum"_("revenue"_)), 0);

  return std::move(query);
}

inline dads::Expression TPCH_Q9_DADS_CYCLE_PARQUET(TPCH_SF sf) {

  auto partUrl = tableURLs[TPCH_PART][sf];
  auto partParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, partUrl, "List"_("p_partkey"_("ALL"_()), "p_retailprice"_("List"_((double_t) 1006.05, (double_t) 1080.1)))), 0);
  
  auto orderUrl = tableURLs[TPCH_ORDERS][sf];
  auto orderParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, orderUrl, "List"_("o_orderkey"_("ALL"_()), "o_orderdate"_("ALL"_()))), 0);
  
  auto nationUrl = tableURLs[TPCH_NATION][sf];
  auto nationParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, nationUrl, "List"_("n_nationkey"_("ALL"_()), "n_name"_("ALL"_()))), 0);
  
  auto supplierUrl = tableURLs[TPCH_SUPPLIER][sf];
  auto supplierParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, supplierUrl, "List"_("s_suppkey"_("ALL"_()), "s_nationkey"_("ALL"_()))), 0);
  
  auto partsuppUrl = tableURLs[TPCH_PARTSUPP][sf];
  auto partsuppParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, partsuppUrl, "List"_("ps_suppkey"_("ALL"_()), "ps_partkey"_("ALL"_()), "ps_supplycost"_("ALL"_()))), 0);
  
  auto lineitemUrl = tableURLs[TPCH_LINEITEM][sf];
  auto lineitemParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, lineitemUrl, "List"_("l_suppkey"_("ALL"_()), "l_partkey"_("ALL"_()), "l_quantity"_("ALL"_()), "l_orderkey"_("ALL"_()), "l_extendedprice"_("ALL"_()), "l_discount"_("ALL"_()))), 0);

  auto nationEncoded = wrapEval("EncodeTable"_(std::move(nationParquetCols)), 0);

  auto query = wrapEval("Order"_(
      wrapEval("Group"_(
          wrapEval("Project"_(
              wrapEval("Join"_(
                  wrapEval("Project"_(std::move(orderParquetCols),
                             "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                   "o_orderdate"_)), 0),
                  wrapEval("Project"_(
                      wrapEval("Join"_(
                          wrapEval("Project"_(
                              wrapEval("Join"_(
                                  wrapEval("Project"_(std::move(partParquetCols),
						      "As"_("p_partkey"_, "p_partkey"_)), 0),
                                  wrapEval("Project"_(
                                      wrapEval("Join"_(
                                          wrapEval("Project"_(
                                              wrapEval("Join"_(
                                                  wrapEval("Project"_(
                                                      std::move(nationEncoded),
                                                      "As"_("n_name"_,
                                                            "n_name"_,
                                                            "n_nationkey"_,
                                                            "n_nationkey"_)), 0),
                                                  wrapEval("Project"_(
                                                      std::move(supplierParquetCols),
                                                      "As"_("s_suppkey"_,
                                                            "s_suppkey"_,
                                                            "s_nationkey"_,
                                                            "s_nationkey"_)), 0),
                                                  "Where"_("Equal"_(
                                                      "n_nationkey"_,
                                                      "s_nationkey"_))), 0),
                                              "As"_("n_name"_, "n_name"_,
                                                    "s_suppkey"_,
                                                    "s_suppkey"_)), 0),
                                          wrapEval("Project"_(std::move(partsuppParquetCols),
                                                     "As"_("ps_partkey"_,
                                                           "ps_partkey"_,
                                                           "ps_suppkey"_,
                                                           "ps_suppkey"_,
                                                           "ps_supplycost"_,
                                                           "ps_supplycost"_)), 0),
                                          "Where"_("Equal"_("s_suppkey"_,
                                                            "ps_suppkey"_))), 0),
                                      "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                            "ps_partkey"_, "ps_suppkey"_,
                                            "ps_suppkey"_, "ps_supplycost"_,
                                            "ps_supplycost"_)), 0),
                                  "Where"_(
					   "Equal"_("p_partkey"_, "ps_partkey"_))), 0),
                              "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                    "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_,
                                    "ps_supplycost"_, "ps_supplycost"_)), 0),
                          wrapEval("Project"_(
                              std::move(lineitemParquetCols),
                              "As"_("l_partkey"_, "l_partkey"_, "l_suppkey"_,
                                    "l_suppkey"_, "l_orderkey"_, "l_orderkey"_,
                                    "l_extendedprice"_, "l_extendedprice"_,
                                    "l_discount"_, "l_discount"_, "l_quantity"_,
                                    "l_quantity"_)), 0),
                          "Where"_(
                              "Equal"_("List"_("ps_partkey"_, "ps_suppkey"_),
                                       "List"_("l_partkey"_, "l_suppkey"_)))), 0),
                      "As"_("n_name"_, "n_name"_, "ps_supplycost"_,
                            "ps_supplycost"_, "l_orderkey"_, "l_orderkey"_,
                            "l_extendedprice"_, "l_extendedprice"_,
                            "l_discount"_, "l_discount"_, "l_quantity"_,
                            "l_quantity"_)), 0),
                  "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))), 0),
              "As"_("nation"_, "n_name"_, "o_year"_, "Year"_("o_orderdate"_),
                    "amount"_,
                    "Minus"_("Times"_("l_extendedprice"_,
                                      "Minus"_(1.0, "l_discount"_)),
                             "Times"_("ps_supplycost"_, "l_quantity"_)))), 0),
          "By"_("nation"_, "o_year"_), "Sum"_("amount"_)), 0),
      "By"_("nation"_, "o_year"_, "desc"_)), 0);

  return std::move(query);
}

inline dads::Expression TPCH_Q18_DADS_CYCLE_PARQUET(TPCH_SF sf) {
  
  auto customerUrl = tableURLs[TPCH_CUSTOMER][sf];
  auto customerParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, customerUrl, "List"_("c_custkey"_("ALL"_()))), 0);
  
  auto orderUrl = tableURLs[TPCH_ORDERS][sf];
  auto orderParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, orderUrl, "List"_("o_orderkey"_("ALL"_()), "o_orderdate"_("ALL"_()), "o_totalprice"_("ALL"_()), "o_custkey"_("ALL"_()))), 0);

  auto lineitemUrl = tableURLs[TPCH_LINEITEM][sf];
  auto lineitemParquetCols = wrapEval("GetColumnsFromParquet"_(RBL_PATH, lineitemUrl, "List"_("l_quantity"_("ALL"_()), "l_orderkey"_("ALL"_()))), 0);

  auto query = wrapEval("Top"_(
      wrapEval("Group"_(
          wrapEval("Project"_(
              wrapEval("Join"_(
                  wrapEval("Select"_(
                      wrapEval("Group"_(wrapEval("Project"_(std::move(lineitemParquetCols),
                                          "As"_("l_orderkey"_, "l_orderkey"_,
                                                "l_quantity"_, "l_quantity"_)), 0),
					"By"_("l_orderkey"_),
					"As"_("sum_l_quantity"_, "Sum"_("l_quantity"_))), 0),
                      "Where"_("Greater"_("sum_l_quantity"_, (double_t) 300))), 0), // NOLINT
                  wrapEval("Project"_(
                      wrapEval("Join"_(
                          wrapEval("Project"_(std::move(customerParquetCols),
					      "As"_("c_custkey"_, "c_custkey"_)), 0),
                          wrapEval("Project"_(std::move(orderParquetCols),
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
  
inline std::vector<std::function<dads::Expression(TPCH_SF)>> dadsParquetCycleQueriesTPCH{
    TPCH_Q1_DADS_CYCLE_PARQUET, TPCH_Q3_DADS_CYCLE_PARQUET, TPCH_Q6_DADS_CYCLE_PARQUET, TPCH_Q9_DADS_CYCLE_PARQUET, TPCH_Q18_DADS_CYCLE_PARQUET};
} // namespace dads::benchmarks::LazyLoading::ParquetTPCHQueries
