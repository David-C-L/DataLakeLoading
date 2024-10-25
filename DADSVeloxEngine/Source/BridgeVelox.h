#pragma once

#include "DadsConnector.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include <DADS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace dads::engines::velox {
std::string const kDadsConnectorId = "dads";
using ExpressionSpanArgument = dads::expressions::ExpressionSpanArgument;

  enum DadsType { bINTEGER = 0, bCHAR, bBIGINT, bREAL, bDOUBLE };

struct DadsArray {
  DadsArray(int64_t span_size, void const* span_begin, ExpressionSpanArgument&& span)
      : length(span_size), buffers(span_begin), holdSpan(std::move(span)) {}

  DadsArray(DadsArray&& other) noexcept
      : length(other.length), buffers(other.buffers), holdSpan(std::move(other.holdSpan)) {}

  DadsArray& operator=(DadsArray&& other) {
    length = other.length;
    buffers = other.buffers;
    holdSpan = std::move(other.holdSpan);
    return *this;
  }

  DadsArray(DadsArray const& dadsArray) = delete;
  DadsArray& operator=(DadsArray const& other) = delete;

  ~DadsArray() = default;

  ExpressionSpanArgument holdSpan;
  int64_t length;
  void const* buffers;
};

VectorPtr importFromDadsAsOwner(DadsType dadsType, DadsArray&& dadsArray, memory::MemoryPool* pool);

BufferPtr importFromDadsAsOwnerBuffer(DadsArray&& dadsArray);

std::vector<RowVectorPtr>
veloxRunQueryParallel(CursorParameters const& params, std::unique_ptr<TaskCursor>& cursor,
                      std::vector<std::pair<core::PlanNodeId, size_t>> const& scanIds);

void veloxPrintResults(std::vector<RowVectorPtr> const& results);

RowVectorPtr makeRowVectorNoCopy(std::shared_ptr<RowType const>& schema,
                                 std::vector<VectorPtr>&& children, memory::MemoryPool* pool);

} // namespace dads::engines::velox

template <> struct fmt::formatter<dads::engines::velox::DadsType> : formatter<string_view> {
  auto format(dads::engines::velox::DadsType c, format_context& ctx) const;
};
