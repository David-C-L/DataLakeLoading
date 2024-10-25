// Data structures and functions in this file are referenced/copied from Velox prototype for Arrow
// Velox conversion.

#include "BridgeVelox.h"

#include <utility>

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace dads::engines::velox {

// Optionally, holds shared_ptrs pointing to the DadsArray object that
// holds the buffer object that describes the DadsArray,
// which will be released to signal that we will no longer hold on to the data
// and the shared_ptr deleters should run the release procedures if no one
// else is referencing the objects.
struct BufferViewReleaser {

  explicit BufferViewReleaser(std::shared_ptr<DadsArray>&& dadsArray)
      : arrayReleaser_(std::move(dadsArray)) {}

  void addRef() const {}

  void release() const {}

private:
  std::shared_ptr<DadsArray> arrayReleaser_;
};

// Wraps a naked pointer using a Velox buffer view, without copying it. This
// buffer view uses shared_ptr to manage reference counting and releasing for
// the DadsArray object
BufferPtr wrapInBufferViewAsOwner(const void* buffer, size_t length,
                                  std::shared_ptr<DadsArray>&& arrayReleaser) {
  return BufferView<BufferViewReleaser>::create(static_cast<const uint8_t*>(buffer), length,
                                                {BufferViewReleaser(std::move(arrayReleaser))});
}

// Dispatch based on the type.
template <TypeKind kind>
VectorPtr createFlatVector(memory::MemoryPool* pool, TypePtr const& type, BufferPtr nulls,
                           size_t length, BufferPtr values) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_shared<FlatVector<T>>(pool, type, nulls, length, values,
                                         std::vector<BufferPtr>(), SimpleVectorStats<T>{},
                                         std::nullopt, std::nullopt);
}

TypePtr importFromDadsType(DadsType dadsType) {
  switch(dadsType) {
  case dads::engines::velox::bINTEGER:
    return INTEGER();
  case dads::engines::velox::bCHAR:
    return TINYINT();
  case dads::engines::velox::bBIGINT:
    return BIGINT();
  case dads::engines::velox::bREAL:
    return REAL();
  case dads::engines::velox::bDOUBLE:
    return DOUBLE();
  }
  VELOX_USER_FAIL("Unable to convert '{}' DadsType format type to Velox.", dadsType)
}

VectorPtr importFromDadsAsOwner(DadsType dadsType, DadsArray&& dadsArray,
                                memory::MemoryPool* pool) {
  VELOX_CHECK_GE(dadsArray.length, 0, "Array length needs to be non-negative.")

  // First parse and generate a Velox type.
  auto type = importFromDadsType(dadsType);

  // Wrap the nulls buffer into a Velox BufferView (zero-copy). Null buffer size
  // needs to be at least one bit per element.
  BufferPtr nulls = nullptr;

  // Other primitive types.
  VELOX_CHECK(type->isPrimitiveType(), "Conversion of '{}' from Dads not supported yet.",
              type->toString())

  // Wrap the values buffer into a Velox BufferView - zero-copy.
  const auto* buffer = dadsArray.buffers;
  auto length = dadsArray.length * type->cppSizeInBytes();
  auto arrayReleaser = std::make_shared<DadsArray>(std::move(dadsArray));
  auto values = wrapInBufferViewAsOwner(buffer, length, std::move(arrayReleaser));

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(createFlatVector, type->kind(), pool, type, nulls,
                                            dadsArray.length, values);
}

BufferPtr importFromDadsAsOwnerBuffer(DadsArray&& dadsArray) {
  VELOX_CHECK_GE(dadsArray.length, 0, "Array length needs to be non-negative.")

  // Wrap the values buffer into a Velox BufferView - zero-copy.
  const auto* buffer = dadsArray.buffers;
  auto length = dadsArray.length * sizeof(int32_t); // assuming always int32 type!
  auto arrayReleaser = std::make_shared<DadsArray>(std::move(dadsArray));
  return wrapInBufferViewAsOwner(buffer, length, std::move(arrayReleaser));
}

std::vector<RowVectorPtr> myReadCursor(CursorParameters const& params,
                                       std::unique_ptr<TaskCursor>& cursor,
                                       std::function<void(exec::Task*)> addSplits) {
  cursor = TaskCursor::create(params);
  // 'result' borrows memory from cursor so the life cycle must be shorter.
  std::vector<RowVectorPtr> result;
  auto* task = cursor->task().get();
  addSplits(task);

  while(cursor->moveNext()) {
    // std::cout << "result for id:" << std::this_thread::get_id() << std::endl;
    result.push_back(cursor->current());
    addSplits(task);
  }

  return std::move(result);
}

std::vector<RowVectorPtr>
veloxRunQueryParallel(CursorParameters const& params, std::unique_ptr<TaskCursor>& cursor,
                      std::vector<std::pair<core::PlanNodeId, size_t>> const& scanIds) {
  try {
    bool noMoreSplits = false;
    auto addSplits = [&](exec::Task* task) {
      if(!noMoreSplits) {
        for(auto const& [scanId, numSpans] : scanIds) {
          for(size_t i = 0; i < numSpans; ++i) {
            task->addSplit(scanId, exec::Split(std::make_shared<DadsConnectorSplit>(
                                       kDadsConnectorId, numSpans, i)));
          }
          task->noMoreSplits(scanId);
        }
      }
      noMoreSplits = true;
    };
    auto result = myReadCursor(params, cursor, addSplits);
    return result;
  } catch(std::exception const& e) {
    LOG(ERROR) << "Query terminated with: " << e.what();
    return {};
  }
}

RowVectorPtr makeRowVectorNoCopy(std::shared_ptr<RowType const>& schema,
                                 std::vector<VectorPtr>&& children, memory::MemoryPool* pool) {
  size_t const vectorSize = children.empty() ? 0 : children.front()->size();
  return std::make_shared<RowVector>(pool, schema, BufferPtr(nullptr), vectorSize,
                                     std::move(children));
}

} // namespace dads::engines::velox

auto fmt::formatter<dads::engines::velox::DadsType>::format(dads::engines::velox::DadsType type,
                                                            format_context& ctx) const {
  string_view name = "unknown";
  switch(type) {
  case dads::engines::velox::bINTEGER:
    name = "bInteger";
    break;
  case dads::engines::velox::bCHAR:
    name = "bCHAR";
    break;
  case dads::engines::velox::bBIGINT:
    name = "bBIGINT";
    break;
  case dads::engines::velox::bREAL:
    name = "bREAL";
    break;
  case dads::engines::velox::bDOUBLE:
    name = "bDOUBLE";
    break;
  }
  return formatter<string_view>::format(name, ctx);
}
