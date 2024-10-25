#include "DadsConnector.h"

// #ifdef DebugInfo
#include <iostream>
// #endif // DebugInfo

namespace dads::engines::velox {

std::string DadsTableHandle::toString() const {
  return fmt::format("table: {}, spanRowCount size: {}", tableName_, spanRowCountVec_.size());
}

DadsDataSource::DadsDataSource(
    std::shared_ptr<RowType const> const& outputType,
    std::shared_ptr<connector::ConnectorTableHandle> const& tableHandle,
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>> const& columnHandles,
    memory::MemoryPool* FOLLY_NONNULL pool)
    : pool_(pool), outputType_(outputType),
      dadsTableHandle_(std::dynamic_pointer_cast<DadsTableHandle>(tableHandle)),
      dadsTableName_(dadsTableHandle_->getTable()),
      dadsRowDataVec_(dadsTableHandle_->getRowDataVec()),
      dadsSpanRowCountVec_(dadsTableHandle_->getSpanRowCountVec()) {
  VELOX_CHECK_NOT_NULL(dadsTableHandle_, "TableHandle must be an instance of DadsTableHandle");

  auto const& dadsTableSchema = dadsTableHandle_->getTableSchema();
  VELOX_CHECK_NOT_NULL(dadsTableSchema, "DadsSchema can't be null.");

  outputColumnMappings_.reserve(outputType->size());

  for(auto const& outputName : outputType->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(it != columnHandles.end(),
                "ColumnHandle is missing for output column '{}' on table '{}'", outputName,
                dadsTableName_);

    auto handle = std::dynamic_pointer_cast<DadsColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(handle,
                         "ColumnHandle must be an instance of DadsColumnHandle "
                         "for '{}' on table '{}'",
                         handle->name(), dadsTableName_);

    auto idx = dadsTableSchema->getChildIdxIfExists(handle->name());
    VELOX_CHECK(idx != std::nullopt, "Column '{}' not found on TPC-H table '{}'.", handle->name(),
                dadsTableName_);
    outputColumnMappings_.emplace_back(*idx);
  }
}

void DadsDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_NULL(currentSplit_,
                   "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<DadsConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(currentSplit_, "Wrong type of split for DadsDataSource.");

  spanCountIdx_ = currentSplit_->partNumber;
  splitOffset_ = 0;
  splitEnd_ = dadsSpanRowCountVec_.at(spanCountIdx_) - splitOffset_;

#ifdef DebugInfo
  std::cout << "dadsSpanRowCountVec_.size() " << dadsSpanRowCountVec_.size() << std::endl;
  std::cout << "spanCountIdx_ " << spanCountIdx_ << std::endl;
  std::cout << "splitOffset_ " << splitOffset_ << std::endl;
  std::cout << "splitEnd_ " << splitEnd_ << std::endl;
#endif // DebugInfo
}

RowVectorPtr DadsDataSource::getDadsData(uint64_t length) {
#ifdef DebugInfo
  std::cout << "getDadsData: spanCountIdx_=" << spanCountIdx_ << " splitOffset_=" << splitOffset_
            << " length=" << length << std::endl;
#endif
  assert(splitOffset_ <= INT_MAX);
  assert(length <= INT_MAX);

  std::vector<VectorPtr> children;
  children.reserve(outputColumnMappings_.size());
  for(auto channelIdx : outputColumnMappings_) {
    children.emplace_back(
        dadsRowDataVec_.at(spanCountIdx_)->childAt(channelIdx)->slice(splitOffset_, length));
  }

  return std::make_shared<RowVector>(pool_, outputType_, BufferPtr(nullptr), length,
                                     std::move(children));
}

std::optional<RowVectorPtr> DadsDataSource::next(uint64_t size, ContinueFuture& /*future*/) {
  VELOX_CHECK_NOT_NULL(currentSplit_, "No split to process. Call addSplit() first.");

  auto maxRows = std::min(size, (splitEnd_ - splitOffset_));
  auto outputVector = getDadsData(maxRows);

  // If the split is exhausted.
  if(!outputVector || outputVector->size() == 0) {
    currentSplit_ = nullptr;
    return nullptr;
  }

  splitOffset_ += maxRows;
  completedRows_ += outputVector->size();
  completedBytes_ += outputVector->retainedSize();

  return std::move(outputVector);
}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<DadsConnectorFactory>())

} // namespace dads::engines::velox
