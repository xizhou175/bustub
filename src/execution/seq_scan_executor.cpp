//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include <optional>
#include <utility>
#include "common/macros.h"
#include "storage/table/table_iterator.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
  : AbstractExecutor(exec_ctx)
  , plan_(plan)
{
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
  iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {}

/**
 * Yield the next tuple from the sequential scan.
 * @param[out] tuple The next tuple produced by the scan
 * @param[out] rid The next tuple RID produced by the scan
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TupleMeta meta{0, false};
  do{
    if (iter_->IsEnd()) {
      return false;
    }
    const auto& [m, t] = iter_->GetTuple();
    *tuple = std::move(t);
    meta = std::move(m);
    *rid = t.GetRid();
    ++(*iter_);
  }while(meta.is_deleted_ == true || (plan_->filter_predicate_ && !plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_).GetAs<bool>()));
  return true;
}

}  // namespace bustub
