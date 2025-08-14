//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "common/macros.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
{
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_).get();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid()).get();
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  iter_ = tree_->GetBeginIterator();
  
  if (plan_->filter_predicate_ == nullptr)
    return;
  
  for (const auto& key : plan_->pred_keys_) {
    //fmt::println("predkey: {}", key);
    auto const_expr = dynamic_cast<ConstantValueExpression*>(key.get());
    Value& v = const_expr->val_;

    // for point lookup
    tree_->ScanKey(Tuple{{v}, index_info_->index_->GetKeySchema()}, &rids_, exec_ctx_->GetTransaction());
  }
  rids_iter_ = rids_.begin();
}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  
  if (plan_->filter_predicate_ != nullptr) {
    // point lookup
    if (rids_iter_ != rids_.end()) {
      *rid = *rids_iter_;
      const auto& [meta, t] = table_info_->table_->GetTuple(*rid);
      *tuple = t;
      rids_iter_++;
      return true;
    }
    return false;
  }
  if (iter_.IsEnd()) return false;

  const auto& [key, value] = *iter_;
  *rid = value;

  //fmt::println("rid: {}", rid->ToString());
  const auto& [meta, t] = table_info_->table_->GetTuple(*rid);
  *tuple = t;
  ++iter_;

  return true;
}
}  // namespace bustub
