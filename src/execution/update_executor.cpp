//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "type/type_id.h"

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , child_executor_(std::move(child_executor))
{
  Init();  
}

/** Initialize the update */
void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
}

/**
 * Yield the next tuple from the update.
 * @param[out] tuple The next tuple produced by the update
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_ == true){
    is_end_ = false;
    return false;
  }
  Tuple child_tuple{};
  RID child_rid{};
  int32_t update_count = 0;

  while(true) {
    // Get the next tuple
    const auto status = child_executor_->Next(&child_tuple, &child_rid);

    if (!status) {
      break;
    }

    std::vector<Value> values;
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());

    for(auto& expr : plan_->target_expressions_) {
      const auto& value = expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
      values.push_back(value);
    }

    Tuple new_t{values, &child_executor_->GetOutputSchema()};

    //TupleMeta old_meta{0, true};
    //table_info_->table_->UpdateTupleMeta(old_meta, *rid);

    TupleMeta new_meta{0, false};
    table_info_->table_->UpdateTupleInPlace(new_meta, new_t, child_rid);
  
    // update indexes
    const std::string& table_name = table_info_->name_;
    const auto& index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
    for(auto& index_info : index_infos) {
      const auto& old_key = child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());

      const auto& new_key = new_t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(new_key, child_rid, exec_ctx_->GetTransaction());
    }
    update_count++;
  }

  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  values.push_back(Value(TypeId::INTEGER, update_count));
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
