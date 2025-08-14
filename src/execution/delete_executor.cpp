//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , child_executor_(std::move(child_executor))
{
  Init();  
}
/** Initialize the delete */
void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
}

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple The integer tuple indicating the number of rows deleted from the table
 * @param[out] rid The next tuple RID produced by the delete (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    is_end_ = false;
    return false;
  }
  Tuple child_tuple{};
  RID child_rid{};
  int32_t delete_count = 0;
  // Get the next tuple
  while(true) {
    const auto status = child_executor_->Next(&child_tuple, &child_rid);

    if (!status) {
      break;
    }
    TupleMeta meta{0, true};
    table_info_->table_->UpdateTupleMeta(meta, child_rid);

    // update indexes
    const std::string& table_name = table_info_->name_;
    const auto& index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
    for(auto& index_info : index_infos) {
      index_info->index_->DeleteEntry(child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
                                    child_rid, exec_ctx_->GetTransaction());

      //auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get())->GetContainer()->DrawBPlusTree();

      
      //fmt::println("tree: {}", tree);
    }
    delete_count++;
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, delete_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
