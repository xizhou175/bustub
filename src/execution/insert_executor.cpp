//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "fmt/base.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , child_executor_(std::move(child_executor))
    , is_end_(false)
{
  //Init();
}

/** Initialize the insert */
void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_end_ == true){
    is_end_ = false;
    return false;
  } 
  Tuple child_tuple{};
  RID child_rid{};
  int32_t insert_count = 0;
  // Get the next tuple
  while(true) {
    
    const auto status = child_executor_->Next(&child_tuple, &child_rid);
    //fmt::println("tuple: {} rid: {}", child_tuple.ToString(&table_info_->schema_), child_rid.ToString());

    if (!status) {
      break;
    }
    TupleMeta meta{0, false};
    auto rid = table_info_->table_->InsertTuple(meta, child_tuple);

    // update indexes
    const std::string& table_name = table_info_->name_;
    const auto& index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
    for(auto& index_info : index_infos) {
      index_info->index_->InsertEntry(child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
                                    *rid, exec_ctx_->GetTransaction());

      //auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get())->GetContainer()->DrawBPlusTree();

      
      //fmt::println("tree: {}", tree);
    }
    insert_count++;
    
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, insert_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  
  return true;
}

}  // namespace bustub
