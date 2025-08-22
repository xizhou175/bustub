//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/macros.h"

#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_).get();
  inner_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->inner_table_oid_).get();
}

void NestedIndexJoinExecutor::Init() {}

auto NestedIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  fmt::println("NestedIndexLoopJoin");
  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)){
    std::vector<Value> values;
    std::vector<RID> result;
    const Value& value = plan_->key_predicate_->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
    //const auto& key = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info_->key_schema_, index_info_->index_->GetKeyAttrs());
    index_info_->index_->ScanKey({{value}, &index_info_->key_schema_}, &result, exec_ctx_->GetTransaction());
    if (result.size() > 0) {
      // there is a match
      const auto& [tuple_meta, right_tuple] = inner_table_info_->table_->GetTuple(result[0]);
      for (size_t j = 0; j < child_executor_->GetOutputSchema().GetColumnCount(); j++) {
        values.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), j));
      }
      for (size_t j = 0; j < inner_table_info_->schema_.GetColumnCount(); j++) {
        values.push_back(right_tuple.GetValue(&inner_table_info_->schema_, j));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
    else {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        for (size_t j = 0; j < child_executor_->GetOutputSchema().GetColumnCount(); j++) {
          values.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), j));
        }
        for (size_t j = 0; j < inner_table_info_->schema_.GetColumnCount(); j++) {
          values.push_back(ValueFactory::GetNullValueByType(inner_table_info_->schema_.GetColumn(j).GetType()));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
