//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <optional>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"

#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , left_executor_(std::move(left_executor))
    , right_executor_(std::move(right_executor))
{
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join
 * @param[out] rid The next tuple RID produced, not used by nested loop join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  
  fmt::println("NestedLoopJoin");
  while (right_idx_.has_value() || left_executor_->Next(&left_tuple_, rid)){
    std::vector<Value> values;
    values.reserve(left_executor_->GetOutputSchema().GetColumnCount() + right_executor_->GetOutputSchema().GetColumnCount());
    //fmt::println("left tuple: {}", left_tuple_.ToString(&left_executor_->GetOutputSchema()));
    if (right_idx_.has_value() == false){
      right_executor_->Init();
    }
    for (size_t i = right_idx_.has_value() ? *right_idx_: 0; i < right_tuples_.size(); i++) {
      const auto& right_tuple = right_tuples_[i];
      if (matched(&left_tuple_, &right_tuple)) {
        for (size_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j++) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), j));
        }
        for (size_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), j));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        right_idx_ = i + 1;
        return true;
      }
    }
    if (right_idx_.has_value() == false) {
      
      if (plan_->GetJoinType() == JoinType::LEFT) {
        for (size_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j++) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), j));
        }
        for (size_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
          values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(j).GetType()));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        return true;
      }
    }
    else {
      right_idx_ = std::nullopt;
    }
  }
  return false;
}

bool NestedLoopJoinExecutor::matched(const Tuple* left_tuple, const Tuple* right_tuple) {
  return plan_->Predicate()->EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple, right_executor_->GetOutputSchema()).GetAs<bool>();
}

}  // namespace bustub
