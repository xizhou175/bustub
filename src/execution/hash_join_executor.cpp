//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/macros.h"
#include "execution/plans/hash_join_plan.h"
#include "fmt/base.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , left_executor_(std::move(left_child))
    , right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  for (const auto& expr : plan_->LeftJoinKeyExpressions()) {
    fmt::println("left: {}", expr->ToString());
  }
  for (const auto& expr : plan_->RightJoinKeyExpressions()) {
    fmt::println("right: {}", expr->ToString());
  }
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple{};
  RID rid{};
  int i = 0;
  while (right_executor_->Next(&tuple, &rid)) {
    const auto& key = MakeJoinKey(&tuple, right_executor_.get(), plan_->RightJoinKeyExpressions());
    if (ht_.count(key) == 0) {
      ht_.insert({key, {}});
    }
    right_tuples_.push_back(std::move(tuple));
    ht_[key].push_back(i);
    i++;
  }
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join.
 * @param[out] rid The next tuple RID, not used by hash join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_key.has_value()){
    cur_idx++;
    
    if (cur_idx < ht_[*cur_key].size()) {
      std::vector<Value> values;
      const auto& right_tuple = right_tuples_[ht_[*cur_key][cur_idx]];
      for (size_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j++) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), j));
      }
      for (size_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
        values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), j));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
    else {
      cur_key = std::nullopt;
      cur_idx = 0;
    }
  }
  while (left_executor_->Next(&left_tuple_, rid)){
    const auto& key = MakeJoinKey(&left_tuple_, left_executor_.get(), plan_->LeftJoinKeyExpressions());
    if (ht_.count(key) == 0) {
      if (plan_->join_type_ == JoinType::LEFT) {
        std::vector<Value> values;
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
      std::vector<Value> values;
      const auto& idx_v = ht_[key];
      
      const auto& right_tuple = right_tuples_[idx_v[0]];
      for (size_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j++) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), j));
      }
      for (size_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
        values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), j));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      cur_key = key;
      return true;
    }
  }
  return false;
}

/** @return The tuple as an AggregateKey */
auto HashJoinExecutor::MakeJoinKey(const Tuple *tuple, const AbstractExecutor* executor, const std::vector<AbstractExpressionRef> &exprs) -> JoinKey {
  std::vector<Value> keys;
  for (const auto &expr : exprs) {
    keys.emplace_back(expr->Evaluate(tuple, executor->GetOutputSchema()));
  }
  
  return {keys};
}

}  // namespace bustub
