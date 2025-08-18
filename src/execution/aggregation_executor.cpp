//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "execution/plans/aggregation_plan.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , child_executor_(std::move(child_executor))
    , aht_(plan_->GetAggregates(), plan_->agg_types_)
    , aht_iterator_(aht_.Begin())
{

}

/** Initialize the aggregation */
void AggregationExecutor::Init() {  }

/**
 * Yield the next tuple from the insert.
 * @param[out] tuple The next tuple produced by the aggregation
 * @param[out] rid The next tuple RID produced by the aggregation
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_end_ == true) {
    if (aht_iterator_ == aht_.End()) {
      return false;
    }
    std::vector<Value> values;
  
    values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
    values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
    *tuple = Tuple{values, &GetOutputSchema()};
    ++aht_iterator_;
    return true;
  }

  Tuple child_tuple{};
  RID child_rid{};
  
  while (true) {
    auto status = child_executor_->Next(&child_tuple, &child_rid);
    if (!status) {
      break;
    }
    
    //fmt::println("{}", child_tuple.ToString(&child_executor_->GetOutputSchema()));

    AggregateKey aggregate_key = MakeAggregateKey(&child_tuple);
    const auto& aggregate_val = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(aggregate_key, aggregate_val);
  }

  aht_iterator_ = aht_.Begin();
  std::vector<Value> values;
  //const auto& aggregate_key = MakeAggregateKey(&child_tuple);
 

  //while ((aht_iterator_.Key() == aggregate_key) == false && aht_iterator_ != aht_.End() ) {
  //  ++aht_iterator_;
  //}
  if (aht_iterator_ != aht_.End()) {
    values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
    values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
    ++aht_iterator_;
  }
  else {
    const auto& output_schema = child_executor_->GetOutputSchema();
    std::vector<Value> v;
    v.reserve(output_schema.GetColumnCount());
    for (size_t i = 0; i < output_schema.GetColumnCount(); i++) {
      v.push_back(ValueFactory::GetNullValueByType(output_schema.GetColumn(i).GetType()));
    }

    Tuple tuple{v, &output_schema};
    const auto& agg_key = MakeAggregateKey(&tuple);
    const auto& agg_val = aht_.GenerateInitialAggregateValue();

    if (agg_val.aggregates_.size() == 1 && plan_->GetAggregateTypes()[0] == AggregationType::CountStarAggregate) {
      values.insert(values.end(), agg_key.group_bys_.begin(), agg_key.group_bys_.end());
      values.insert(values.end(), agg_val.aggregates_.begin(), agg_val.aggregates_.end());
    }
    else {
      is_end_ = true;
      return false;
    }
  }

  fmt::println("{}", GetOutputSchema().ToString());
  fmt::println("{} {}", values.size(), GetOutputSchema().GetColumnCount());
  *tuple = Tuple{values, &GetOutputSchema()};
  *rid = child_rid;
  is_end_ = true;
  return true;
}

/** Do not use or remove this function, otherwise you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
