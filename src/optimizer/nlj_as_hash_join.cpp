//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto NLJAsHashJoinHelper(AbstractExpressionRef expr, std::vector<AbstractExpressionRef>& left_key_expressions, std::vector<AbstractExpressionRef>& right_key_expressions) -> bool {
  if (auto col_expr = dynamic_cast<ColumnValueExpression*>(expr.get()); col_expr != nullptr){
    if (col_expr->GetTupleIdx() == 0) {
      left_key_expressions.push_back(expr);
    }
    else {
      right_key_expressions.push_back(expr);
    }
    return true;
  }
  if (auto compare_expr = dynamic_cast<ComparisonExpression*>(expr.get()); compare_expr != nullptr) {
    if (compare_expr->comp_type_ != ComparisonType::Equal) {
      return false;
    }
    return NLJAsHashJoinHelper(expr->GetChildAt(0), left_key_expressions, right_key_expressions) &&
           NLJAsHashJoinHelper(expr->GetChildAt(1), left_key_expressions, right_key_expressions);
  }
  if (auto logic_expr = dynamic_cast<LogicExpression*>(expr.get()); logic_expr != nullptr) {
    if (logic_expr->logic_type_ != LogicType::And) {
      return false;
    }
    return NLJAsHashJoinHelper(expr->GetChildAt(0), left_key_expressions, right_key_expressions) &&
           NLJAsHashJoinHelper(expr->GetChildAt(1), left_key_expressions, right_key_expressions);
  }
  return false;
}

/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for Spring 2025: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto& nested_loop_join_plan = dynamic_cast<const NestedLoopJoinPlanNode&>(*optimized_plan);
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    if (NLJAsHashJoinHelper(nested_loop_join_plan.predicate_, left_key_expressions, right_key_expressions)) {
      return std::make_shared<HashJoinPlanNode>(nested_loop_join_plan.output_schema_,
                                                nested_loop_join_plan.GetLeftPlan(),
                                                nested_loop_join_plan.GetRightPlan(),
                                                left_key_expressions,
                                                right_key_expressions,
                                                nested_loop_join_plan.join_type_);
    }
  }
  return optimized_plan;
}


}  // namespace bustub
