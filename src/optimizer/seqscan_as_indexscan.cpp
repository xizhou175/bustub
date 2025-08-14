//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "optimizer/optimizer.h"

#include "execution/plans/seq_scan_plan.h"

namespace bustub {

auto get_col_expr(AbstractExpression* expr, std::vector<AbstractExpressionRef>& pred_keys) -> ColumnValueExpression*{
  auto left = expr->GetChildAt(0);
  auto right = expr->GetChildAt(1);
  auto col_expr = dynamic_cast<ColumnValueExpression*>(left.get());
  if (col_expr == nullptr) {
    col_expr = dynamic_cast<ColumnValueExpression*>(right.get());
    pred_keys.push_back(left);
  }
  else {
    pred_keys.push_back(right);
  }
  return col_expr;
};

bool helper(AbstractExpression* expr, ColumnValueExpression*& col_expr, std::vector<AbstractExpressionRef>& pred_keys) {
  auto left = expr->GetChildAt(0);
  auto right = expr->GetChildAt(1);
  auto left_expr = dynamic_cast<ComparisonExpression*>(left.get());
  auto right_expr = dynamic_cast<ComparisonExpression*>(right.get());

  ColumnValueExpression* left_col_expr = nullptr;
  ColumnValueExpression* right_col_expr = nullptr;

  bool l = true, r = true;
  if (!left_expr) {
    l = helper(left.get(), left_col_expr, pred_keys);
  }
  else {
    left_col_expr = get_col_expr(left_expr, pred_keys);
  }

  if (!right_expr){
    r = helper(right.get(), right_col_expr, pred_keys);
  }
  else {
    right_col_expr = get_col_expr(right_expr, pred_keys);
  }

  col_expr = right_col_expr;

  return l && r && right_col_expr->GetColIdx() == left_col_expr->GetColIdx();
}

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(P3): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_ != nullptr && !seq_scan_plan.filter_predicate_->GetChildren().empty()) {

      std::vector<AbstractExpressionRef> pred_keys;

      if (auto cur_expr = dynamic_cast<ComparisonExpression*>(seq_scan_plan.filter_predicate_.get()); cur_expr != nullptr){
        auto col_expr = get_col_expr(seq_scan_plan.filter_predicate_.get(), pred_keys);
        fmt::println("pred keys size: {}", pred_keys.size());
        if (auto index = MatchIndex(seq_scan_plan.table_name_, col_expr->GetColIdx()); index != std::nullopt) {
          auto [index_oid, index_name] = *index;
          return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                                    index_oid, seq_scan_plan.filter_predicate_, pred_keys);
        }
      }
      else {
        ColumnValueExpression* col_expr = nullptr;
        if (helper(seq_scan_plan.filter_predicate_.get(), col_expr, pred_keys)) {
          if (auto index = MatchIndex(seq_scan_plan.table_name_, col_expr->GetColIdx()); index != std::nullopt) {
            auto [index_oid, index_name] = *index;
            return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                                    index_oid, seq_scan_plan.filter_predicate_, pred_keys);
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
