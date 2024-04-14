//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "storage/table/table_iterator.h"
#include "type/limits.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"
namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  auto GenerateInitialAggregateValue(std::unordered_map<WindowKey, WindowValue>& ht,std::vector<AbstractExpressionRef>& expr,
    std::vector<WindowFunctionType>& ty,std::vector<bool>& is_flag) -> WindowValue;
  
  void CombineAggregateValues(WindowValue *result, const WindowValue &input,std::unordered_map<WindowKey, WindowValue>& ht,std::vector<AbstractExpressionRef>& expr,
    std::vector<WindowFunctionType>& ty,std::vector<bool>& is_flag,int32_t&rank);
  
  void InsertCombine(std::unordered_map<WindowKey, WindowValue>& ht,const WindowKey &agg_key, const WindowValue &agg_val,std::vector<AbstractExpressionRef>& expr,
    std::vector<WindowFunctionType>& ty,std::vector<bool>& is_flag,int32_t&rank); 

  auto MakeWindowKey(const Tuple *tuple) -> WindowKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->window_functions_) {
      keys.emplace_back(expr.second.partition_by_[0]->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }
  auto MakeWindowValue(const Tuple *tuple) -> WindowValue {
    std::vector<Value> vals;
    for (const auto &expr : plan_->window_functions_) {
      vals.emplace_back(expr.second.function_->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {vals};
  }
 private:
 
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;
  std::vector<Tuple> res_;
  uint32_t idx_=0;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  bool order_flag_=false;
  std::vector<Tuple> rest_;
  int32_t rank_=1;
};
}  // namespace bustub
