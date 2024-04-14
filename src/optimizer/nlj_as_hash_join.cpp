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

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  
  if(optimized_plan->GetType()==PlanType::NestedLoopJoin){
    const auto& nlj_plan=dynamic_cast<const NestedLoopJoinPlanNode&>(*optimized_plan);
    if(nlj_plan.predicate_!=nullptr){
      std::vector<AbstractExpressionRef> left;
      std::vector<AbstractExpressionRef> right;
      std::queue<AbstractExpressionRef> q;
      q.push(nlj_plan.predicate_);
      while(!q.empty()){
        const auto expr=dynamic_cast<const LogicExpression*>(q.front().get());
        const auto expr_eq=dynamic_cast<const ComparisonExpression*>(q.front().get());
        if(expr!=nullptr){
          if(expr->logic_type_==LogicType::And){
            q.push(expr->GetChildAt(0));
            q.push(expr->GetChildAt(1));
          }
          else{
            return optimized_plan;
          }
          q.pop();
        }
        if(expr_eq!=nullptr){
          if(expr_eq->comp_type_==ComparisonType::Equal){
             auto left_expr=dynamic_cast< ColumnValueExpression*>(expr_eq->GetChildAt(0).get());
             auto right_expr=dynamic_cast< ColumnValueExpression*>(expr_eq->GetChildAt(1).get());
            if(left_expr==nullptr||right_expr==nullptr){
              return optimized_plan;
            }
            //开始判断归属
            if(left_expr->GetTupleIdx()==0&&right_expr->GetTupleIdx()==1){
              left.push_back(expr_eq->GetChildAt(0));
              right_expr->NotTupleIdx();
              right.push_back(expr_eq->GetChildAt(1));
            }
            else if(left_expr->GetTupleIdx()==1&&right_expr->GetTupleIdx()==0){
              left_expr->NotTupleIdx();
              left.push_back(expr_eq->GetChildAt(1));
              right.push_back(expr_eq->GetChildAt(0));
            }
          }
          else{
            return optimized_plan;
          }
          q.pop();
        }
        if(expr==nullptr&&expr_eq==nullptr){
          return optimized_plan;
        }
      }
      return std::make_shared<HashJoinPlanNode>(optimized_plan->output_schema_,nlj_plan.GetLeftPlan(),nlj_plan.GetRightPlan(),left,right,nlj_plan.GetJoinType());
    }
  }


  return optimized_plan;
}

}  // namespace bustub
