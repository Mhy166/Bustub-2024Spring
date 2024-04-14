#include <memory>
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if(optimized_plan->GetType()==PlanType::Limit){
    const auto& limit=dynamic_cast<const LimitPlanNode&>(*optimized_plan);
    if(limit.GetChildren().size()==1&&limit.GetChildPlan()->GetType()==PlanType::Sort){
      const auto& sort_node=dynamic_cast<const SortPlanNode&>(*limit.GetChildAt(0));
      if(sort_node.GetChildren().size()==1){
        return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_,sort_node.GetChildAt(0),sort_node.GetOrderBy(),limit.GetLimit());
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
