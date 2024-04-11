#include <cstdint>
#include <memory>
#include <new>
#include <queue>
#include <set>
#include <vector>
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

//终于完事了！！！！

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for(const auto&child:plan->GetChildren()){
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan=plan->CloneWithChildren(std::move(children));

  if(optimized_plan->GetType()==PlanType::SeqScan){
    const auto &seq_plan=dynamic_cast<const SeqScanPlanNode&>(*optimized_plan);
    const auto *table_info=catalog_.GetTable(seq_plan.GetTableOid());
    if(seq_plan.filter_predicate_!=nullptr){//有谓语
      std::vector<AbstractExpressionRef> pred_keys;//常值
      uint32_t col_id;
      bool flag=false;
      std::queue<AbstractExpressionRef> q;
      q.push(seq_plan.filter_predicate_);
      while(!q.empty()){
        const auto expr=dynamic_cast<const ColumnValueExpression*>(q.front().get());
        const auto expr_com=dynamic_cast<const ComparisonExpression*>(q.front().get());
        const auto expr_logic=dynamic_cast<const LogicExpression*>(q.front().get());
        if(expr!=nullptr){//列值
          if(!flag){
            col_id=expr->GetColIdx();
            flag=true;
          }
          else{
            if(col_id!=expr->GetColIdx()){
              return optimized_plan;
            }
          }
          q.pop();
          continue;
        }
        if(expr_com!=nullptr){
          if(expr_com->comp_type_==ComparisonType::Equal){
            q.push(expr_com->GetChildAt(0));
            q.push(expr_com->GetChildAt(1));
          }
          else{
            return optimized_plan;
          }
          q.pop();
          continue;
        }
        if(expr_logic!=nullptr){
          if(expr_logic->logic_type_==LogicType::Or){
            q.push(expr_logic->GetChildAt(0));
            q.push(expr_logic->GetChildAt(1));
          }
          else{
            return optimized_plan;
          }
          q.pop();
          continue;
        }
        pred_keys.push_back(q.front());
        q.pop();
      }
      //匹配索引
      
      if (auto index = MatchIndex(table_info->name_, col_id);index != std::nullopt) {
        auto [index_oid, index_name] = *index;
        return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_,seq_plan.table_oid_,index_oid,seq_plan.filter_predicate_,pred_keys);
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
