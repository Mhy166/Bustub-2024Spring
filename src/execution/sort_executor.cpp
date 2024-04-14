#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <utility>
#include "binder/bound_order_by.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
    child_executor_->Init();
    res_.clear();
  
    Tuple tuple;
    RID rid;
    while(child_executor_->Next(&tuple, &rid)){
        res_.push_back(tuple);
    }
    
    std::sort(res_.begin(),res_.end(), [&](const Tuple& a, const Tuple& b) {
        for(const auto &iter:plan_->GetOrderBy()){
            Value va=iter.second->Evaluate(&a, plan_->GetChildPlan()->OutputSchema());
            Value vb=iter.second->Evaluate(&b, plan_->GetChildPlan()->OutputSchema());
            if(iter.first==OrderByType::DESC){
                if(va.CompareGreaterThan(vb)==CmpBool::CmpTrue){
                    return true;
                }
                if(va.CompareLessThan(vb)==CmpBool::CmpTrue){
                    return false;
                }
                if(va.CompareEquals(vb)==CmpBool::CmpTrue){
                    continue;
                }
            }
            if(iter.first==OrderByType::ASC||iter.first==OrderByType::DEFAULT){
                if(va.CompareLessThan(vb)==CmpBool::CmpTrue){
                    return true;
                }
                if(va.CompareGreaterThan(vb)==CmpBool::CmpTrue){
                    return false;
                }
                if(va.CompareEquals(vb)==CmpBool::CmpTrue){
                    continue;
                }
            }
        }
        return true;
    });
    
    idx_=0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(idx_==res_.size()){
        return false;
    }
    *tuple=res_[idx_];
    *rid=tuple->GetRid();
    idx_++;
    return true;
}

}  // namespace bustub
