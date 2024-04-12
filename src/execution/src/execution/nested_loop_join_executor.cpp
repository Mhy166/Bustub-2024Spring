//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_l_(std::move(left_executor)),child_executor_r_(std::move(right_executor)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  child_executor_l_->Init();
  child_executor_r_->Init();
  left_tuple_=new Tuple;
  right_tuple_=new Tuple;
  Tuple tuple;
  RID rid;
  child_executor_l_->Next(left_tuple_, &rid);
  flag_=true;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto & expr=plan_->Predicate();
    while (true) {
      //针对左，遍历右。
      while(child_executor_r_->Next(right_tuple_, rid)){
        if(expr->EvaluateJoin(left_tuple_, child_executor_l_->GetOutputSchema(), right_tuple_, child_executor_r_->GetOutputSchema()).GetAs<bool>()){
          std::vector<Value> res;
          for(uint32_t i0=0;i0<child_executor_l_->GetOutputSchema().GetColumnCount();i0++){
            res.push_back(left_tuple_->GetValue(&child_executor_l_->GetOutputSchema(), i0));
          }
          for(uint32_t i0=0;i0<child_executor_r_->GetOutputSchema().GetColumnCount();i0++){
            res.push_back(right_tuple_->GetValue(&child_executor_r_->GetOutputSchema(), i0));
          }
          *tuple=Tuple(res,&GetOutputSchema());
          *rid=tuple->GetRid();
          flag_=false;
          return true; 
        }
      }
      //右遍历完了,如果没有匹配的，那么left的输出空值
      if(flag_&&plan_->join_type_==JoinType::LEFT){
          std::vector<Value> res;
          for(uint32_t i0=0;i0<child_executor_l_->GetOutputSchema().GetColumnCount();i0++){
            res.push_back(left_tuple_->GetValue(&child_executor_l_->GetOutputSchema(), i0));
          }
          for(uint32_t i0=0;i0<child_executor_r_->GetOutputSchema().GetColumnCount();i0++){
            res.push_back(ValueFactory::GetNullValueByType(child_executor_r_->GetOutputSchema().GetColumn(i0).GetType()));
          }
          *tuple=Tuple(res,&GetOutputSchema());
          *rid=tuple->GetRid();
          flag_=false;
          child_executor_r_->Init();
          return true; 
      }
      if(!child_executor_l_->Next(left_tuple_, rid)){//结束！
        delete left_tuple_;left_tuple_=nullptr;
        delete right_tuple_;right_tuple_=nullptr;
        return false;
      }
      child_executor_r_->Init();
      flag_=true;
    }
  return true;
}

}  // namespace bustub
