//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <utility>
#include "binder/table_ref/bound_join_ref.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),plan_(plan),left_child_(std::move(left_child)),right_child_(std::move(right_child)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}
void HashJoinExecutor::Init() { 
  left_child_->Init();
  right_child_->Init();
  ht_.clear();
  Tuple tuple;
  RID rid;
  //对右表构造哈希，因为是左连接，匹配不到便于空值
  while(right_child_->Next(&tuple,&rid)){
    HJKey hjkey;
    const auto &expr=plan_->RightJoinKeyExpressions();
    for(auto& iter:expr){
      hjkey.hjkey_.push_back(iter->Evaluate(&tuple, right_child_->GetOutputSchema()));
    }
    ht_[hjkey].target_tuple_.push_back(tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    //输出匹配的右元组
    if(!tar_tuple_.empty()){
      Tuple right_tuple=tar_tuple_.back();
      tar_tuple_.pop_back();
      std::vector<Value> res;
      for(uint32_t i0=0;i0<left_child_->GetOutputSchema().GetColumnCount();i0++){
        res.push_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i0));
      }
      for(uint32_t i0=0;i0<right_child_->GetOutputSchema().GetColumnCount();i0++){
        res.push_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i0));
      }
      *tuple=Tuple(res,&GetOutputSchema());
      //*rid=tuple->GetRid();
      return true;
    }

    if(!left_child_->Next(&left_tuple_,rid)){//到头了
      return false;
    }
    HJKey hjkey;
    const auto &expr=plan_->LeftJoinKeyExpressions();
    for(auto& iter:expr){
      hjkey.hjkey_.push_back(iter->Evaluate(&left_tuple_, left_child_->GetOutputSchema()));
    }
    if(ht_.count(hjkey)==0&&plan_->join_type_==JoinType::LEFT){//空值
      std::vector<Value> res;
      for(uint32_t i0=0;i0<left_child_->GetOutputSchema().GetColumnCount();i0++){
        res.push_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i0));
      }
      for(uint32_t i0=0;i0<right_child_->GetOutputSchema().GetColumnCount();i0++){
        res.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i0).GetType()));
      }
      *tuple=Tuple(res,&GetOutputSchema());
      //*rid=tuple->GetRid();
      return true;
    }
    if(ht_.count(hjkey)!=0){
      tar_tuple_=ht_[hjkey].target_tuple_;
    }
  }
  return true;
}
  
}  // namespace bustub
