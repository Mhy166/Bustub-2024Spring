//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>
#include <vector>

#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)){}

void UpdateExecutor::Init() { 
  child_executor_->Init();
  table_info_=GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(flags_){
    return false;
  }
  int update_sum=0;
  Tuple new_tuple;
  while(true){
    auto status=child_executor_->Next(&new_tuple, rid);
    if(!status){
      break;
    }
    //目标值
    std::vector<Value> target_value;
    for(const auto & target_expression : plan_->target_expressions_){
      target_value.push_back(target_expression->Evaluate(&new_tuple,table_info_->schema_));
    }
    new_tuple=Tuple(target_value,&table_info_->schema_);
    auto new_rid=table_info_->table_->InsertTuple({0,false},new_tuple);
    if(new_rid==std::nullopt){
      continue;
    }
    //删除旧的
    auto old_tuple=table_info_->table_->GetTuple(*rid).second;
    table_info_->table_->UpdateTupleMeta({0,true}, *rid);

    update_sum++;
    auto table_indexs=GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
    for(auto index_info : table_indexs){
        auto attrs=index_info->index_->GetKeyAttrs();
        auto key_schema=index_info->index_->GetKeySchema();
        index_info->index_->DeleteEntry(old_tuple.KeyFromTuple(table_info_->schema_, *key_schema,attrs), *rid,nullptr);
        index_info->index_->InsertEntry(new_tuple.KeyFromTuple(table_info_->schema_, *key_schema, attrs), *new_rid, nullptr);
    }
  }

  flags_=true;
  std::vector<Value> out_value{{INTEGER,update_sum}};
  out_value.reserve(1);
  *tuple=Tuple(out_value,&GetOutputSchema());
  return true;
}
}  // namespace bustub
