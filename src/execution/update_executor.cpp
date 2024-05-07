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
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"
#include "concurrency/transaction_manager.h"
namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)){}

void UpdateExecutor::Init() { 
  child_executor_->Init();
  table_info_=GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());

  Tuple new_tuple;
  RID new_rid;
  while(child_executor_->Next(&new_tuple, &new_rid)){
    tuple_buf_.push_back(new_tuple);
    rid_buf_.push_back(new_rid);
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(exe_flag_){
    return false;
  }
  uint32_t update_sum=0;
  bool is_pk=false;
  while(update_sum<tuple_buf_.size()){
    //目标值
    //auto [de_meta,de_tuple]=table_info_->table_->GetTuple(rid_buf_[update_sum]);
    //auto de_link=GetExecutorContext()->GetTransactionManager()->GetUndoLink(de_tuple.GetRid());
    auto [de_meta,de_tuple,de_link]=bustub::GetTupleAndUndoLink(GetExecutorContext()->GetTransactionManager()
    ,table_info_->table_.get(),rid_buf_[update_sum]);
    std::vector<Value> target_value;
    for(const auto & target_expression : plan_->target_expressions_){
      Value val=target_expression->Evaluate(&tuple_buf_[update_sum],table_info_->schema_);
      target_value.push_back(val);
    }
    auto upda_tuple=Tuple(target_value,&table_info_->schema_);
    auto table_indexs=GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
    for(auto index_info : table_indexs){
        auto attrs=index_info->index_->GetKeyAttrs();
        auto key_schema=index_info->index_->GetKeySchema();
        auto key1=upda_tuple.KeyFromTuple(table_info_->schema_, *key_schema,attrs);
        auto key2=de_tuple.KeyFromTuple(table_info_->schema_, *key_schema, attrs);
        if(index_info->is_primary_key_&&!key1.GetValue(key_schema, 0).CompareExactlyEquals(key2.GetValue(key_schema, 0))){
          is_pk=true;
        }
    };
    
    std::vector<Value> new_value;
    std::vector<bool> allmodi;
    std::vector<uint32_t> par_schema;
    UndoLog update_log;
    update_log.is_deleted_=false;
    update_log.ts_=de_meta.ts_;
    
    
    if(de_meta.ts_==GetExecutorContext()->GetTransaction()->GetTransactionTempTs()){
      if(de_link==std::nullopt){
        if(!is_pk){
          bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(),de_tuple.GetRid(), 
                  de_link, table_info_->table_.get(), GetExecutorContext()->GetTransaction(), de_meta, upda_tuple);
        }
        else{//主键更新
          for(auto index_info : table_indexs){
              auto attrs=index_info->index_->GetKeyAttrs();
              auto key_schema=index_info->index_->GetKeySchema();

              if(!pk_){//第一轮
                bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(),de_tuple.GetRid(), 
                  de_link, table_info_->table_.get(), GetExecutorContext()->GetTransaction(), {de_meta.ts_,true}, de_tuple);
              }
              else{//第二轮
                std::vector<RID> res;
                index_info->index_->ScanKey({upda_tuple.KeyFromTuple(table_info_->schema_,
                *key_schema, attrs)},&res, GetExecutorContext()->GetTransaction());
                if(res.empty()){//需要插
                  std::optional<RID> insert_oid=table_info_->table_->InsertTuple({GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),false},upda_tuple);
                  bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(),*insert_oid, 
                  std::nullopt, table_info_->table_.get(), GetExecutorContext()->GetTransaction(), {de_meta.ts_,false}, upda_tuple);
                  index_info->index_->InsertEntry({upda_tuple.KeyFromTuple(table_info_->schema_,
                    *key_schema, attrs)},*insert_oid, GetExecutorContext()->GetTransaction());
                  GetExecutorContext()->GetTransaction()->AppendWriteSet(table_info_->oid_, *insert_oid);
                }
                else{

                  auto [de_meta,de_tuple,de_link]=bustub::GetTupleAndUndoLink(GetExecutorContext()->GetTransactionManager()
                        ,table_info_->table_.get(),res[0]);
                  if(de_meta.ts_==GetExecutorContext()->GetTransaction()->GetTransactionTempTs()){
                    bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(), res[0], GetExecutorContext()->GetTransactionManager()->GetUndoLink(res[0]), 
                    table_info_->table_.get(),GetExecutorContext()->GetTransaction(), 
                    {GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),false}, upda_tuple);
                  }
                  else{
                    UndoLog new_log;
                    new_log.is_deleted_=de_meta.is_deleted_;
                    if(de_link.has_value()){
                      new_log.prev_version_=de_link.value();
                    }
                    new_log.ts_=de_meta.ts_;
                    new_log.tuple_=de_tuple;
                    for(uint32_t i=0;i<table_info_->schema_.GetColumns().size();i++){
                      new_log.modified_fields_.push_back(true);
                    }
                    std::optional<UndoLink> new_link=GetExecutorContext()->GetTransaction()->AppendUndoLog(new_log);
                    bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(),res[0], 
                    new_link, table_info_->table_.get(), GetExecutorContext()->GetTransaction(), {GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),false}, upda_tuple);
                  }
                  GetExecutorContext()->GetTransaction()->AppendWriteSet(table_info_->oid_, res[0]);
                  
                }
              }
          };
        }
      }
      else{//更过新！
        bool flag=false;
        auto old_log=GetExecutorContext()->GetTransactionManager()->GetUndoLog(de_link.value());
        for(uint32_t i=0;i<table_info_->schema_.GetColumns().size();i++){
          if(de_tuple.GetValue(&table_info_->schema_,i).CompareExactlyEquals(upda_tuple.GetValue(&table_info_->schema_,i))){
            allmodi.push_back(false);
          }
          else{
            allmodi.push_back(true);
          }
        }
        for(uint32_t i=0;i<allmodi.size();i++){
          if(allmodi[i]&&!old_log.modified_fields_[i]){
            flag=true;
          }
        }
        if(flag){
          std::vector<uint32_t> old_schema_id;
          for(uint32_t i=0;i<old_log.modified_fields_.size();i++){
            if(old_log.modified_fields_[i]){
              old_schema_id.push_back(i);
            }
          }
          auto old_schema=bustub::Schema::CopySchema(&table_info_->schema_, old_schema_id);
          for(uint32_t i=0,j=0;i<table_info_->schema_.GetColumns().size();i++){
            if(old_log.modified_fields_[i]){
              par_schema.push_back(i);
              new_value.push_back(old_log.tuple_.GetValue(&old_schema,j));
              j++;
            }
            if(allmodi[i]&&!old_log.modified_fields_[i]){
              par_schema.push_back(i);
              old_log.modified_fields_[i]=true;
              new_value.push_back(de_tuple.GetValue(&table_info_->schema_,i));
            }
          }
          auto schema_0=bustub::Schema::CopySchema(&table_info_->schema_,par_schema);
          old_log.tuple_=Tuple(new_value,&schema_0);
          GetExecutorContext()->GetTransaction()->ModifyUndoLog(de_link->prev_log_idx_, old_log);
        }
        if(!is_pk){
          bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(),de_tuple.GetRid(), 
                de_link, table_info_->table_.get(), GetExecutorContext()->GetTransaction(), de_meta, upda_tuple);
        }
        else{
          for(auto index_info : table_indexs){
              auto attrs=index_info->index_->GetKeyAttrs();
              auto key_schema=index_info->index_->GetKeySchema();
              if(!pk_){//第一轮
                auto old_log=GetExecutorContext()->GetTransactionManager()->GetUndoLog(de_link.value());
                std::vector<Value> old_val;
                for(uint32_t i=0;i<table_info_->schema_.GetColumns().size();i++){
                  old_log.modified_fields_[i]=true;
                  old_val.push_back(de_tuple.GetValue(&table_info_->schema_, i));
                }
                old_log.tuple_=Tuple(old_val,&table_info_->schema_);
                GetExecutorContext()->GetTransaction()->ModifyUndoLog(de_link->prev_log_idx_, old_log);
                bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(),de_tuple.GetRid(), 
                  de_link, table_info_->table_.get(), GetExecutorContext()->GetTransaction(), {de_meta.ts_,true}, de_tuple);
              }
              else{//第二轮
                std::vector<RID> res;
                index_info->index_->ScanKey({upda_tuple.KeyFromTuple(table_info_->schema_,
                *key_schema, attrs)},&res, GetExecutorContext()->GetTransaction());
                if(res.empty()){//需要插
                  std::optional<RID> insert_oid=table_info_->table_->InsertTuple({GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),false},upda_tuple);
                  bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(),*insert_oid, 
                  std::nullopt, table_info_->table_.get(), GetExecutorContext()->GetTransaction(), {de_meta.ts_,false}, upda_tuple);
                  index_info->index_->InsertEntry({upda_tuple.KeyFromTuple(table_info_->schema_,
                    *key_schema, attrs)},*insert_oid, GetExecutorContext()->GetTransaction());
                  GetExecutorContext()->GetTransaction()->AppendWriteSet(table_info_->oid_, *insert_oid);
                }
                else{
                  auto [de_meta,de_tuple,de_link]=bustub::GetTupleAndUndoLink(GetExecutorContext()->GetTransactionManager()
                        ,table_info_->table_.get(),res[0]);
                  if(de_meta.ts_==GetExecutorContext()->GetTransaction()->GetTransactionTempTs()){
                    bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(), res[0], GetExecutorContext()->GetTransactionManager()->GetUndoLink(res[0]), 
                    table_info_->table_.get(),GetExecutorContext()->GetTransaction(), 
                    {GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),false}, upda_tuple);
                  }
                  else{
                    UndoLog new_log;
                    new_log.is_deleted_=de_meta.is_deleted_;
                    if(de_link.has_value()){
                      new_log.prev_version_=de_link.value();
                    }
                    new_log.ts_=de_meta.ts_;
                    new_log.tuple_=de_tuple;
                    for(uint32_t i=0;i<table_info_->schema_.GetColumns().size();i++){
                      new_log.modified_fields_.push_back(true);
                    }
                    std::optional<UndoLink> new_link=GetExecutorContext()->GetTransaction()->AppendUndoLog(new_log);
                    bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(),res[0], 
                    new_link, table_info_->table_.get(), GetExecutorContext()->GetTransaction(), {GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),false}, upda_tuple);
                  }
                  GetExecutorContext()->GetTransaction()->AppendWriteSet(table_info_->oid_, res[0]);
                }
              }
          }
        }
      }
    }
    else{
      if(de_link!=std::nullopt){
        update_log.prev_version_=de_link.value();
      }
      else{
        update_log.prev_version_={INVALID_TXN_ID,0};
      }
      for(uint32_t i=0;i<table_info_->schema_.GetColumns().size();i++){
        if(de_tuple.GetValue(&table_info_->schema_,i).CompareExactlyEquals(upda_tuple.GetValue(&table_info_->schema_,i))){
          allmodi.push_back(false);
        }
        else{
          allmodi.push_back(true);
          par_schema.push_back(i);
          new_value.push_back(de_tuple.GetValue(&table_info_->schema_,i));
        }
      }
      update_log.modified_fields_=allmodi;
      auto schema_0=bustub::Schema::CopySchema(&table_info_->schema_,par_schema);
      update_log.tuple_=Tuple(new_value,&schema_0);
      std::optional<UndoLink> new_link=GetExecutorContext()->GetTransaction()->AppendUndoLog(update_log);
      if(!is_pk){
            bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(), de_tuple.GetRid(), new_link, 
            table_info_->table_.get(),GetExecutorContext()->GetTransaction(), 
            {GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),de_meta.is_deleted_}, upda_tuple,[&](const TupleMeta& meta, const Tuple& table, RID rid,std::optional<UndoLink>){
                if(this->GetExecutorContext()->GetTransaction()->GetReadTs()<meta.ts_){
                    this->GetExecutorContext()->GetTransaction()->SetTainted();
                    throw bustub::ExecutionException("execution error!");
                    return false;
                }
                return true; 
            });
      }
      else{
        for(auto index_info : table_indexs){
              auto attrs=index_info->index_->GetKeyAttrs();
              auto key_schema=index_info->index_->GetKeySchema();
              if(!pk_){//第一轮
                bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(), de_tuple.GetRid(), new_link, 
                  table_info_->table_.get(),GetExecutorContext()->GetTransaction(), 
                  {GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),true}, de_tuple,[&](const TupleMeta& meta, const Tuple& table, RID rid,std::optional<UndoLink>){
                      if(this->GetExecutorContext()->GetTransaction()->GetReadTs()<meta.ts_){
                          this->GetExecutorContext()->GetTransaction()->SetTainted();
                          throw bustub::ExecutionException("execution error!");
                          return false;
                      }
                      return true; 
                });
                auto old_log=GetExecutorContext()->GetTransactionManager()->GetUndoLog(new_link.value());
                for(uint32_t i=0;i<table_info_->schema_.GetColumns().size();i++){
                  old_log.modified_fields_[i]=true;
                }
                old_log.tuple_=de_tuple;
                GetExecutorContext()->GetTransaction()->ModifyUndoLog(new_link->prev_log_idx_, old_log);
              }
              else{//第二轮
                std::vector<RID> res;
                index_info->index_->ScanKey({upda_tuple.KeyFromTuple(table_info_->schema_,
                *key_schema, attrs)},&res, GetExecutorContext()->GetTransaction());
                if(res.empty()){//需要插
                  std::optional<RID> insert_oid=table_info_->table_->InsertTuple({GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),false},upda_tuple);
                  bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(), *insert_oid, std::nullopt, 
                  table_info_->table_.get(),GetExecutorContext()->GetTransaction(), 
                  {GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),false}, upda_tuple,[&](const TupleMeta& meta, const Tuple& table, RID rid,std::optional<UndoLink>){
                      if(this->GetExecutorContext()->GetTransaction()->GetReadTs()<meta.ts_){
                          this->GetExecutorContext()->GetTransaction()->SetTainted();
                          throw bustub::ExecutionException("execution error!");
                          return false;
                      }
                      return true; 
                  });
                  index_info->index_->InsertEntry({upda_tuple.KeyFromTuple(table_info_->schema_,
                    *key_schema, attrs)},*insert_oid, GetExecutorContext()->GetTransaction());
                  GetExecutorContext()->GetTransaction()->AppendWriteSet(table_info_->oid_, *insert_oid);
                }
                else{
                  
                  auto [de_meta,de_tuple,de_link]=bustub::GetTupleAndUndoLink(GetExecutorContext()->GetTransactionManager()
                        ,table_info_->table_.get(),res[0]);
                  if(de_meta.ts_==GetExecutorContext()->GetTransaction()->GetTransactionTempTs()){
                    bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(), res[0], GetExecutorContext()->GetTransactionManager()->GetUndoLink(res[0]), 
                    table_info_->table_.get(),GetExecutorContext()->GetTransaction(), 
                    {GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),false}, upda_tuple);
                  }
                  else{
                    UndoLog new_log;
                    new_log.is_deleted_=de_meta.is_deleted_;
                    if(de_link.has_value()){
                      new_log.prev_version_=de_link.value();
                    }
                    new_log.ts_=de_meta.ts_;
                    new_log.tuple_=de_tuple;
                    for(uint32_t i=0;i<table_info_->schema_.GetColumns().size();i++){
                      new_log.modified_fields_.push_back(true);
                    }
                    std::optional<UndoLink> new_link=GetExecutorContext()->GetTransaction()->AppendUndoLog(new_log);
                    bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(), res[0], new_link, 
                    table_info_->table_.get(),GetExecutorContext()->GetTransaction(), 
                    {GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),false}, upda_tuple,[&](const TupleMeta& meta, const Tuple& table, RID rid,std::optional<UndoLink>){
                        if(this->GetExecutorContext()->GetTransaction()->GetReadTs()<meta.ts_){
                            this->GetExecutorContext()->GetTransaction()->SetTainted();
                            throw bustub::ExecutionException("execution error!");
                            return false;
                        }
                        return true; 
                    });
                  }
                  GetExecutorContext()->GetTransaction()->AppendWriteSet(table_info_->oid_, res[0]);
                }
              }
          }
      }
      GetExecutorContext()->GetTransaction()->AppendWriteSet(table_info_->oid_, de_tuple.GetRid());
    }
    update_sum++;
    if(update_sum==tuple_buf_.size()&&is_pk&&!pk_){
      update_sum=0;
      pk_=true;
    }
  }
  exe_flag_=true;
  std::vector<Value> out_value{{INTEGER,static_cast<int>(update_sum)}};
  out_value.reserve(1);
  *tuple=Tuple(out_value,&GetOutputSchema());
  return true;
}
}  // namespace bustub
