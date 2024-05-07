//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "execution/executors/delete_executor.h"
#include "type/type_id.h"
#include "type/value.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
    child_executor_->Init();
}
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    if(flags_){
        return false;
    }
    int delete_num=0;
    Tuple delete_tuple;
    while (true) {
        auto status=child_executor_->Next(&delete_tuple,rid);
        if(!status){
            break;
        }
        delete_num++;
        auto table_info=GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
        //p3:table_info->table_->UpdateTupleMeta({0,true},*rid);

        //auto [de_meta,de_tuple]=table_info->table_->GetTuple(*rid);
        //auto de_link=GetExecutorContext()->GetTransactionManager()->GetUndoLink(*rid);
        auto [de_meta,de_tuple,de_link]=bustub::GetTupleAndUndoLink(GetExecutorContext()->GetTransactionManager()
        ,table_info->table_.get(),*rid);
        std::vector<bool> allmodi;
        for(uint32_t i=0;i<table_info->schema_.GetColumns().size();i++){
            allmodi.push_back(true);
        }
        UndoLog update_log;
        update_log.is_deleted_=false;
        update_log.tuple_=de_tuple;
        update_log.modified_fields_=allmodi;
        update_log.ts_=de_meta.ts_;
        

        if(de_meta.ts_==GetExecutorContext()->GetTransaction()->GetTransactionTempTs()){
            if(de_link==std::nullopt){
                bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(),*rid, 
                de_link, table_info->table_.get(), GetExecutorContext()->GetTransaction(), {0,true}, de_tuple);
            }
            else{
                auto upda_log=GetExecutorContext()->GetTransactionManager()->GetUndoLog(de_link.value());
                std::vector<Value> upda_val;
                std::vector<uint32_t> upda_schema_id;
                for(uint32_t i=0;i<upda_log.modified_fields_.size();i++){
                    if(upda_log.modified_fields_[i]){
                        upda_schema_id.push_back(i);
                    }
                }
                auto upda_schema=bustub::Schema::CopySchema(&table_info->schema_, upda_schema_id);

                for(uint32_t i=0,j=0;i<upda_log.modified_fields_.size();i++){
                    if(upda_log.modified_fields_[i]){
                        upda_val.push_back(upda_log.tuple_.GetValue(&upda_schema, j));
                        j++;
                    }
                    else{
                        upda_log.modified_fields_[i]=true;
                        upda_val.push_back(de_tuple.GetValue(&table_info->schema_, i));
                    }
                }
                upda_log.tuple_=Tuple(upda_val,&table_info->schema_);
                bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(),*rid, 
                de_link, table_info->table_.get(), GetExecutorContext()->GetTransaction(), {de_meta.ts_,true}, de_tuple);
                GetExecutorContext()->GetTransaction()->ModifyUndoLog(de_link->prev_log_idx_,upda_log);
            }
        }
        else{//别人持有
            if(de_link!=std::nullopt){
                update_log.prev_version_=de_link.value();
            }
            else{
                update_log.prev_version_={INVALID_TXN_ID,0};
            }
            std::optional<UndoLink> new_link=GetExecutorContext()->GetTransaction()->AppendUndoLog(update_log);
            bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(), *rid, new_link, 
            table_info->table_.get(),GetExecutorContext()->GetTransaction(), 
            {GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),true}, de_tuple,[&](const TupleMeta& meta, const Tuple& table, RID rid,std::optional<UndoLink>){
                if(this->GetExecutorContext()->GetTransaction()->GetReadTs()<meta.ts_){
                    this->GetExecutorContext()->GetTransaction()->SetTainted();
                    throw bustub::ExecutionException("execution error!");
                    return false;
                }
                return true; 
            });
            GetExecutorContext()->GetTransaction()->AppendWriteSet(table_info->oid_, *rid);
        }
        /*
        auto table_indexs=GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
        for(auto index_info : table_indexs){
            auto attrs=index_info->index_->GetKeyAttrs();
            auto key_schema=index_info->index_->GetKeySchema();
            index_info->index_->DeleteEntry(delete_tuple.KeyFromTuple(table_info->schema_, *key_schema,attrs), *rid,nullptr);
        }
        */
    }
    flags_=true;
    std::vector<Value> delete_val{{INTEGER,delete_num}};
    delete_val.reserve(1);
    *tuple=Tuple(delete_val,&GetOutputSchema());
    return true;
}

}  // namespace bustub
