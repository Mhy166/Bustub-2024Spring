//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>
#include <random>
#include <thread>
#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/index/index.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"
#include "concurrency/transaction_manager.h"
namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
    child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    if(flag_){
        return false;
    }
    int insert_sum=0;
    bool f=true;
    Tuple insert_tuple;
    RID new_rid;
    while(true){
        auto status=child_executor_->Next(&insert_tuple,&new_rid);
        if(!status){//结束
            break;
        }
        auto table_cata=GetExecutorContext()->GetCatalog();
        auto table_info=table_cata->GetTable(plan_->GetTableOid());
        auto table_indexs=table_cata->GetTableIndexes(table_info->name_);
        std::optional<RID> insert_oid=std::nullopt;
        //主键索引
        for(auto indexs:table_indexs){
            auto attrs=indexs->index_->GetKeyAttrs();
            auto key_schema=indexs->index_->GetKeySchema();
            std::vector<RID> res;
            indexs->index_->ScanKey({insert_tuple.KeyFromTuple(table_info->schema_,
                *key_schema, attrs)},&res, GetExecutorContext()->GetTransaction());
            if(!res.empty()){//存在了
                auto [at_meta,at_tuple,at_link]=bustub::GetTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(), table_info->table_.get(), res[0]);
                if(at_meta.is_deleted_){//更新
                    UndoLog new_undo;
                    new_undo.is_deleted_=true;
                    if(at_link!=std::nullopt){
                        new_undo.prev_version_=at_link.value();
                    }
                    new_undo.ts_=at_meta.ts_;
                    std::vector<Value> val;
                    for(uint32_t i=0;i<table_info->schema_.GetColumns().size();i++){
                        new_undo.modified_fields_.push_back(true);
                        val.push_back(at_tuple.GetValue(&table_info->schema_, i));
                    }
                    new_undo.tuple_=Tuple(val,&table_info->schema_);
                    std::optional<UndoLink> new_link=GetExecutorContext()->GetTransaction()->AppendUndoLog(new_undo);
                    bustub::UpdateTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(), res[0], new_link , table_info->table_.get(), 
                    GetExecutorContext()->GetTransaction(),{GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),false},insert_tuple);
                    //MVCC
                    GetExecutorContext()->GetTransaction()->AppendWriteSet(table_info->oid_, res[0]);
                    //成功插入影响了
                    insert_sum++;
                    f=false;
                    break;
                }
                GetExecutorContext()->GetTransaction()->SetTainted();
                throw bustub::ExecutionException("execution error!");
                return false;
               
            }
        }
        if(!f){
            continue;
        }
        insert_oid=table_info->table_->InsertTuple({GetExecutorContext()->GetTransaction()->GetTransactionTempTs(),false},insert_tuple);
        if(insert_oid==std::nullopt){
            continue;
        }
        for(auto indexs:table_indexs){
            auto attrs=indexs->index_->GetKeyAttrs();
            auto key_schema=indexs->index_->GetKeySchema();
            auto [at_meta,at_tuple,at_link]=bustub::GetTupleAndUndoLink(GetExecutorContext()->GetTransactionManager(), table_info->table_.get(), *insert_oid);
            auto statu=indexs->index_->InsertEntry({at_tuple.KeyFromTuple(table_info->schema_,
                *key_schema, attrs)},at_tuple.GetRid(), GetExecutorContext()->GetTransaction());
            if(!statu){
                GetExecutorContext()->GetTransaction()->SetTainted();
                throw bustub::ExecutionException("execution error!");
                return false;
            }
        }
        //MVCC
        GetExecutorContext()->GetTransaction()->AppendWriteSet(table_info->oid_, *insert_oid);
        GetExecutorContext()->GetTransactionManager()->UpdateUndoLink(*insert_oid,std::nullopt);
        //成功插入影响了
        insert_sum++;
    }
    //输出insert_sum
    flag_=true;
    std::vector<Value> out_value{{INTEGER,insert_sum}};
    out_value.reserve(1);
    *tuple=Tuple(out_value,&GetOutputSchema());
    return true;
}
}  // namespace bustub
