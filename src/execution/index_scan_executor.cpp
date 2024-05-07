//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <vector>
#include <set>
#include "catalog/schema.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),plan_(plan) {}

void IndexScanExecutor::Init() {
    res_.clear();
    auto index_info=GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());//索引信息
    htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());//哈希索引表
    std::vector<Value> indexval;
    std::vector<RID> res;//待存结果
    for(const auto & pred_key : plan_->pred_keys_){
        auto tmp=pred_key->Evaluate(nullptr,index_info->key_schema_);
        indexval.push_back(tmp);//随便传
        Tuple key1=Tuple(indexval,htable_->GetKeySchema()); 
        htable_->ScanKey(key1, &res, GetExecutorContext()->GetTransaction());//RID结果已经存好。
        indexval.pop_back();
    }
    //res去重
    for(auto & re : res){
        bool flag=false;
        for(auto & iter_res : res_){
            if(re==iter_res){
                flag=true;
            }
        }
        if(!flag){
            res_.push_back(re);
        }
    }
    idx_=0;//迭代索引初始化
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    auto expr=plan_->filter_predicate_;
    auto table_info=GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_);//表信息
    while(true){
        if(idx_==res_.size()){
            return false;
        }
        auto [ba_meta,ba_tuple,ba_link] =bustub::GetTupleAndUndoLink(GetExecutorContext()->GetTransactionManager()
        ,table_info->table_.get(),res_[idx_]);
        auto local_ts=GetExecutorContext()->GetTransaction()->GetReadTs();
        //MVCC
        bool do_this=false;
        if(ba_meta.ts_<TXN_START_ID){
            if(ba_meta.ts_<=local_ts){//case 1
                do_this=true;
            }
        }
        else{
            if(ba_meta.ts_==GetExecutorContext()->GetTransaction()->GetTransactionTempTs()){//case 2
                do_this=true;
            }
        }
        if(do_this){//就处理基元组了
            if(ba_meta.is_deleted_){
                idx_++;
                continue;
            }
            if(expr!=nullptr&&!expr->Evaluate(&ba_tuple,GetOutputSchema()).GetAs<bool>()){
                idx_++;
                continue;
            }
            *rid=ba_tuple.GetRid();
            *tuple=ba_tuple;
            idx_++;
            return true;
        }
        //case 3 重构版本
        *rid=ba_tuple.GetRid();
        const auto &txnmgr=GetExecutorContext()->GetTransactionManager();
        std::optional<UndoLink> undo_link=txnmgr->GetUndoLink(*rid);
        std::optional<UndoLog> undo_log=std::nullopt;
        if(undo_link!=std::nullopt){
            undo_log=txnmgr->GetUndoLog(undo_link.value());
        }
        if(undo_log==std::nullopt){//不返回
            idx_++;
            continue;
        }
        std::vector<UndoLog> res_log;
        bool flag=false;
        bool delete_flag=false;
        while(true){
            res_log.push_back(*undo_log);
            if(undo_log->ts_<=local_ts){
                if(undo_log->is_deleted_){
                    delete_flag=true;
                }
                break;
            }
            if(undo_log->prev_version_.prev_txn_==INVALID_TXN_ID){//最后一个
                if(undo_log->ts_>local_ts){
                    flag=true;
                }
                break;
            }
            undo_log=txnmgr->GetUndoLog(undo_log->prev_version_);
        }
        if(flag||delete_flag){
            idx_++;
            continue;
        }
        auto re_ruple=bustub::ReconstructTuple(&GetOutputSchema(),ba_tuple,ba_meta,res_log);
        if(re_ruple.has_value()){
            if(expr!=nullptr&&!expr->Evaluate(&re_ruple.value(),GetOutputSchema()).GetAs<bool>()){
                idx_++;
                continue;
            }
            *tuple=re_ruple.value();
            idx_++;
            return true;
        }
        idx_++;
    }
    return true;
}

}  // namespace bustub
