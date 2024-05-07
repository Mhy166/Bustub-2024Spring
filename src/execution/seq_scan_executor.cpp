//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "common/config.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "concurrency/transaction_manager.h"
namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),plan_(plan) {}

void SeqScanExecutor::Init() {
    auto table_info=GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
    iter_.emplace(table_info->table_->MakeIterator());
}
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    auto expr=plan_->filter_predicate_;
    while(true){
        if(iter_->IsEnd()){
            return false;
        }
        //未结束呢，针对某个元组
        auto [meta_base, tuple_base] = iter_->GetTuple();
        auto local_ts=GetExecutorContext()->GetTransaction()->GetReadTs();
        
        //MVCC
        bool do_this=false;
        if(meta_base.ts_<TXN_START_ID){
            if(meta_base.ts_<=local_ts){//case 1
                do_this=true;
            }
        }
        else{
            if(meta_base.ts_==GetExecutorContext()->GetTransaction()->GetTransactionTempTs()){//case 2
                do_this=true;
            }
        }
        if(do_this){//就处理基元组了
            if(meta_base.is_deleted_){
                ++(*iter_);
                continue;
            }
            if(expr!=nullptr&&!expr->Evaluate(&tuple_base,GetOutputSchema()).GetAs<bool>()){
                ++(*iter_);
                continue;
            }
            *rid=tuple_base.GetRid();
            *tuple=tuple_base;
            ++(*iter_);
            return true;
        }
        //case 3 重构版本
        *rid=tuple_base.GetRid();
        const auto &txnmgr=GetExecutorContext()->GetTransactionManager();
        std::optional<UndoLink> undo_link=txnmgr->GetUndoLink(*rid);
        std::optional<UndoLog> undo_log=std::nullopt;
        if(undo_link!=std::nullopt){
            undo_log=txnmgr->GetUndoLog(undo_link.value());
        }
        if(undo_log==std::nullopt){//不返回
            ++(*iter_);
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
            ++(*iter_);
            continue;
        }
        auto re_ruple=bustub::ReconstructTuple(&GetOutputSchema(),tuple_base,meta_base,res_log);
        if(re_ruple.has_value()){
            if(expr!=nullptr&&!expr->Evaluate(&re_ruple.value(),GetOutputSchema()).GetAs<bool>()){
                ++(*iter_);
                continue;
            }
            *tuple=re_ruple.value();
            ++(*iter_);
            return true;
        }
        ++(*iter_);
    }
}
}  // namespace bustub