//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <atomic>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/timestamp_type.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));
  txn_ref->read_ts_=last_commit_ts_.load();
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);
  if (txn->state_ != TransactionState::RUNNING) {//确保运行中才能提交
    throw Exception("txn not in running state");
  }
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  timestamp_t commit_s=last_commit_ts_.load();
  commit_s++;
  for(auto & iter : txn->write_set_){
    auto table=catalog_->GetTable(iter.first);
    for(auto iter1 : iter.second){
      auto [meta, tuple] =table->table_->GetTuple(iter1);
      if(meta.ts_!=0){
        table->table_->UpdateTupleInPlace({commit_s,meta.is_deleted_}, tuple, iter1);
      }
    }
  }
  last_commit_ts_++;
  txn->commit_ts_=last_commit_ts_.load();
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  timestamp_t water=GetWatermark();
  
  std::unordered_map<txn_id_t, uint32_t>txns;
  for(auto &iter:txn_map_){
    auto txn=iter.second;
    if(txn->state_==TransactionState::ABORTED||txn->state_==TransactionState::COMMITTED){
      txns[iter.first]=0;
    }
    for(auto &iterset:txn->write_set_){
      t_oit_=iterset.first;
    }
  }
  auto table_info=catalog_->GetTable(t_oit_);
  auto iter=table_info->table_->MakeIterator();
  while(!iter.IsEnd()){
    bool flag=true;
    if(iter.GetTuple().first.ts_<=water){
      flag=false;
    }
    auto prv_link=GetUndoLink(iter.GetRID());
    std::optional<UndoLog> prv_log=std::nullopt;
    while(prv_link!=std::nullopt&&txn_map_.find(prv_link->prev_txn_)!=txn_map_.end()){
      prv_log=GetUndoLog(prv_link.value());
      if(prv_log->ts_<=water){
        if(flag){
          flag=false;
        }
        else{
          txns[prv_link->prev_txn_]++;
        }
      }
      if(prv_log->prev_version_.prev_txn_!=INVALID_TXN_ID){
        prv_link=prv_log->prev_version_;
      }
      else{
        prv_link=std::nullopt;
      }
    }
    ++iter;
  }
  for(auto it:txns){
    if(txn_map_[it.first]->undo_logs_.size()==it.second){
      txn_map_.erase(it.first);
    }
  }
}

}  // namespace bustub
