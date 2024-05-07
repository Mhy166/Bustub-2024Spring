#include "execution/execution_common.h"
#include <cstdint>
#include <cstdio>
#include <optional>
#include <vector>
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::optional<Tuple> tuple;
  std::optional<Tuple> sub_tuple=base_tuple;
  if(base_meta.is_deleted_){
    tuple=std::nullopt;
  }
  else{
    tuple=base_tuple;
  }
  for(const auto& log_iter:undo_logs){
    if(log_iter.is_deleted_){
      tuple=std::nullopt;
      continue;
    }
    std::vector<uint32_t> modif;
    for(uint32_t i=0;i<log_iter.modified_fields_.size();i++){
      if(log_iter.modified_fields_[i]){//重建
        modif.push_back(i);
      }
    }
    auto par_schema=bustub::Schema::CopySchema(schema, modif);
    std::vector<Value> tmp;
    for(uint32_t i=0,j=0;i<log_iter.modified_fields_.size();i++){
      if(log_iter.modified_fields_[i]){//重建
        tmp.push_back(log_iter.tuple_.GetValue(&par_schema, j));
        j++;
      }
      else{
        tmp.push_back(sub_tuple->GetValue(schema,i));
      }
    }
    tuple=Tuple(tmp,schema);
    sub_tuple=tuple;
  }
  return tuple;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr,"------------------------------------------------");
  fmt::println(stderr, "debug_hook: {}", info);
  fmt::println(stderr,"table_oid:{}",table_info->oid_);
  fmt::println(stderr," ");
  auto iter=table_heap->MakeIterator();
  auto schema=table_info->schema_;
  for(;!iter.IsEnd();){
    auto [meta,tuple]=iter.GetTuple();
    fmt::print(stderr,"|RID={}/{}  |",tuple.GetRid().GetPageId(),tuple.GetRid().GetSlotNum());
    if(meta.ts_<TXN_START_ID){
      fmt::print(stderr,"ts={}  |",meta.ts_);
    }
    else{
      fmt::print(stderr,"ts=txn{}  |",meta.ts_-TXN_START_ID);
    }
    if(meta.is_deleted_){
      fmt::print(stderr," <deleted> ");
    }
    fmt::print(stderr,"tuple=(");
    for(uint32_t i=0;i<schema.GetColumns().size();i++){
      auto value=tuple.GetValue(&schema,i);
      if(value.IsNull()){
        fmt::print(stderr,"<NULL>");
      }
      else{
        fmt::print(stderr," {}",value);
      }
      if(i!=schema.GetColumns().size()-1){
        fmt::print(stderr,",");
      }
      else{
        fmt::print(stderr," ");
      }
    }
    fmt::println(stderr,")  |");
    //输出版本链
    auto undolink=txn_mgr->GetUndoLink(tuple.GetRid());
    std::optional<UndoLog> undolog=std::nullopt;
    if(undolink==std::nullopt){
      ++iter;
      continue;
    }
    std::vector<UndoLog> remake_log; 
    while(true){
      if(txn_mgr->txn_map_.find(undolink->prev_txn_)==txn_mgr->txn_map_.end()){
        break;
      }
      undolog=txn_mgr->GetUndoLog(undolink.value());
      fmt::print(stderr,"  -->txn{}@{} : ",undolink->prev_txn_-TXN_START_ID,undolink->prev_log_idx_);
      remake_log.push_back(undolog.value());
      auto remake_tuple=bustub::ReconstructTuple(&schema,tuple,meta,remake_log);
      if(remake_tuple==std::nullopt){
        fmt::print(stderr,"is deleted ");
      }
      else{
        fmt::print(stderr,"tuple=(");
        for(uint32_t i=0;i<schema.GetColumns().size();i++){
          auto value=remake_tuple->GetValue(&schema,i);
          if(value.IsNull()){
            fmt::print(stderr,"<NULL>");
          }
          else{
            fmt::print(stderr," {}",value);
          }
          if(i!=schema.GetColumns().size()-1){
            fmt::print(stderr,",");
          }
          else{
            fmt::print(stderr," ");
          }
        }
        fmt::print(stderr,") ");
      }
      fmt::println(stderr,"ts={}",undolog->ts_);
      if(undolog->prev_version_.prev_txn_==INVALID_TXN_ID){
        break;
      }
      undolink=undolog->prev_version_;
    }
    fmt::println(stderr," ");
    ++iter;
  }
  fmt::println(stderr,"------------------------------------------------");
    // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}
}  // namespace bustub
