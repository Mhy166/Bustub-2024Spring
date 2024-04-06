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
#include <utility>
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
    plan_=plan;
    auto table_info=GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
    iter_=std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(iter_->IsEnd()){
        return false;
    }
    while(iter_->GetTuple().first.is_deleted_){
        ++(*iter_);
        if(iter_->IsEnd()){
            return false;
        }
    }
    *tuple=iter_->GetTuple().second;
    *rid=iter_->GetRID();
    ++(*iter_);
    return true;
}
}  // namespace bustub
