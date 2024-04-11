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
#include "storage/table/tuple.h"

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
        if(iter_->GetTuple().first.is_deleted_){
            ++(*iter_);
            continue;
        }
        Tuple tuple_con=(iter_->GetTuple().second);
        if(expr!=nullptr){
            if(!expr->Evaluate(&tuple_con,GetOutputSchema()).GetAs<bool>()){
                ++(*iter_);
                continue;
            }
        }
        break;
    }
    *tuple=iter_->GetTuple().second;
    *rid=iter_->GetRID();
    ++(*iter_);
    return true;
}
}  // namespace bustub
