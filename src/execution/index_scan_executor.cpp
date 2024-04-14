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
        if(table_info->table_->GetTuple(res_[idx_]).first.is_deleted_){
            idx_++;
            continue;
        }
        Tuple tuple_con=table_info->table_->GetTuple(res_[idx_]).second;
        if(expr!=nullptr){
            if(!expr->Evaluate(&tuple_con,GetOutputSchema()).GetAs<bool>()){
                idx_++;
                continue;
            }
        }
        break;
    }
    *tuple=table_info->table_->GetTuple(res_[idx_]).second;
    *rid=res_[idx_];
    idx_++;
    return true;
}

}  // namespace bustub
