//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/page/hash_table_directory_page.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)){
        htble_=std::make_unique<SimpleAggregationHashTable>(plan->GetAggregates(),plan->GetAggregateTypes());
        iter_=std::make_unique<SimpleAggregationHashTable::Iterator>(htble_->Begin());
    }
    

void AggregationExecutor::Init() {
    child_executor_->Init();
    htble_->Clear();
    Tuple tuple;
    RID rid;
    htble_->GenerateInitialAggregateValue();
    while(child_executor_->Next(&tuple, &rid)){
        flag_=true;
        AggregateKey akey=MakeAggregateKey(&tuple);
        AggregateValue avalue=MakeAggregateValue(&tuple);
        htble_->InsertCombine(akey, avalue);
    }
    *iter_=htble_->Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    while((*iter_)!=htble_->End()){
        auto &key=iter_->Key();
        auto &val=iter_->Val();
        std::vector<Value> out;
        for(auto &iter:key.group_bys_){
            out.push_back(iter);
        }
        for(auto &iter:val.aggregates_){
            out.push_back(iter);
        }
        ++(*iter_);
        *tuple=Tuple(out,&GetOutputSchema());
        *rid=tuple->GetRid();
        return true;
    }
    if(!flag_&&plan_->group_bys_.empty()){
        std::vector<Value> out;
        for(auto &iter:plan_->agg_types_){
            if(iter==AggregationType::CountStarAggregate){
                out.push_back(ValueFactory::GetIntegerValue(0));
            }
            else{
                out.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
            }
        }
        *tuple=Tuple(out,&GetOutputSchema());
        *rid=tuple->GetRid();
        flag_=true;
        return true;
    }
    return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
