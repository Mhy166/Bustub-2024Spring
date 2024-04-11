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

#include <memory>
#include <utility>
#include <vector>

#include "execution/executors/delete_executor.h"
#include "type/type_id.h"
#include "type/value.h"

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
        table_info->table_->UpdateTupleMeta({0,true},*rid);
        auto table_indexs=GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
        for(auto index_info : table_indexs){
            auto attrs=index_info->index_->GetKeyAttrs();
            auto key_schema=index_info->index_->GetKeySchema();
            index_info->index_->DeleteEntry(delete_tuple.KeyFromTuple(table_info->schema_, *key_schema,attrs), *rid,nullptr);
        }
    }

    flags_=true;
    std::vector<Value> delete_val{{INTEGER,delete_num}};
    delete_val.reserve(1);
    *tuple=Tuple(delete_val,&GetOutputSchema());
    return true;
}

}  // namespace bustub
