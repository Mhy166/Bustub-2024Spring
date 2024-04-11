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
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "execution/executors/insert_executor.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"

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
    Tuple insert_tuple;
    while(true){
        auto status=child_executor_->Next(&insert_tuple,rid);
        if(!status){//结束
            break;
        }
        auto table_cata=GetExecutorContext()->GetCatalog();
        auto table_info=table_cata->GetTable(plan_->GetTableOid());
        auto insert_oid=table_info->table_->InsertTuple({0,false},insert_tuple);
        if(insert_oid==std::nullopt){
            continue;
        }
        //成功插入影响了
        insert_sum++;
        //改索引
        auto table_indexs=table_cata->GetTableIndexes(table_info->name_);
        for(auto index_info : table_indexs){
            auto attrs=index_info->index_->GetKeyAttrs();
            auto key_schema=index_info->index_->GetKeySchema();
            index_info->index_->InsertEntry({insert_tuple.KeyFromTuple(table_info->schema_, *key_schema, attrs)}, *insert_oid, nullptr);
        }
    }
    //输出insert_sum
    flag_=true;
    std::vector<Value> out_value{{INTEGER,insert_sum}};
    out_value.reserve(1);
    *tuple=Tuple(out_value,&GetOutputSchema());
    return true;
}
}  // namespace bustub
