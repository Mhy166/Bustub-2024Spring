#include "execution/executors/window_function_executor.h"
#include <sys/types.h>
#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "binder/bound_order_by.h"
#include "catalog/column.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}
    
auto WindowFunctionExecutor::GenerateInitialAggregateValue(std::unordered_map<WindowKey, WindowValue>& ht,std::vector<AbstractExpressionRef>& expr,
    std::vector<WindowFunctionType>& ty,std::vector<bool>& is_flag) -> WindowValue {
    std::vector<Value> values{};
    for (const auto &agg_type : ty) {
      switch (agg_type) {
        case WindowFunctionType::CountStarAggregate:
          // Count start starts at zero.
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
        case WindowFunctionType::CountAggregate:
        case WindowFunctionType::SumAggregate:
        case WindowFunctionType::MinAggregate:
        case WindowFunctionType::MaxAggregate:
          // Others starts at null.
          values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
        case WindowFunctionType::Rank:
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
      }
    }
    return {values};
  }

  void WindowFunctionExecutor::CombineAggregateValues(WindowValue *result, const WindowValue &input,std::unordered_map<WindowKey, WindowValue>& ht,std::vector<AbstractExpressionRef>& expr,
    std::vector<WindowFunctionType>& ty,std::vector<bool>& is_flag,int32_t& rank) {
    for (uint32_t i = 0; i < expr.size(); i++) {
      switch (ty[i]) {
        case WindowFunctionType::CountStarAggregate:
        {
          Value one=ValueFactory::GetIntegerValue(1);
          result->aggregates_[i]=one.Add(result->aggregates_[i]);
        }
          break;
        case WindowFunctionType::CountAggregate:
        {
          if(!input.aggregates_[i].IsNull()){
            if(result->aggregates_[i].IsNull()){
              result->aggregates_[i]=ValueFactory::GetIntegerValue(1);
            }
            else{
              Value one=ValueFactory::GetIntegerValue(1);
              result->aggregates_[i]=one.Add(result->aggregates_[i]);
            }
          }
        }
          break;
        case WindowFunctionType::SumAggregate:
        {
          if(!input.aggregates_[i].IsNull()){
            if(result->aggregates_[i].IsNull()){
              result->aggregates_[i]=input.aggregates_[i];
            }
            else{
              result->aggregates_[i]=input.aggregates_[i].Add(result->aggregates_[i]);
            }
          }
        }
          break;
        case WindowFunctionType::MinAggregate:
        {
          if(!input.aggregates_[i].IsNull()){
            if(result->aggregates_[i].IsNull()){
              result->aggregates_[i]=input.aggregates_[i];
            }
            else{
              if(result->aggregates_[i].CompareGreaterThan(input.aggregates_[i])==CmpBool::CmpTrue){
                result->aggregates_[i]=input.aggregates_[i];
              }
            }
          }
        }
          break;
        case WindowFunctionType::MaxAggregate:
        {
          if(!input.aggregates_[i].IsNull()){
            if(result->aggregates_[i].IsNull()){
              result->aggregates_[i]=input.aggregates_[i];
            }
            else{
              if(result->aggregates_[i].CompareLessThan(input.aggregates_[i])==CmpBool::CmpTrue){
                result->aggregates_[i]=input.aggregates_[i];
              }
            }
          }
        }
          break;
        case WindowFunctionType::Rank:
        {
          const auto&expr=dynamic_cast<const ColumnValueExpression*>(plan_->window_functions_.begin()->second.order_by_[0].second.get());
          if(idx_!=0&&expr->Evaluate(&res_[idx_],child_executor_->GetOutputSchema()).CompareEquals(expr->Evaluate(&res_[idx_-1],child_executor_->GetOutputSchema()))==CmpBool::CmpTrue){
            result->aggregates_[i]=result->aggregates_[i];
            rank++;
          }
          else{
            result->aggregates_[i]=result->aggregates_[i].Add(ValueFactory::GetIntegerValue(rank));
            rank=1;
          }
        }
          break;
      }
    }
  }

void WindowFunctionExecutor::InsertCombine(std::unordered_map<WindowKey, WindowValue>& ht,const WindowKey &agg_key, const WindowValue &agg_val,std::vector<AbstractExpressionRef>& expr,
    std::vector<WindowFunctionType>& ty,std::vector<bool>& is_flag,int32_t&rank) {
    if (ht.count(agg_key) == 0) {
      ht.insert({agg_key, GenerateInitialAggregateValue(ht,expr,ty,is_flag)});
    }
    CombineAggregateValues(&ht[agg_key], agg_val,ht,expr,ty,is_flag,rank);
}
void WindowFunctionExecutor::Init() {
    child_executor_->Init();
    uint32_t i=0;auto it=plan_->window_functions_.begin();
    //总哈希拆分组合
    std::vector<AbstractExpressionRef> expr;
    std::vector<WindowFunctionType> ty;
    std::vector<bool>is_flag;
    std::vector<std::unordered_map<WindowKey, WindowValue>> ht;
    std::vector<std::vector<AbstractExpressionRef>> expr_f;
    std::vector<AbstractExpressionRef> part_f;
    std::vector<std::vector<WindowFunctionType>> ty_f;
    std::vector<std::vector<bool>> is_flag_f;
    while(i<plan_->window_functions_.size()){
        expr_f.push_back(expr);
        ty_f.push_back(ty);
        is_flag_f.push_back(is_flag);
        expr_f[i].push_back(it->second.function_);
        ty_f[i].push_back(it->second.type_);
        if(it->second.partition_by_.empty()){
            is_flag_f[i].push_back(false);
            part_f.push_back(nullptr);
        }
        else{
            is_flag_f[i].push_back(true);
            part_f.push_back(it->second.partition_by_[0]);
        }
        i++;
        it++;
    }
    //wht_=std::make_unique<WindowHashTable>(expr,ty,is_flag);
    for(uint32_t id=0;id<expr_f.size();id++){
        std::unordered_map<WindowKey, WindowValue> ht0{};
        ht.push_back(ht0);
    }

    res_.clear();

    Tuple tuple;
    RID rid;
    while(child_executor_->Next(&tuple, &rid)){
        res_.push_back(tuple);
    }

    //先排序
    const auto &order_expr=plan_->window_functions_.begin()->second.order_by_;
    if(!order_expr.empty()){
        order_flag_=true;
    }
    if(order_flag_){//有order by
        std::sort(res_.begin(),res_.end(), [&](const Tuple& a, const Tuple& b) {
            for(const auto &iter:order_expr){
                Value va=iter.second->Evaluate(&a, plan_->GetChildPlan()->OutputSchema());
                Value vb=iter.second->Evaluate(&b, plan_->GetChildPlan()->OutputSchema());
                if(iter.first==OrderByType::DESC){
                    if(va.CompareGreaterThan(vb)==CmpBool::CmpTrue){
                        return true;
                    }
                    if(va.CompareLessThan(vb)==CmpBool::CmpTrue){
                        return false;
                    }
                    if(va.CompareEquals(vb)==CmpBool::CmpTrue){
                        continue;
                    }
                }
                if(iter.first==OrderByType::ASC||iter.first==OrderByType::DEFAULT){
                    if(va.CompareLessThan(vb)==CmpBool::CmpTrue){
                        return true;
                    }
                    if(va.CompareGreaterThan(vb)==CmpBool::CmpTrue){
                        return false;
                    }
                    if(va.CompareEquals(vb)==CmpBool::CmpTrue){
                        continue;
                    }
                }
            }
            return true;
        });
    }
    idx_=0;
    //出结果。
    if(order_flag_){
        while(idx_<res_.size()){
            //对于每一个tuple，遍历哈希
            for(uint32_t i=0;i<expr_f.size();i++){
                if(is_flag_f[i][0]){
                    //有partition
                    std::vector<Value> wkey_image;
                    WindowValue wval=MakeWindowValue(&res_[idx_]);
                    const auto&expr=dynamic_cast<const ColumnValueExpression*>(part_f[i].get());
                    wkey_image.emplace_back(expr->Evaluate(&res_[idx_], child_executor_->GetOutputSchema()));
                    WindowKey wkey={wkey_image};
                    InsertCombine(ht[i],wkey,wval,expr_f[i],ty_f[i],is_flag_f[i],rank_);
                }
                else{
                    //无partition
                    WindowValue wval=MakeWindowValue(&res_[idx_]);
                    std::vector<Value> wkey_image;
                    wkey_image.push_back(ValueFactory::GetIntegerValue(1));
                    WindowKey wkey={wkey_image};
                    InsertCombine(ht[i],wkey,wval,expr_f[i],ty_f[i],is_flag_f[i],rank_);
                }
            }
            std::vector<Value> out_value;
            uint32_t colidx=ht.size();
            for(const auto& iter:plan_->columns_){
                const auto&ex=dynamic_cast<const ColumnValueExpression*>(iter.get());
                if(ex->GetColIdx()<1314520){//lyh
                    out_value.push_back(iter->Evaluate(&res_[idx_], child_executor_->GetOutputSchema()));
                }
                else{//空的,输出聚合
                    for(const auto& iter:ht[colidx-1]){
                        
                        if(is_flag_f[colidx-1][0]){
                            std::vector<Value> wkey_image;
                            WindowValue wval=MakeWindowValue(&res_[idx_]);
                            const auto&expr=dynamic_cast<const ColumnValueExpression*>(part_f[colidx-1].get());
                            wkey_image.emplace_back(expr->Evaluate(&res_[idx_], child_executor_->GetOutputSchema()));
                            WindowKey wkey={wkey_image};
                            if(wkey==iter.first){
                                out_value.push_back(iter.second.aggregates_[0]);
                                colidx--;
                                break;
                            }
                        }
                        else{
                            out_value.push_back(iter.second.aggregates_[0]);
                            colidx--;
                            break;
                        }
                    }
                }
            }
            tuple=Tuple(out_value,&GetOutputSchema());
            rest_.push_back(tuple);
            idx_++;
        }
        
    }   
    else{//没顺序
        while(idx_<res_.size()){
            //对于每一个tuple，遍历哈希
            for(uint32_t i=0;i<expr_f.size();i++){
                if(is_flag_f[i][0]){
                    //有partition
                    std::vector<Value> wkey_image;
                    WindowValue wval=MakeWindowValue(&res_[idx_]);
                    const auto&expr=dynamic_cast<const ColumnValueExpression*>(part_f[i].get());
                    wkey_image.emplace_back(expr->Evaluate(&res_[idx_], child_executor_->GetOutputSchema()));
                    WindowKey wkey={wkey_image};
                    InsertCombine(ht[i],wkey,wval,expr_f[i],ty_f[i],is_flag_f[i],rank_);
                }
                else{
                    //无partition
                    WindowValue wval=MakeWindowValue(&res_[idx_]);
                    std::vector<Value> wkey_image;
                    wkey_image.push_back(ValueFactory::GetIntegerValue(1));
                    WindowKey wkey={wkey_image};
                    InsertCombine(ht[i],wkey,wval,expr_f[i],ty_f[i],is_flag_f[i],rank_);
                }
            }
            idx_++;
        }
        idx_=0;
        while(idx_<res_.size()){
            std::vector<Value> out_value;
                uint32_t colidx=ht.size();
                for(const auto& iter:plan_->columns_){
                    const auto&ex=dynamic_cast<const ColumnValueExpression*>(iter.get());
                    if(ex->GetColIdx()<1314520){
                        out_value.push_back(iter->Evaluate(&res_[idx_], child_executor_->GetOutputSchema()));
                    }
                    else{//空的,输出聚合
                        for(const auto& iter:ht[colidx-1]){
                            
                            if(is_flag_f[colidx-1][0]){
                                std::vector<Value> wkey_image;
                                WindowValue wval=MakeWindowValue(&res_[idx_]);
                                const auto&expr=dynamic_cast<const ColumnValueExpression*>(part_f[colidx-1].get());
                                wkey_image.emplace_back(expr->Evaluate(&res_[idx_], child_executor_->GetOutputSchema()));
                                WindowKey wkey={wkey_image};
                                if(wkey==iter.first){
                                    out_value.push_back(iter.second.aggregates_[0]);
                                    colidx--;
                                    break;
                                }
                            }
                            else{
                                out_value.push_back(iter.second.aggregates_[0]);
                                colidx--;
                                break;
                            }
                        }
                    }
                }
            tuple=Tuple(out_value,&GetOutputSchema());
            rest_.push_back(tuple);
            idx_++;
        }
    }

    idx_=0;
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(idx_==rest_.size()){
        return false;
    }
    *tuple=rest_[idx_];
    *rid=tuple->GetRid();
    idx_++;
    return true; 
}
}  // namespace bustub
