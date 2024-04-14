#include "execution/executors/topn_executor.h"
#include <queue>
#include <utility>
#include <vector>
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
    child_executor_->Init();
    Tuple tuple;
    RID rid;
    res_.clear();
    auto cmp=[&](const Tuple& a, const Tuple& b) {
        for(const auto &iter:plan_->GetOrderBy()){
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
    };
    std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> heap(cmp);
    heap_n_=0;
    while (child_executor_->Next(&tuple,&rid)) {
        if(GetNumInHeap()<plan_->GetN()){
            heap.push(tuple);
            heap_n_++;
        }
        else{
            heap.push(tuple);
            heap.pop();
        }
    }
    idx_=0;
    while (!heap.empty()) {
        res_.push_back(heap.top());
        heap.pop();
    }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(idx_==res_.size()){
        return false;
    }
    *tuple=res_[res_.size()-1-idx_];
    *rid=tuple->GetRid();
    idx_++;
    return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
    return heap_n_;
};

}  // namespace bustub
