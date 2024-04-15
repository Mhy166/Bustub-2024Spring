#include "concurrency/watermark.h"
#include <cstdint>
#include <exception>
#include "common/exception.h"
#include "storage/table/tuple.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  if(current_reads_.find(read_ts)==current_reads_.end()){
    current_reads_[read_ts]=0;
  }
  current_reads_[read_ts]++;
  for(timestamp_t i=watermark_;;i++){
    if(current_reads_.find(i)!=current_reads_.end()){
      watermark_=i;
      break;
    }
  }
  // TODO(fall2023): implement me!
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  
  current_reads_[read_ts]--;
  if(current_reads_[read_ts]==0){
    current_reads_.erase(read_ts);
  }
  if(current_reads_.empty()){
    return;
  }
  for(timestamp_t i=watermark_;;i++){
    if(current_reads_.find(i)!=current_reads_.end()){
      watermark_=i;
      break;
    }
  }
}

}  // namespace bustub
