//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"
#include <cmath>

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  for(uint32_t i=0;i<HTABLE_DIRECTORY_ARRAY_SIZE;i++){
    local_depths_[i]=0;
    bucket_page_ids_[i]=INVALID_PAGE_ID;
  }
  max_depth_=max_depth;
  global_depth_=0;
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash&((1<<global_depth_)-1);
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
   return bucket_page_ids_[bucket_idx]; 
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx]=bucket_page_id;
}
//____________________________________________________________________________________________________________
auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  if(bucket_idx<static_cast<uint32_t>(pow(2, global_depth_))){
    return bucket_idx+static_cast<uint32_t>(pow(2, global_depth_));
  }
  return bucket_idx-static_cast<uint32_t>(pow(2, global_depth_));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
   return global_depth_;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if(global_depth_>=max_depth_){
    return;
  }
  for(uint32_t i=0;i<static_cast<uint32_t>(pow(2,global_depth_));i++) {
    bucket_page_ids_[GetSplitImageIndex(i)]=bucket_page_ids_[i];
    local_depths_[GetSplitImageIndex(i)]=local_depths_[i];
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  global_depth_--;
  for(uint32_t i=0;i<static_cast<uint32_t>(pow(2,global_depth_));i++) {
    bucket_page_ids_[GetSplitImageIndex(i)]=INVALID_PAGE_ID;
    local_depths_[GetSplitImageIndex(i)]=0;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  bool flag=true;
  for(unsigned char local_depth : local_depths_){
    if(local_depth>=global_depth_){
      flag=false;
    }
  }
  return flag;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  uint64_t count=0;
  for(int bucket_page_id : bucket_page_ids_)
  {
    if(bucket_page_id!=INVALID_PAGE_ID){
      count++;
    }
  }
  return count;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx]; 
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx]=local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  local_depths_[bucket_idx]--;
}

}  // namespace bustub