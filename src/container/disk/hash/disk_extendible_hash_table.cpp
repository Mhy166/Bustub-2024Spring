//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
        index_name_=name;
        WritePageGuard header_guard=bpm_->NewPageGuarded(&header_page_id_).UpgradeWrite();
        auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
        header_page->Init(header_max_depth);
        header_guard.Drop();
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint32_t hash=Hash(key);
  bool flag=false;
  ReadPageGuard header_guard=bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  page_id_t direct_id=header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash));
  if(direct_id==INVALID_PAGE_ID){
    return false;
  }
  ReadPageGuard direct_guard=bpm_->FetchPageRead(direct_id);
  auto direct_page=direct_guard.As<ExtendibleHTableDirectoryPage>();
  page_id_t bucket_id=direct_page->GetBucketPageId(direct_page->HashToBucketIndex(hash));
  if(bucket_id==INVALID_PAGE_ID){
    return false;
  }
  ReadPageGuard bucket_guard=bpm_->FetchPageRead(bucket_id);
  auto bucket_page=bucket_guard.As<ExtendibleHTableBucketPage<K,V,KC>>();
  V val;
  flag=bucket_page->Lookup(key,val,cmp_);
  if(flag){result->push_back(val);}
  direct_guard.Drop();
  header_guard.Drop();
  bucket_guard.Drop();
  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash=Hash(key);
  page_id_t direct_id=INVALID_PAGE_ID;
  page_id_t bucket_id=INVALID_PAGE_ID;
  bool flag=true;
  //寻找对应目录
  WritePageGuard header_guard=bpm_->FetchPageWrite(header_page_id_);
  auto header_page= header_guard.AsMut<ExtendibleHTableHeaderPage>();
  direct_id=header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash));
  //让目录出现,这个目录绝对会存在或者插入新的
  if(direct_id==INVALID_PAGE_ID){
    page_id_t new_directory_id;
    WritePageGuard direct_guard=bpm_->NewPageGuarded(&new_directory_id).UpgradeWrite();
    auto direct_page=direct_guard.AsMut<ExtendibleHTableDirectoryPage>();
    direct_page->Init(directory_max_depth_);
    header_page->SetDirectoryPageId(header_page->HashToDirectoryIndex(hash),new_directory_id);
    page_id_t new_bucket_id;
    WritePageGuard bucket_guard=bpm_->NewPageGuarded(&new_bucket_id).UpgradeWrite();
    auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
    bucket_page->Init(bucket_max_size_);
    bucket_guard.Drop();
    direct_page->SetBucketPageId(0,new_bucket_id);
    direct_guard.Drop();
  }
  direct_id=header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash));
  WritePageGuard direct_guard=bpm_->FetchPageWrite(direct_id);
  auto direct_page=direct_guard.AsMut<ExtendibleHTableDirectoryPage>();
  bucket_id=direct_page->GetBucketPageId(direct_page->HashToBucketIndex(hash));
  WritePageGuard bucket_guard=bpm_->FetchPageWrite(bucket_id);
  auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  V val;
  flag=bucket_page->Lookup(key,val,cmp_);
  if(flag){
    header_guard.Drop();
    direct_guard.Drop();
    bucket_guard.Drop();
    return false;
  }
  //此时没查到，桶中无此条目！下面看未满时！
  if(!bucket_page->IsFull()){
    bucket_page->Insert(key,value,cmp_);
    header_guard.Drop();
    direct_guard.Drop();
    bucket_guard.Drop();
    return true;
  }
  //此时桶必是满的！需要分裂！
  if(direct_page->GetGlobalDepth()==direct_page->GetLocalDepth(direct_page->HashToBucketIndex(hash))){
    if(direct_page->GetGlobalDepth()==directory_max_depth_){
      header_guard.Drop();
      direct_guard.Drop();
      bucket_guard.Drop();
      return false;
    }
    direct_page->IncrGlobalDepth();
  }
  //至此一定保证可以进行本地扩展了,先创建新桶
  page_id_t new_bucket_id;
  WritePageGuard new_bucket_guard=bpm_->NewPageGuarded(&new_bucket_id).UpgradeWrite();
  auto new_bucket_page=new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  new_bucket_page->Init(bucket_max_size_);
  //new_bucket_guard.Drop();
  //更改映射桶
  //链接映射
  uint32_t cl=direct_page->GetLocalDepth(direct_page->HashToBucketIndex(hash));
  for(uint32_t i=0;i<direct_page->Size();i++){
    if(direct_page->GetBucketPageId(i)==bucket_id){
      direct_page->IncrLocalDepth(i);
      if((i&static_cast<uint32_t>(pow(2,cl)))!=0){
        direct_page->SetBucketPageId(i,new_bucket_id);
      }
    }
  }
  //桶内容分配
  std::vector<std::pair<K, V>> tmp;
  for(uint32_t i=0;i<bucket_page->Size();i++){
    tmp.push_back(std::make_pair(bucket_page->KeyAt(i), bucket_page->ValueAt(i)));
  }
  bucket_page->Init(bucket_max_size_);
  for(uint32_t i=0;i<tmp.size();i++){
    if((Hash(tmp[i].first)&static_cast<uint32_t>(pow(2,cl)))!=0){
      new_bucket_page->Insert(tmp[i].first,tmp[i].second,cmp_);   
    }
    else{
      bucket_page->Insert(tmp[i].first,tmp[i].second,cmp_);
    }
  }
  header_guard.Drop();
  direct_guard.Drop();
  bucket_guard.Drop();
  new_bucket_guard.Drop();
  return Insert(key,value,transaction);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash=Hash(key);
  page_id_t direct_id=INVALID_PAGE_ID;
  page_id_t bucket_id=INVALID_PAGE_ID;
  bool flag=false;
  WritePageGuard header_guard=bpm_->FetchPageWrite(header_page_id_);
  auto header_page= header_guard.AsMut<ExtendibleHTableHeaderPage>();
  direct_id=header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash));
  if(direct_id==INVALID_PAGE_ID){
    return false;
  }

  WritePageGuard direct_guard=bpm_->FetchPageWrite(direct_id);
  auto direct_page=direct_guard.AsMut<ExtendibleHTableDirectoryPage>();
  bucket_id=direct_page->GetBucketPageId(direct_page->HashToBucketIndex(hash));
  if(bucket_id==INVALID_PAGE_ID){
    return false;
  }
  WritePageGuard bucket_guard=bpm_->FetchPageWrite(bucket_id);
  auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  V val;
  flag=bucket_page->Lookup(key,val,cmp_);
  if(!flag){
    return false;
  }
  //查到了！
  bucket_page->Remove(key,cmp_);
  if(bucket_page->IsEmpty()){
    uint32_t bucket_idx=direct_page->HashToBucketIndex(hash);//空桶的索引
    if(direct_page->GetGlobalDepth()==0){
      header_page->SetDirectoryPageId(header_page->HashToDirectoryIndex(hash), INVALID_PAGE_ID);
      direct_page->SetBucketPageId(0, INVALID_PAGE_ID);
      return true;
    }
    //目录至少有俩,先找image
    uint32_t bucket_image_idx;
    if(bucket_idx<static_cast<uint32_t>(pow(2,direct_page->GetLocalDepth(bucket_idx)-1))){
      bucket_image_idx=bucket_idx+static_cast<uint32_t>(pow(2, direct_page->GetLocalDepth(bucket_idx)-1));
    }
    else{
      bucket_image_idx=bucket_idx-static_cast<uint32_t>(pow(2, direct_page->GetLocalDepth(bucket_idx)-1));
    }
    if(direct_page->GetLocalDepth(bucket_idx)!=direct_page->GetLocalDepth(bucket_image_idx)){
      return true;
    }
    page_id_t bucket_image_id=direct_page->GetBucketPageId(bucket_image_idx);
    //不涉及桶内迁移，只涉及链接迁移
    for(uint32_t i=0;i<direct_page->Size();i++){
      if(direct_page->GetBucketPageId(i)==bucket_id){
        direct_page->SetBucketPageId(i, bucket_image_id);
        direct_page->DecrLocalDepth(i);
      }
      else if(direct_page->GetBucketPageId(i)==bucket_image_id){
        direct_page->DecrLocalDepth(i);
      }
    }
    bpm_->DeletePage(bucket_id);
    if(direct_page->CanShrink()){
      direct_page->DecrGlobalDepth();
    }
    direct_guard.Drop();
    header_guard.Drop();
    bucket_guard.Drop();
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
