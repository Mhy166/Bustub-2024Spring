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

#include <cmath>
#include <sys/types.h>
#include <cstdint>
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
#include "concurrency/transaction.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"
#include "type/value.h"

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
        BasicPageGuard header_guard=bpm_->NewPageGuarded(&header_page_id_);
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
  header_guard.Drop();

  if(direct_id==INVALID_PAGE_ID){
    return false;
  }

  ReadPageGuard direct_guard=bpm_->FetchPageRead(direct_id);
  auto direct_page=direct_guard.As<ExtendibleHTableDirectoryPage>();
  page_id_t bucket_id=direct_page->GetBucketPageId(direct_page->HashToBucketIndex(hash));
  direct_guard.Drop();

  if(bucket_id==INVALID_PAGE_ID){
    return false;
  }
  
  ReadPageGuard bucket_guard=bpm_->FetchPageRead(bucket_id);
  auto bucket_page=bucket_guard.As<ExtendibleHTableBucketPage<K,V,KC>>();
  V val;
  flag=bucket_page->Lookup(key,val,cmp_);
  if(flag){result->push_back(val);}
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
  bool flag=false;
  //寻找对应目录
  ReadPageGuard header_guard=bpm_->FetchPageRead(header_page_id_);
  auto header_page= header_guard.As<ExtendibleHTableHeaderPage>();
  direct_id=header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash));
  header_guard.Drop();

  //让目录出现,这个目录绝对会存在或者插入新的
  if(direct_id==INVALID_PAGE_ID){
    WritePageGuard header_guard=bpm_->FetchPageWrite(header_page_id_);
    auto header_page=header_guard.AsMut<ExtendibleHTableHeaderPage>();
    InsertToNewDirectory(header_page, header_page->HashToDirectoryIndex(hash), hash, key, value);
    direct_id=header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash));
    header_guard.Drop();
  }

  //寻找桶,至此direct_id必定有了！
  {
  ReadPageGuard direct_guard=bpm_->FetchPageRead(direct_id);
  auto direct_page=direct_guard.As<ExtendibleHTableDirectoryPage>();
  bucket_id=direct_page->GetBucketPageId(direct_page->HashToBucketIndex(hash));
  direct_guard.Drop();
  }
  //让桶出现
  if(bucket_id==INVALID_PAGE_ID){
    flag=true;
    WritePageGuard direct_guard=bpm_->FetchPageWrite(direct_id);
    auto direct_page=direct_guard.AsMut<ExtendibleHTableDirectoryPage>();
    InsertToNewBucket(direct_page, direct_page->HashToBucketIndex(hash), key, value);
    bucket_id=direct_page->GetBucketPageId(direct_page->HashToBucketIndex(hash));
    direct_guard.Drop();
  }
  //寻找键值对,至此bucket_id必定有了！
  if(flag){
    return true;
  }
  //原来页面的保护
  WritePageGuard bucket_guard=bpm_->FetchPageWrite(bucket_id);
  auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  V val;
  flag=bucket_page->Lookup(key,val,cmp_);
  if(flag){
    return false;
  }
  //此时没查到，桶中无此条目！下面看未满时！
  if(!bucket_page->IsFull()){
    bucket_page->Insert(key,value,cmp_);
    return true;
  }
  //此时桶必是满的！需要分裂！
  WritePageGuard direct_guard_split=bpm_->FetchPageWrite(direct_id);
  auto direct_page_split=direct_guard_split.AsMut<ExtendibleHTableDirectoryPage>();
  //扩展深度
  if(direct_page_split->GetGlobalDepth()==direct_page_split->GetLocalDepth(direct_page_split->HashToBucketIndex(hash))){
    if(direct_page_split->GetGlobalDepth()==directory_max_depth_){
      return false;
    }
    direct_page_split->IncrGlobalDepth();
  }
  //至此一定保证可以进行本地扩展了,先创建新桶
  page_id_t new_bucket_id;
  BasicPageGuard new_bucket_guard=bpm_->NewPageGuarded(&new_bucket_id);
  auto new_bucket_page=new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  new_bucket_page->Init(bucket_max_size_);
  //new_bucket_guard.Drop();
  //更改映射桶
  //链接映射
  uint32_t cl=direct_page_split->GetLocalDepth(direct_page_split->HashToBucketIndex(hash));
  for(uint32_t i=0;i<direct_page_split->Size();i++){
    if(direct_page_split->GetBucketPageId(i)==bucket_id){
      direct_page_split->IncrLocalDepth(i);
      if((i&static_cast<uint32_t>(pow(2,cl)))!=0){
        direct_page_split->SetBucketPageId(i,new_bucket_id);
      }
    }
  }
  //桶内容分配
  for(uint32_t i=0;i<bucket_page->Size();i++){
    if((Hash(bucket_page->KeyAt(i))&static_cast<uint32_t>(pow(2,cl)))!=0){
      new_bucket_page->Insert(bucket_page->KeyAt(i),bucket_page->ValueAt(i),cmp_);
      bucket_page->RemoveAt(i);
    }
  }
  direct_guard_split.Drop();
  bucket_guard.Drop();
  new_bucket_guard.Drop();
  return Insert(key,value,transaction);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t new_directory_id;
  BasicPageGuard direct_guard=bpm_->NewPageGuarded(&new_directory_id);
  auto direct_page=direct_guard.AsMut<ExtendibleHTableDirectoryPage>();
  direct_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx,new_directory_id);
  direct_guard.Drop();
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t new_bucket_id;
  BasicPageGuard bucket_guard=bpm_->NewPageGuarded(&new_bucket_id);
  auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  bucket_page->Init(bucket_max_size_);
  bucket_page->Insert(key,value,cmp_);
  directory->SetBucketPageId(bucket_idx,new_bucket_id);
  directory->SetLocalDepth(bucket_idx, directory->GetGlobalDepth());
  bucket_guard.Drop();
  return true;
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

  ReadPageGuard header_guard=bpm_->FetchPageRead(header_page_id_);
  auto header_page= header_guard.As<ExtendibleHTableHeaderPage>();
  direct_id=header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash));
  header_guard.Drop();
  if(direct_id==INVALID_PAGE_ID){
    return false;
  }

  {
  ReadPageGuard direct_guard=bpm_->FetchPageRead(direct_id);
  auto direct_page=direct_guard.As<ExtendibleHTableDirectoryPage>();
  bucket_id=direct_page->GetBucketPageId(direct_page->HashToBucketIndex(hash));
  direct_guard.Drop();
  }
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
    bucket_guard.Drop();
    RemoveCur(direct_id, bucket_id,hash);
  }
  return true;
}
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::RemoveCur(page_id_t directory_id, page_id_t bucket_id,uint32_t hash) {
    WritePageGuard direct_guard_merge=bpm_->FetchPageWrite(directory_id);
    auto direct_page_merge=direct_guard_merge.AsMut<ExtendibleHTableDirectoryPage>();
    uint32_t bucket_idx=direct_page_merge->HashToBucketIndex(hash);//空桶的索引
    if(direct_page_merge->GetGlobalDepth()==0){
      WritePageGuard header_guard=bpm_->FetchPageWrite(header_page_id_);
      auto header_page_merge=header_guard.AsMut<ExtendibleHTableHeaderPage>();
      header_page_merge->SetDirectoryPageId(header_page_merge->HashToDirectoryIndex(hash), INVALID_PAGE_ID);
      //bucket_guard.Drop();
      direct_guard_merge.Drop();
      header_guard.Drop();
      bpm_->DeletePage(directory_id);
      bpm_->DeletePage(bucket_id);
      return ;
    }
    //目录至少有俩,先找image
    uint32_t bucket_image_idx;
    if(bucket_idx<static_cast<uint32_t>(pow(2,direct_page_merge->GetGlobalDepth()-1))){
      bucket_image_idx=bucket_idx+static_cast<uint32_t>(pow(2, direct_page_merge->GetGlobalDepth()-1));
    }
    else{
      bucket_image_idx=bucket_idx-static_cast<uint32_t>(pow(2, direct_page_merge->GetGlobalDepth()-1));
    }
    if(direct_page_merge->GetLocalDepth(bucket_idx)!=direct_page_merge->GetLocalDepth(bucket_image_idx)){
      return ;
    }
    page_id_t bucket_image_id=direct_page_merge->GetBucketPageId(bucket_image_idx);
    WritePageGuard bucket_image_guard=bpm_->FetchPageWrite(bucket_image_id);
    auto bucket_image_page=bucket_image_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
    //不涉及桶内迁移，只涉及链接迁移
    for(uint32_t i=0;i<direct_page_merge->Size();i++){
      if(direct_page_merge->GetBucketPageId(i)==bucket_id){
        direct_page_merge->SetBucketPageId(i, bucket_image_id);
        direct_page_merge->DecrLocalDepth(i);
      }
      else if(direct_page_merge->GetBucketPageId(i)==bucket_image_id){
        direct_page_merge->DecrLocalDepth(i);
      }
    }
    bpm_->DeletePage(bucket_id);
    if(direct_page_merge->CanShrink()){
      direct_page_merge->DecrGlobalDepth();
    }
    if(bucket_image_page->IsEmpty()){
      RemoveCur(directory_id, bucket_image_id, hash);
    }
    direct_guard_merge.Drop();
    bucket_image_guard.Drop();
}
template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
