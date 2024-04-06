//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <future>
#include <mutex>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  bool flag = true;
  Page *page_tmp;
  frame_id_t new_frame;
  //检查缓冲池中有没有unpin的页面
  if(replacer_->Size()==0){
    flag=false;
  }
  //先从freelist寻找frame
  if (!free_list_.empty()) {
    new_frame = free_list_.front();
    free_list_.pop_front();
    for (size_t i = 0; i < pool_size_; i++) {
      page_tmp = &pages_[i];
      if (page_tmp->page_id_ == INVALID_PAGE_ID) {
        break;
      }
    }
  } else {
    //找不到而且也都pin了
    if (!flag) {
      page_id = nullptr;
      return nullptr;
    }
    //存在没pin的
    replacer_->Evict(&new_frame);
    page_id_t evi_page;
    for (auto &iter : page_table_) {
      if (iter.second == new_frame) {
        evi_page = iter.first;
        break;
      }
    }  //要被驱逐的页面id
    for (size_t i = 0; i < pool_size_; i++) {
      page_tmp = &pages_[i];
      if (page_tmp->page_id_ == evi_page) {
        break;
      }
    }
    // page_tmp指向要被替换的页面
    //擦除旧页面
    page_table_.erase(page_table_.find(page_tmp->page_id_));
    if (page_tmp->is_dirty_) {
      auto promise1 = disk_scheduler_->CreatePromise();
      auto future1 = promise1.get_future();
      disk_scheduler_->Schedule({true,page_tmp->data_,page_tmp->page_id_,std::move(promise1)});
      future1.get();
      //disk_scheduler_->GetdiskManager()->WritePage(page_tmp->page_id_, page_tmp->data_);
      page_tmp->is_dirty_ = false;
    }
    page_tmp->ResetMemory();
  }
  page_id_t new_page_id = AllocatePage();
  page_tmp->page_id_ = new_page_id;
  page_tmp->pin_count_ =1;
  replacer_->RecordAccess(new_frame);
  replacer_->SetEvictable(new_frame, false);
  page_table_.insert({new_page_id, new_frame});
  *page_id = new_page_id;
  return page_tmp;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  Page *page_tmp;
  //检查缓冲池中有没有页面
  for (size_t i = 0; i < pool_size_; i++) {
    page_tmp = &pages_[i];
    if (page_tmp->page_id_ == page_id) {
      auto iter = page_table_.find(page_id);
      replacer_->RecordAccess(iter->second);
      page_tmp->pin_count_++;
      return page_tmp;
    }
  }
  //池中没有该页面
  frame_id_t new_frame;
  bool flag = true;
  //检查缓冲池中有没有unpin的页面
  if(replacer_->Size()==0){
    flag=false;
  }
  //先找frame
  if (!free_list_.empty()) {
    new_frame = free_list_.front();
    free_list_.pop_front();
    for (size_t i = 0; i < pool_size_; i++) {
      page_tmp = &pages_[i];
      if (page_tmp->page_id_ == INVALID_PAGE_ID) {
        break;
      }
    }
  } else {
    if (!flag) {
      return nullptr;
    }
    //存在没pin的
    replacer_->Evict(&new_frame);
    page_id_t evi_page;
    for (auto &iter : page_table_) {
      if (iter.second == new_frame) {
        evi_page = iter.first;
        break;
      }
    }
    for (size_t i = 0; i < pool_size_; i++) {
      page_tmp = &pages_[i];
      if (page_tmp->page_id_ == evi_page) {
        break;
      }
    }
    page_table_.erase(page_table_.find(page_tmp->page_id_));
    if (page_tmp->is_dirty_) {
      auto promise1 = disk_scheduler_->CreatePromise();
      auto future1 = promise1.get_future();
      disk_scheduler_->Schedule({true,page_tmp->data_,page_tmp->page_id_,std::move(promise1)});
      future1.get();
      page_tmp->is_dirty_ = false;
    }
    page_tmp->ResetMemory();
  }
  auto promise2 = disk_scheduler_->CreatePromise();
  auto future2 = promise2.get_future();
  disk_scheduler_->Schedule({false,page_tmp->data_,page_id,std::move(promise2)});
  future2.get();
  page_tmp->page_id_ = page_id;
  page_tmp->pin_count_ =1;
  replacer_->RecordAccess(new_frame);
  replacer_->SetEvictable(new_frame, false);
  page_table_.insert({page_id, new_frame});
  return page_tmp;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    Page *page_tmp = &pages_[i];
    if (page_tmp->page_id_ == page_id) {
      if (page_tmp->pin_count_ <= 0) {
        return false;
      }
      page_tmp->pin_count_--;
      if (page_tmp->pin_count_ == 0) {
        auto iter = page_table_.find(page_tmp->page_id_);
        frame_id_t &sef = iter->second;
        replacer_->SetEvictable(sef, true);
      }
      if(is_dirty){
        page_tmp->is_dirty_ = true;
      }
      return true;
    }
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    Page *page_tmp = &pages_[i];
    if (page_tmp->page_id_ == page_id) {
      auto promise1 = disk_scheduler_->CreatePromise();
      auto future1 = promise1.get_future();
      disk_scheduler_->Schedule({true,page_tmp->data_,page_tmp->page_id_,std::move(promise1)});
      future1.get();
      page_tmp->is_dirty_ = false;
      return true;
    }
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    Page *page_tmp = &pages_[i];
    if (page_tmp->page_id_ != INVALID_PAGE_ID) {
      auto promise1 = disk_scheduler_->CreatePromise();
      auto future1 = promise1.get_future();
      disk_scheduler_->Schedule({true,page_tmp->data_,page_tmp->page_id_,std::move(promise1)});
      future1.get();
      page_tmp->is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  for (size_t i = 0; i < pool_size_; i++) {
    Page *page_tmp = &pages_[i];
    if (page_tmp->page_id_ == page_id) {
      if (page_tmp->pin_count_ > 0) {
        return false;
      }
    if (page_tmp->is_dirty_) {
      auto promise1 = disk_scheduler_->CreatePromise();
      auto future1 = promise1.get_future();
      disk_scheduler_->Schedule({true,page_tmp->data_,page_tmp->page_id_,std::move(promise1)});
      future1.get();
      page_tmp->is_dirty_ = false;
    }
      replacer_->Remove(iter->second);
      free_list_.push_front(iter->second);
      page_table_.erase(iter);
      page_tmp->ResetMemory();
      page_tmp->page_id_ = INVALID_PAGE_ID;
      page_tmp->pin_count_ = 0;
      DeallocatePage(page_id);
      break;
    }
  }
  return true;
}
//-----------------------------------------------------------------------------
auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  return {this,FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page_fetch = FetchPage(page_id);
  if(page_fetch==nullptr){
    return {this,nullptr};
  }
  //page_fetch->RLatch();
  return {this,page_fetch};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page_fetch = FetchPage(page_id);
  if(page_fetch==nullptr){
    return {this,nullptr};
  }
  //page_fetch->WLatch();
  return {this,page_fetch};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  return  {this,NewPage(page_id)};;
}

}  // namespace bustub
