#include "storage/page/page_guard.h"
#include <cstddef>
#include <utility>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/page.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    if(page_->GetPageId()!=INVALID_PAGE_ID){
      bpm_->UnpinPage(PageId(), is_dirty_);
    }
    page_ = nullptr;
    is_dirty_=false;
    bpm_=nullptr;
  }
  is_dirty_=false;
  bpm_=nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    Drop();
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  ReadPageGuard read_new_guard(bpm_, page_);
  Drop();
  //if(page_!=nullptr){page_->RLatch();}
  return read_new_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard write_new_guard(bpm_, page_);
  Drop();
  //if(page_!=nullptr){page_->WLatch();}
  return write_new_guard;
}


ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept{
  guard_=std::move(that.guard_);
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    if(guard_.page_!=nullptr){//释放锁
      guard_.page_->RUnlatch();
    }
    guard_=std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_=std::move(that.guard_);
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    if(guard_.page_!=nullptr){//释放锁
      guard_.page_->WUnlatch();
    }
    guard_=std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if(guard_.page_!=nullptr){
    guard_.page_->WUnlatch();
  }
   guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
