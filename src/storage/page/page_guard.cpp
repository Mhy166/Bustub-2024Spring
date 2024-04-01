#include "storage/page/page_guard.h"
#include <cstddef>
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
  //page_->RLatch();
  return read_new_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard write_new_guard(bpm_, page_);
  Drop();
  //page_->WLatch();
  return write_new_guard;
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page):guard_(bpm,page){
  guard_.page_->RLatch();
}  //√

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept{
  if(that.guard_.page_!=nullptr){
    that.guard_.page_->RUnlatch();
  }
  guard_.page_=that.guard_.page_;
  guard_.bpm_=that.guard_.bpm_;
  guard_.is_dirty_=that.guard_.is_dirty_;
  guard_.page_->RLatch();
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  that.guard_.is_dirty_ = false;
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    if(guard_.page_!=nullptr){
      guard_.page_->RUnlatch();
      guard_.Drop();
    }
    if(that.guard_.page_!=nullptr){
      that.guard_.page_->RUnlatch();
    }
  guard_.page_=that.guard_.page_;
  guard_.bpm_=that.guard_.bpm_;
  guard_.is_dirty_=that.guard_.is_dirty_;
  guard_.page_->RLatch();
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  that.guard_.is_dirty_ = false;
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

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page):guard_(bpm,page){
  guard_.page_->WLatch();
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  if(that.guard_.page_!=nullptr){
    that.guard_.page_->WUnlatch();
  }
  guard_.page_=that.guard_.page_;
  guard_.bpm_=that.guard_.bpm_;
  guard_.is_dirty_=that.guard_.is_dirty_;
  guard_.page_->WLatch();
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  that.guard_.is_dirty_ = false;
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    if(guard_.page_!=nullptr){
      guard_.page_->WUnlatch();
      guard_.Drop();
    }
    if(that.guard_.page_!=nullptr){
      that.guard_.page_->WUnlatch();
    }
  guard_.page_=that.guard_.page_;
  guard_.bpm_=that.guard_.bpm_;
  guard_.is_dirty_=that.guard_.is_dirty_;
  guard_.page_->WLatch();
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  that.guard_.is_dirty_ = false;
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
