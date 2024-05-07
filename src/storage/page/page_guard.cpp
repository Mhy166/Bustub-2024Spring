#include "storage/page/page_guard.h"
#include <cstddef>
#include <utility>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/page.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_),deleted_(that.deleted_) {
  that.deleted_=true;
}

void BasicPageGuard::Drop() {
  if (deleted_) {
    return ;
  }
  if(page_!=nullptr&&page_->GetPageId()!=INVALID_PAGE_ID){
    bpm_->UnpinPage(PageId(), is_dirty_);
  }
  deleted_=true;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    deleted_=that.deleted_;
    that.deleted_=true;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  //if(page_!=nullptr){page_->RLatch();}
  deleted_=true;
  return {bpm_,page_};
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  deleted_=true;
  //if(page_!=nullptr){page_->WLatch();}
  return {bpm_,page_};
}


ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept=default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    guard_.bpm_ = that.guard_.bpm_;
    guard_.page_ = that.guard_.page_;
    guard_.is_dirty_ = that.guard_.is_dirty_;
    guard_.deleted_=that.guard_.deleted_;
    that.guard_.deleted_=true;
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if(guard_.deleted_){
    return;
  }
  if(guard_.page_!=nullptr){
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept=default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    guard_.bpm_ = that.guard_.bpm_;
    guard_.page_ = that.guard_.page_;
    guard_.is_dirty_ = that.guard_.is_dirty_;
    guard_.deleted_=that.guard_.deleted_;
    that.guard_.deleted_=true;
  }
  return *this;
}

void WritePageGuard::Drop() {
  if(guard_.deleted_){
    return;
  }
  if(guard_.page_!=nullptr){
    guard_.page_->WUnlatch();
  } 
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub