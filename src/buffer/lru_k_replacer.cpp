//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <cmath>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}  //总大小和k值确定了

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  bool flag_inf = false;
  bool flag = false;
  size_t earliest_cur = current_timestamp_ + 5;
  size_t earliest_cur_inf = current_timestamp_ + 5;
  for (auto &iter : node_store_) {
    LRUKNode &tmp = iter.second;
    if (!tmp.is_evictable_) {
      continue;
    }
    if (tmp.history_.size() < tmp.k_) {
      flag_inf = true;
      if (earliest_cur_inf > tmp.history_.back()) {
        earliest_cur_inf = tmp.history_.back();
        *frame_id = tmp.fid_;
        flag = true;
      }
    }
    if (tmp.history_.size() == tmp.k_ && !flag_inf) {
      if (earliest_cur > tmp.history_.back()) {
        earliest_cur = tmp.history_.back();
        *frame_id = tmp.fid_;
        flag = true;
      }
    }
  }
  auto iter1 = node_store_.find(*frame_id);
  if (flag) {
    //LRUKNode &node1=iter1->second;
    node_store_.erase(iter1);
    //node1.history_.clear();
    curr_size_--;
  }
  return flag;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw Exception("Frame_id invalid!");
  }
  current_timestamp_++;
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    LRUKNode &node1 = iter->second;
    node1.history_.push_front(current_timestamp_);
    if (node1.history_.size() > node1.k_) {
      node1.history_.pop_back();
    }
  } else {
    LRUKNode node2;
    node2.k_ = k_;
    node2.fid_ = frame_id;
    node2.history_.push_front(current_timestamp_);
    node_store_[frame_id] = node2;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    LRUKNode &node1 = iter->second;
    if (node1.is_evictable_ != set_evictable) {
      if (set_evictable) {
        node1.is_evictable_ = true;
        curr_size_++;
      } else {
        node1.is_evictable_ = false;
        curr_size_--;
      }
    }
  } else {
    throw Exception("Frame_id invalid!");
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    LRUKNode &node1 = iter->second;
    if (!node1.is_evictable_) {
      throw Exception("Remove action is called on a non-evictable frame!");
    }
    node_store_.erase(iter);
    //node1.history_.clear();
    curr_size_--;
  } else {
    return;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
