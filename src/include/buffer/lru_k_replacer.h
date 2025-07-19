//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <vector>
#include <chrono>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

using TimePoint = std::chrono::time_point<std::chrono::high_resolution_clock>;
using TimeDiff = std::chrono::duration<double>;

class LRUKNode {
 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.
    

    std::list<TimePoint> history_;
    size_t k_;
    frame_id_t fid_;
    bool is_evictable_{false};
 public:
    LRUKNode() = default;
    LRUKNode(frame_id_t fid, size_t k, bool is_evictable)
        : k_(k)
        , fid_(fid)
        , is_evictable_(is_evictable)
    {}

    bool is_evictable() {
        return is_evictable_;
    }

    bool toggle_evictable() {
      return is_evictable_ = !is_evictable_;
    }

    std::optional<TimeDiff> get_k_distance() {
        if( history_.size() < k_ ) {
            return std::nullopt;
        }
        else {
            return history_.front() - history_.back();   
        }
    }

    std::optional<TimePoint> get_earliest_timestamp() {
        if( history_.size() == 0 ) {
            return std::nullopt;
        }
        else {
            return history_.back();
        }  
    }

    void update_history() {
        const auto cur_time = std::chrono::high_resolution_clock::now();
        history_.push_front(cur_time);
        if (history_.size() > k_) {
            history_.resize(k_);
        }
    }
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  std::unordered_map<frame_id_t, LRUKNode> node_store_;
  [[maybe_unused]] size_t current_timestamp_{0};
  size_t curr_size_{0};
  [[maybe_unused]] size_t replacer_size_;
  size_t k_;
  [[maybe_unused]] std::mutex latch_;
};

}  // namespace bustub
