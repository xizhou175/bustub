//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
    std::optional<frame_id_t> frame_to_evict = std::nullopt;
    
    TimeDiff max_k_distance;
    bool first = true;
    std::optional<TimePoint> earliest_timestamp = std::nullopt;

    for (auto& [frame_id, node] : node_store_) {
        if (node.is_evictable() == false) {
            continue;
        }

        auto k_distance = node.get_k_distance();
        if (k_distance == std::nullopt) {
            auto t = node.get_earliest_timestamp();
            if (t != std::nullopt) {
                if (earliest_timestamp == std::nullopt || earliest_timestamp->time_since_epoch() > t->time_since_epoch()) {
                    frame_to_evict = frame_id;
                    earliest_timestamp = t;
                }
            }
            else {
                frame_to_evict = frame_id;
                break;
            }
        }
        else {
            if (earliest_timestamp != std::nullopt) { 
                continue;
            }
            if (first == true || max_k_distance < k_distance) {
                frame_to_evict = frame_id;
                max_k_distance = k_distance.value();
            }
            first = false;
        }
    }
    if (frame_to_evict.has_value()) {
        node_store_.erase(frame_to_evict.value());
        curr_size_--;
    }
    return frame_to_evict;
 }

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
    BUSTUB_ASSERT((size_t)frame_id <= replacer_size_, "frame_id > replacer_size!");
    if(node_store_.count(frame_id) == 0) {
        node_store_.emplace(frame_id, LRUKNode(frame_id, k_, false));
    }
    node_store_[frame_id].update_history();
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    if (node_store_.count(frame_id) == 0) {
        return;
    }
    auto& node = node_store_[frame_id];
    if (set_evictable == node.is_evictable()) {
        return;
    }
    else if(node.is_evictable() == false) {
        curr_size_++;
    }
    else {
        if (curr_size_ > 0){
            curr_size_--;
        }
    }
    node.toggle_evictable();
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
    if (node_store_.count(frame_id) == 0) {
        return;
    }
    auto node = node_store_[frame_id];
    if (node.is_evictable() == false) {
        throw std::runtime_error("Cannot remove a non-evictable frame!");
    }
    else {
        node_store_.erase(frame_id);
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
