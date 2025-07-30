//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstring>
#include <iterator>
#include <sstream>
#include <stdexcept>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "fmt/base.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size, int size, page_id_t next_page_id) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(size);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetNextPageId(next_page_id);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType& key, const KeyComparator& key_comparator) const -> int {
  auto target = std::lower_bound(key_array_, key_array_ + GetSize(), key, [&key_comparator](const KeyType& a, const KeyType& b){
    return key_comparator(a, b) < 0;
  });
  int dist = std::distance(key_array_, target);
  return dist;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType& key, const ValueType& value, const KeyComparator& key_comparator) {
  int index = KeyIndex(key, key_comparator);
  if (index == GetSize()) {
    key_array_[GetSize()] = key;
    rid_array_[GetSize()] = value;
    ChangeSizeBy(1);
    return;
  }

  if (key_comparator(key_array_[index], key) == 0) {
    return;
  }
  std::move_backward(key_array_ + index, key_array_ + GetSize(), key_array_ + GetSize() + 1);
  key_array_[index] = key;

  std::move_backward(rid_array_ + index, rid_array_ + GetSize(), rid_array_ + GetSize() + 1);
  rid_array_[index] = value;
  ChangeSizeBy(1);

  return;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType& key, const KeyComparator& key_comparator) {

  int index = KeyIndex(key, key_comparator);
  if (key_comparator(KeyAt(index), key) != 0) return;

  std::move(key_array_ + index + 1, key_array_ + GetSize(), key_array_ + index);

  std::move(rid_array_ + index + 1, rid_array_ + GetSize(), rid_array_ + index);
 
  ChangeSizeBy(-1);
  return;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(B_PLUS_TREE_LEAF_PAGE_TYPE* recipient) {
  int start = GetMinSize();
  //fmt::print("start: {}\n", start);
  std::copy(key_array_ + start, key_array_ + GetSize(), recipient->key_array_ + recipient->GetSize());
  std::copy(rid_array_ + start, rid_array_ + GetSize(), recipient->rid_array_ + recipient->GetSize());
  recipient->ChangeSizeBy(GetSize() - start);
  SetSize(start);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(B_PLUS_TREE_LEAF_PAGE_TYPE* recipient) {
  std::copy(key_array_, key_array_ + GetSize(), recipient->key_array_ + recipient->GetSize());
  std::copy(rid_array_, rid_array_ + GetSize(), recipient->rid_array_ + recipient->GetSize());
  recipient->SetNextPageId(GetNextPageId());
  recipient->ChangeSizeBy(GetSize());
  SetSize(0);
  return;
}

INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::MoveOneTo(int index, B_PLUS_TREE_LEAF_PAGE_TYPE* recipient, int recipient_index) {
  KeyType ret = KeyAt(index);
  if (recipient_index != recipient->GetSize()){
    std::move_backward(recipient->key_array_ + recipient_index, recipient->key_array_ + GetSize(), recipient->key_array_ + GetSize() + 1);
    std::move_backward(recipient->rid_array_ + recipient_index, recipient->rid_array_ + GetSize(), recipient->rid_array_ + GetSize() + 1);
  }
  recipient->SetKeyAt(recipient_index, KeyAt(index));
  recipient->SetValueAt(recipient_index, ValueAt(index));
  std::move(key_array_ + index + 1, key_array_ + GetSize(), key_array_ + index);
  std::move(rid_array_ + index + 1, rid_array_ + GetSize(), rid_array_ + index);
  
  recipient->ChangeSizeBy(1);
  ChangeSizeBy(-1);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType& key) {
  if ((size_t)index >= LEAF_PAGE_SLOT_CNT) {
    throw std::runtime_error("out of bounds");
  }
  key_array_[index] = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType& value) {
  if ((size_t)index >= LEAF_PAGE_SLOT_CNT) {
    throw std::runtime_error("out of bounds");
  }
  rid_array_[index] = value;
}

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  if ((size_t)index >= LEAF_PAGE_SLOT_CNT) {
    throw std::runtime_error("out of bounds");
  }
  return key_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if ((size_t)index >= LEAF_PAGE_SLOT_CNT) {
    throw std::runtime_error("out of bounds");
  }
  return rid_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PrintKey() const -> void {
  fmt::print(">PrintKey\n");
  for (int i = 0; i < GetSize(); i++){
    std::cout << KeyAt(i);
  }
  std::cout << std::endl;
  fmt::print("<PrintKey\n");
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
