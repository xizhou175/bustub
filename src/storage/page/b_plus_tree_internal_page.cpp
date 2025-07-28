//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "storage/page/page_guard.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new internal page.
 *
 * Writes the necessary header information to a newly created page,
 * including set page type, set current size, set page id, set parent id and set max page size,
 * must be called after the creation of a new page to make a valid BPlusTreeInternalPage.
 *
 * @param max_size Maximal size of the page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size, int size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(size);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
}

/**
 * @brief Helper method to get/set the key associated with input "index"(a.k.a
 * array offset).
 *
 * @param index The index of the key to get. Index must be non-zero.
 * @return Key at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  if (index >= INTERNAL_PAGE_SLOT_CNT || index < 0) {
    throw std::runtime_error("out of bounds");
  }
  return key_array_[index];
}

/**
 * @brief Set key at the specified index.
 *
 * @param index The index of the key to set. Index must be non-zero.
 * @param key The new value for key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index >= INTERNAL_PAGE_SLOT_CNT || index < 0) {
    throw std::runtime_error("out of bounds");
  }
  key_array_[index] = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  if (index >= INTERNAL_PAGE_SLOT_CNT || index < 0) {
    throw std::runtime_error("out of bounds");
  }
  page_id_array_[index] = value;
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index >= INTERNAL_PAGE_SLOT_CNT || index < 0) {
    throw std::runtime_error("out of bounds");
  }
  return page_id_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType& key, const KeyComparator& key_comparator) const -> int {
  auto target = std::lower_bound(key_array_ + 1, key_array_ + GetSize(), key, [&key_comparator](const KeyType& a, const KeyType& b){
    return key_comparator(a, b) <= 0;
  });
  int dist = std::distance(key_array_, target);
  return dist;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType& key, const ValueType& value, const KeyComparator& key_comparator) {
  int index = KeyIndex(key, key_comparator);
  if (index == GetSize()) {
    key_array_[GetSize()] = key;
    page_id_array_[GetSize()] = value;
    ChangeSizeBy(1);
    return;
  }

  if (key_comparator(key_array_[index], key) == 0) {
    return;
  }
  std::move_backward(key_array_ + index, key_array_ + GetSize(), key_array_ + GetSize() + 1);
  key_array_[index] = key;

  std::move_backward(page_id_array_ + index, page_id_array_ + GetSize(), page_id_array_ + GetSize() + 1);
  page_id_array_[index] = value;
  ChangeSizeBy(1);
  return;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage* recipient) {
  int start = GetMinSize();
  if (start == 1) {
    start++;
  }
  std::copy(key_array_ + start, key_array_ + GetSize(), recipient->key_array_ + recipient->GetSize());
  std::copy(page_id_array_ + start, page_id_array_ + GetSize(), recipient->page_id_array_ + recipient->GetSize());
  recipient->ChangeSizeBy(GetSize() - start);
  SetSize(start);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PrintKey() const -> void {
  fmt::print(">PrintKey\n");
  for (int i = 1; i < GetSize(); i++){
    std::cout << KeyAt(i);
  }
  std::cout << std::endl;
  fmt::print("<PrintKey\n");
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
