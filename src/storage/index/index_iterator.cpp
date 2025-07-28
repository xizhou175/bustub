//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, BufferPoolManager* bpm, int index)
  : cur_page_id_(page_id)
  , bpm_(bpm)
  , cur_idx_(index)
{}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;


INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return cur_page_id_ == INVALID_PAGE_ID;
  //ReadPageGuard guard = bpm_->ReadPage(cur_page_id_);
  //auto* cur_page = guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  //return cur_idx_ == cur_page->GetSize() && cur_page->GetNextPageId() == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  ReadPageGuard guard = bpm_->ReadPage(cur_page_id_);
  auto* cur_page = guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  return std::pair(cur_page->KeyAt(cur_idx_), cur_page->ValueAt(cur_idx_));
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (cur_page_id_ == INVALID_PAGE_ID) return *this;
  ReadPageGuard guard = bpm_->ReadPage(cur_page_id_);
  auto* cur_page = guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  if (cur_idx_ == cur_page->GetSize()-1) {
    cur_idx_ = 0;
    cur_page_id_ = cur_page->GetNextPageId();
  }
  else {
    cur_idx_++;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return (this->cur_page_id_ == itr.GetPageId() && this->cur_page_id_ == INVALID_PAGE_ID) || (this->cur_page_id_ == itr.GetPageId() && this->cur_idx_ == itr.GetCurIdx());
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  return this->cur_page_id_ != itr.GetPageId() || this->cur_idx_ != itr.GetCurIdx();
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
