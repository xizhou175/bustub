//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include <algorithm>
#include "common/config.h"
#include "fmt/base.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  page_id_t leaf_page_id = FindLeafPage(key);
  if (leaf_page_id == INVALID_PAGE_ID) return false;
  ReadPageGuard leaf_page_guard = bpm_->ReadPage(leaf_page_id);
  const B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = leaf_page_guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  int index = leaf_page->KeyIndex(key, comparator_);
  if (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
    //leaf_page->PrintKey();
    //std::cout << index << std::endl;
    result->push_back(leaf_page->ValueAt(index));
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> page_id_t {
  if (header_page_id_ == INVALID_PAGE_ID) return INVALID_PAGE_ID;
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  const BPlusTreePage *page = guard.As<BPlusTreePage>();
  while (page->IsLeafPage() == false) {
    const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *internal_page =
        guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    int index = internal_page->KeyIndex(key, comparator_);

    auto page_id = internal_page->ValueAt(index - 1);
    //std::cout << "index: " << index << "page id: " << page_id << std::endl;
    guard = bpm_->ReadPage(page_id);
    page = guard.As<BPlusTreePage>();
  }

  return page->GetPageId();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = nullptr;
  WritePageGuard guard;
  if (IsEmpty()) {
    guard = bpm_->WritePage(header_page_id_);
    auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
    root_page->root_page_id_ = header_page_id_;

    leaf_page = guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    leaf_page->Init(header_page_id_, INVALID_PAGE_ID, leaf_max_size_, 0, INVALID_PAGE_ID);

    // ctx.header_page_ = std::move(guard);
    // ctx.root_page_id_ = header_page_id_;
  } else {
    auto leaf_page_id = FindLeafPage(key);
    if (leaf_page_id == INVALID_PAGE_ID) return false;
    guard = bpm_->WritePage(leaf_page_id);
    leaf_page = guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  }
  leaf_page->Insert(key, value, comparator_);
  // std::cout << leaf_page->GetParentPageId() << std::endl;
  // static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page)->PrintKey();

  if (leaf_page->GetSize() > leaf_max_size_) {
    // need to split
    // fmt::print("before split\n");
    // leaf_page->PrintKey();
    // new_leaf_page->PrintKey();
    auto new_page_id = Split(leaf_page);
    // auto old_page_id = leaf_page->GetPageId();
    // guard.Drop();

    WritePageGuard new_page_guard = bpm_->WritePage(new_page_id);
    auto *new_leaf_page = new_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    auto risen_key = new_leaf_page->KeyAt(0);
    // new_page_guard.Drop();
    // fmt::print("after split\n");
    // static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page)->PrintKey();
    // static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(new_leaf_page)->PrintKey();

    InsertToParent(guard, new_page_guard, risen_key);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertToParent(WritePageGuard &old_page_guard, WritePageGuard &new_page_guard, const KeyType &key)
    -> void {
  // using MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  WritePageGuard parent_guard;
  // WritePageGuard old_page_guard = bpm_->WritePage(old_page_id);
  BPlusTreePage *old_node = old_page_guard.AsMut<BPlusTreePage>();
  // WritePageGuard new_page_guard = bpm_->WritePage(new_page_id);
  BPlusTreePage *new_node = new_page_guard.AsMut<BPlusTreePage>();
  auto new_page_id = new_node->GetPageId();

  if (old_node->IsRootPage()) {
    auto new_root_page_id = bpm_->NewPage();
    parent_guard = bpm_->WritePage(new_root_page_id);
    auto root_page = parent_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    root_page->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_, 2);

    header_page_id_ = new_root_page_id;

    root_page->SetKeyAt(1, key);
    root_page->SetValueAt(0, old_node->GetPageId());
    root_page->SetValueAt(1, new_node->GetPageId());
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    old_page_guard.Drop();
    new_page_guard.Drop();
    return;
  }

  parent_guard = bpm_->WritePage(old_node->GetParentPageId());
  new_node->SetParentPageId(old_node->GetParentPageId());
  auto parent_page = parent_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();

  old_page_guard.Drop();
  new_page_guard.Drop();

  // std::cout << old_node->GetParentPageId() << std::endl;
  // static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(old_node)->PrintKey();
  // static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(new_node)->PrintKey();
  // parent_page->PrintKey();

  if (parent_page->GetSize() < internal_max_size_) {
    parent_page->Insert(key, new_page_id, comparator_);
    return;
  }

  parent_page->Insert(key, new_page_id, comparator_);
  auto parent_sibling_page_id = Split(parent_page);
  // auto parent_page_id = parent_page->GetPageId();
  // parent_guard.Drop();

  WritePageGuard parent_sibling_guard = bpm_->WritePage(parent_sibling_page_id);
  auto parent_sibling_page = parent_sibling_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
  auto risen_key = parent_sibling_page->KeyAt(1);
  parent_sibling_page->RemoveFirstKey();
  // parent_sibling_guard.Drop();

  InsertToParent(parent_guard, parent_sibling_guard, risen_key);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(BPlusTreePage *page) -> page_id_t {
  // using MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  page_id_t new_page_id = bpm_->NewPage();
  if (page->IsLeafPage()) {
    WritePageGuard new_page_guard = bpm_->WritePage(new_page_id);
    B_PLUS_TREE_LEAF_PAGE_TYPE *cur_leaf_page = static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);
    B_PLUS_TREE_LEAF_PAGE_TYPE *new_leaf_page = new_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    new_leaf_page->Init(new_page_id, cur_leaf_page->GetParentPageId(), leaf_max_size_, 0,
                        cur_leaf_page->GetNextPageId());
    // new_leaf_page->SetNextPageId(cur_leaf_page->GetNextPageId());
    cur_leaf_page->SetNextPageId(new_page_id);
    cur_leaf_page->MoveHalfTo(new_leaf_page);
  } else {
    WritePageGuard new_page_guard = bpm_->WritePage(new_page_id);
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *cur_internal_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page);
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *new_internal_page = new_page_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    new_internal_page->Init(new_page_id, cur_internal_page->GetParentPageId(), internal_max_size_);
    cur_internal_page->MoveHalfTo(new_internal_page, bpm_);
  }
  return new_page_id;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  if (IsEmpty()) {
    return;
  }
  //std::cout << "Remove" << std::endl;
  page_id_t leaf_page_id = FindLeafPage(key);
  if (leaf_page_id == INVALID_PAGE_ID) return;
  //std::cout << "leaf page " << leaf_page_id << std::endl;
  WritePageGuard guard = bpm_->WritePage(leaf_page_id);
  auto *leaf_page = guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  leaf_page->Remove(key, comparator_);
  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    return;
  }
  JoinOrRedistribute(guard);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::JoinOrRedistribute(WritePageGuard &page_guard) {
  BPlusTreePage *page = page_guard.AsMut<BPlusTreePage>();
  if (page->IsRootPage()) {
    if (!page->IsLeafPage() && page->GetSize() == 1) {
      auto* root_page = page_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
      auto child_page_id = root_page->ValueAt(0);
      WritePageGuard child_page_guard = bpm_->WritePage(child_page_id);
      auto* child_page = child_page_guard.AsMut<BPlusTreePage>();
      header_page_id_ = child_page->GetPageId();
      child_page->SetParentPageId(INVALID_PAGE_ID);
    }
    else if (page->GetSize() == 0 && page->IsLeafPage()) {
      header_page_id_ = INVALID_PAGE_ID;
    }
    return;
  }

  if (page->GetSize() >= page->GetMinSize()) {
    return;
  }
  page_id_t parent_page_id = page->GetParentPageId();
  WritePageGuard parent_guard = bpm_->WritePage(parent_page_id);
  auto *parent_page = parent_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
  int index = parent_page->ValueIndex(page->GetPageId());
  
  // check whether this parent has only one child, tihs situation should not be allowed, but in case.
  if (CheckIfOnlyChild(index, parent_page)) {
    fmt::println("only child for page {}", parent_page_id);
    return;
  }

  if (index >= 0 && index != parent_page->GetSize() - 1) {
    page_id_t sibling_page_id = parent_page->ValueAt(index + 1);
    //fmt::println("index: {} page_id: {} sibling_page: {}", index, page->GetPageId(), sibling_page_id);
    WritePageGuard sibling_page_guard = bpm_->WritePage(sibling_page_id);
    auto *sibling_page = sibling_page_guard.AsMut<BPlusTreePage>();
    if (sibling_page->GetSize() > sibling_page->GetMinSize()) {
      // redistribute
      Redistribute(page_guard, sibling_page_guard, parent_guard, index, false);
      return;
    }

    // We have to coalesce
    Coalesce(page_guard, sibling_page_guard, parent_guard, index + 1);
    return;
  }
  if (index == parent_page->GetSize() - 1) {
    page_id_t sibling_page_id = parent_page->ValueAt(index - 1);
    WritePageGuard sibling_page_guard = bpm_->WritePage(sibling_page_id);
    auto *sibling_page = sibling_page_guard.AsMut<BPlusTreePage>();
    if (sibling_page->GetSize() > sibling_page->GetMinSize()) {
      // redistribute
      Redistribute(page_guard, sibling_page_guard, parent_guard, index, true);
      return;
    }
    Coalesce(sibling_page_guard, page_guard, parent_guard, index);
    return;
  }
  
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::CheckIfOnlyChild(int index, BPlusTreePage* parent_page) {
  if (index == 0 && index == parent_page->GetSize() - 1) return true;
  /*if (index == 1) {
    page_id_t page_id = reinterpret_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent_page)->ValueAt(0);
    if (page_id == 0) {
      ReadPageGuard guard = bpm_->ReadPage(0);
      const auto* page = guard.As<BPlusTreePage>();
      if (page->GetParentPageId() != parent_page->GetPageId()) return true;
    }
  }*/
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(WritePageGuard& guard, WritePageGuard& sibling_guard, WritePageGuard& parent_guard, int index) {
  MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE* parent_page = parent_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
  const KeyType pull_down_key = parent_page->KeyAt(index);
  BPlusTreePage *page = guard.AsMut<BPlusTreePage>();
  if (page->IsLeafPage()) {
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_sibling_page = sibling_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    //leaf_sibling_page->PrintKey();
    leaf_sibling_page->MoveAllTo(leaf_page);
    //leaf_page->PrintKey();
  }
  else {
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_page = guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_sibling_page = sibling_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    //internal_sibling_page->PrintKey();
    internal_sibling_page->MoveAllTo(internal_page, pull_down_key, bpm_);
    
    //internal_page->PrintKey();
  }
  guard.Drop();
  sibling_guard.Drop();
  parent_page->Remove(index);
  //parent_page->PrintKey();
  JoinOrRedistribute(parent_guard);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(WritePageGuard& guard, WritePageGuard& sibling_guard, WritePageGuard& parent_guard, int index, bool from_prev) {
  BPlusTreePage *page = guard.AsMut<BPlusTreePage>();
  if (page->IsLeafPage()) {
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_sibling_page = sibling_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE* parent_page = parent_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    if (!from_prev){
      leaf_sibling_page->MoveOneTo(0, leaf_page, leaf_page->GetSize());
      parent_page->SetKeyAt(index+1, leaf_sibling_page->KeyAt(0));
    }
    else {
      leaf_sibling_page->MoveOneTo(leaf_sibling_page->GetSize() - 1, leaf_page, 0);
      parent_page->SetKeyAt(index, leaf_page->KeyAt(0));
    }
  }
  else {
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_page = guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_sibling_page = sibling_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE* parent_page = parent_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    if (!from_prev){
      auto pull_down_key = parent_page->KeyAt(index + 1);
      parent_page->SetKeyAt(index+1, internal_sibling_page->KeyAt(1));
      internal_sibling_page->MoveFirstToEnd(internal_page, pull_down_key, bpm_);
    }
    else {
      auto pull_down_key = parent_page->KeyAt(index);
      internal_sibling_page->MoveLastToBegin(internal_page, pull_down_key, bpm_);
      parent_page->SetKeyAt(index, internal_page->KeyAt(1));
    }
    
  }
  guard.Drop();
  sibling_guard.Drop();
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  const BPlusTreePage *page = guard.As<BPlusTreePage>();
  while (page->IsLeafPage() == false) {
    const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *internal_page =
        guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto page_id = internal_page->ValueAt(0);
    guard = bpm_->ReadPage(page_id);
    page = guard.As<BPlusTreePage>();
  }

  return INDEXITERATOR_TYPE(page->GetPageId(), bpm_);
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  page_id_t page_id = FindLeafPage(key);
  if (page_id == INVALID_PAGE_ID) return INDEXITERATOR_TYPE(INVALID_PAGE_ID, bpm_);
  ReadPageGuard guard = bpm_->ReadPage(page_id);
  auto *leaf_page = guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  int index = leaf_page->KeyIndex(key, comparator_);
  if (index == leaf_page->GetSize() || comparator_(leaf_page->KeyAt(index), key) != 0) {
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, bpm_);
  }
  return INDEXITERATOR_TYPE(page_id, bpm_, index);
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(INVALID_PAGE_ID, bpm_); }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return header_page_id_; }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
