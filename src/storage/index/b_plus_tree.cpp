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
  ReadPageGuard leaf_page_guard = bpm_->ReadPage(leaf_page_id);
  const B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = leaf_page_guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  int index = leaf_page->KeyIndex(key, comparator_);
  if (comparator_(leaf_page->KeyAt(index), key) == 0) {
    result->push_back(leaf_page->ValueAt(index));
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType& key) -> page_id_t {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  const BPlusTreePage* page = guard.As<BPlusTreePage>();
  while(page->IsLeafPage() == false){
    const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *internal_page = guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    int index = internal_page->KeyIndex(key, comparator_);
    
    auto page_id = internal_page->ValueAt(index-1);
    //std::cout << "key: " << key << "index: " << index << "page id: " << page_id << std::endl;
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
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = nullptr;
  WritePageGuard guard;
  if (IsEmpty()) {
    guard = bpm_->WritePage(header_page_id_);
    auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
    root_page->root_page_id_ = header_page_id_;
    
    leaf_page = guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    leaf_page->Init(header_page_id_, INVALID_PAGE_ID, leaf_max_size_, 0, INVALID_PAGE_ID);
    
    //ctx.header_page_ = std::move(guard);
    //ctx.root_page_id_ = header_page_id_;
  }
  else {
    auto leaf_page_id = FindLeafPage(key);
    guard = bpm_->WritePage(leaf_page_id);
    leaf_page = guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  }
  leaf_page->Insert(key, value, comparator_);
  //std::cout << leaf_page->GetParentPageId() << std::endl;
  //static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page)->PrintKey();
  
  if (leaf_page->GetSize() > leaf_max_size_) {
    // need to split
    //fmt::print("before split\n");
    //leaf_page->PrintKey();
    //new_leaf_page->PrintKey();
    auto new_page_id = Split(leaf_page);
    auto old_page_id = leaf_page->GetPageId();
    guard.Drop();
    
    WritePageGuard new_page_guard = bpm_->WritePage(new_page_id);
    auto* new_leaf_page = new_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    auto risen_key = new_leaf_page->KeyAt(0);
    new_page_guard.Drop();
    //fmt::print("after split\n");
    //static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page)->PrintKey();
    //static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(new_leaf_page)->PrintKey();
    //InsertToParent(leaf_page, new_leaf_page, risen_key);
    InsertToParent(old_page_id, new_page_id, risen_key);
    
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertToParent(page_id_t old_page_id, page_id_t new_page_id, const KeyType& key) -> void {
  using MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  WritePageGuard parent_guard;
  WritePageGuard old_page_guard = bpm_->WritePage(old_page_id);
  BPlusTreePage* old_node = old_page_guard.AsMut<BPlusTreePage>();
  WritePageGuard new_page_guard = bpm_->WritePage(new_page_id);
  BPlusTreePage* new_node = new_page_guard.AsMut<BPlusTreePage>();
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
    return;
  }

  parent_guard = bpm_->WritePage(old_node->GetParentPageId());
  new_node->SetParentPageId(old_node->GetParentPageId());
  auto parent_page = parent_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
  
  old_page_guard.Drop();
  new_page_guard.Drop();

  parent_page->Insert(key, new_page_id, comparator_);
  //std::cout << old_node->GetParentPageId() << std::endl;
  //static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(old_node)->PrintKey();
  //static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(new_node)->PrintKey();
  //parent_page->PrintKey();

  if (parent_page->GetSize() <= internal_max_size_) {
    return;
  }

  auto parent_sibling_page_id = Split(parent_page);
  auto parent_page_id = parent_page->GetPageId();
  parent_guard.Drop();
  
  ReadPageGuard parent_sibling_guard = bpm_->ReadPage(parent_sibling_page_id);
  auto parent_sibling_page = parent_sibling_guard.As<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
  auto risen_key = parent_sibling_page->KeyAt(1);
  parent_sibling_guard.Drop();

  InsertToParent(parent_page_id, parent_sibling_page_id, risen_key);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(BPlusTreePage* page) -> page_id_t {
  using MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  page_id_t new_page_id = bpm_->NewPage();
  if (page->IsLeafPage()) {
    WritePageGuard new_page_guard = bpm_->WritePage(new_page_id);
    B_PLUS_TREE_LEAF_PAGE_TYPE* cur_leaf_page = static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
    B_PLUS_TREE_LEAF_PAGE_TYPE* new_leaf_page = new_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    new_leaf_page->Init(new_page_id, cur_leaf_page->GetParentPageId(), leaf_max_size_, 0, cur_leaf_page->GetNextPageId());
    //new_leaf_page->SetNextPageId(cur_leaf_page->GetNextPageId());
    cur_leaf_page->SetNextPageId(new_page_id);
    cur_leaf_page->MoveHalfTo(new_leaf_page);
  }
  else {
    WritePageGuard new_page_guard = bpm_->WritePage(new_page_id);
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE* cur_internal_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page);
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE* new_internal_page = new_page_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    new_internal_page->Init(new_page_id, cur_internal_page->GetParentPageId(), internal_max_size_);
    cur_internal_page->MoveHalfTo(new_internal_page);
    for (int i = 0; i < new_internal_page->GetSize(); i++) {
      auto page_id = new_internal_page->ValueAt(i);
      if (page_id == 0 || page_id == INVALID_PAGE_ID) {
        continue;
      }
      //std::cout << page_id << std::endl;
      WritePageGuard guard = bpm_->WritePage(page_id);
      BPlusTreePage* child_page = guard.AsMut<BPlusTreePage>();
      child_page->SetParentPageId(new_page_id);
    }
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
  UNIMPLEMENTED("TODO(P2): Add implementation.");
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
  const BPlusTreePage* page = guard.As<BPlusTreePage>();
  while(page->IsLeafPage() == false){
    const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *internal_page = guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
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
  ReadPageGuard guard = bpm_->ReadPage(page_id);
  auto* leaf_page = guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
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
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, bpm_);
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  return header_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
