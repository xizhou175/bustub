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
#include <cassert>
#include <cstddef>
#include <mutex>
#include "common/config.h"
#include "fmt/base.h"
#include "fmt/chrono.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {
  std::mutex print_m;

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

  if (leaf_max_size_ == LEAF_PAGE_SLOT_CNT) leaf_max_size_-=2;
  if (internal_max_size_ == INTERNAL_PAGE_SLOT_CNT) internal_max_size_-=2;

}

INDEX_TEMPLATE_ARGUMENTS
template<typename T>
auto BPLUSTREE_TYPE::Drop(T& queue) -> void {
  while(!queue.empty()) {
    auto guard = std::move(queue.front());
    if (guard.template As<BPlusTreePage>()->IsRootPage()) {
      //fmt::println("Dropped root page id latch");
      root_page_id_latch_.unlock();
    }
    queue.pop_front();
    guard.Drop();
  }
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
  root_page_id_latch_.lock_shared();
  auto leaf_page = FindLeafPageForRead(key, ctx);
  
  if (leaf_page == nullptr) {
    root_page_id_latch_.unlock_shared();
    return false;
  }

  int index = leaf_page->KeyIndex(key, comparator_);
  if (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
    //leaf_page->PrintKey();
    //std::cout << index << std::endl;
    result->push_back(leaf_page->ValueAt(index));
    Drop(ctx.read_set_);
    return true;
  }
  Drop(ctx.read_set_);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageForRead(const KeyType &key, Context& ctx) -> const B_PLUS_TREE_LEAF_PAGE_TYPE* {
  if (header_page_id_ == INVALID_PAGE_ID) return nullptr;
  
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  const BPlusTreePage *page = guard.As<BPlusTreePage>();

  if (page->IsLeafPage()) {
    ctx.read_set_.push_back(std::move(guard));
    return static_cast<const B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
  }

  //ctx.write_set_.push_back(std::move(guard));
  while (page->IsLeafPage() == false) {
    const MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_page = static_cast<const MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page);
    int index = internal_page->KeyIndex(key, comparator_);

    Drop(ctx.read_set_);
    ctx.read_set_.push_back(std::move(guard));

    auto child_page_id = internal_page->ValueAt(index - 1);
    guard = bpm_->ReadPage(child_page_id);
    page = guard.As<BPlusTreePage>();
    
    if (page->IsLeafPage()) {
      ctx.read_set_.push_back(std::move(guard));
    }
  }

  return static_cast<const B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageForWrite(const KeyType &key, Context& ctx, Operation op) -> B_PLUS_TREE_LEAF_PAGE_TYPE* {
  //fmt::println(">FindLeaf");
  if (header_page_id_ == INVALID_PAGE_ID) return nullptr;
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  BPlusTreePage *page = guard.AsMut<BPlusTreePage>();

  if (page->IsLeafPage()) {
    ctx.write_set_.push_back(std::move(guard));
    return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
  }

  //ctx.write_set_.push_back(std::move(guard));
  while (page->IsLeafPage() == false) {
    const MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page);
    int index = internal_page->KeyIndex(key, comparator_);

    if (op == Operation::INSERT) {
      if (internal_page->GetSize() < internal_max_size_) {
          Drop(ctx.write_set_);
      }
    }
    else {
      if (internal_page->GetSize() > internal_page->GetMinSize()) {
          Drop(ctx.write_set_);
      }
    }
    ctx.write_set_.push_back(std::move(guard));

    auto child_page_id = internal_page->ValueAt(index - 1);
    guard = bpm_->WritePage(child_page_id);
    page = guard.AsMut<BPlusTreePage>();
    
    if (page->IsLeafPage()) {
      if (op == Operation::INSERT) {
        if (page->GetSize() < leaf_max_size_) {
          Drop(ctx.write_set_);
        }
      }
      else {
        if (page->GetSize() > page->GetMinSize()) {
          Drop(ctx.write_set_);
        }
      }
      ctx.write_set_.push_back(std::move(guard));
    }
    //if (ctx.FindLatchedPage(page_id) != nullptr) {
    //  fmt::println("Deadlock! {} {} {} {} {}", key.ToString(), index, page_id, header_page_id_, internal_page->GetPageId());
    //}
    
  }
  //fmt::println("<FindLeaf");
  return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
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
  root_page_id_latch_.lock();
  if (IsEmpty()) {
    WritePageGuard guard;
    guard = bpm_->WritePage(header_page_id_);
    auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
    root_page->root_page_id_ = header_page_id_;

    leaf_page = guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    leaf_page->Init(header_page_id_, INVALID_PAGE_ID, leaf_max_size_, 0, INVALID_PAGE_ID);
    ctx.write_set_.push_back(std::move(guard));

    // ctx.header_page_ = std::move(guard);
    // ctx.root_page_id_ = header_page_id_;
  } 
  else {
    leaf_page = FindLeafPageForWrite(key, ctx, Operation::INSERT);
    if (leaf_page == nullptr) {
      //ctx.Drop(ctx.write_set_);
      Drop(ctx.write_set_);
      return false;
    }
  }
  leaf_page->Insert(key, value, comparator_);
  // std::cout << leaf_page->GetParentPageId() << std::endl;
  // static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page)->PrintKey();

  if (leaf_page->GetSize() > leaf_max_size_) {
    // need to split
    // fmt::print("before split\n");
    // leaf_page->PrintKey();
    // new_leaf_page->PrintKey();
    auto new_page = Split(leaf_page, ctx);
    // auto old_page_id = leaf_page->GetPageId();
    // guard.Drop();
    auto new_leaf_page = static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(new_page);
    auto risen_key = new_leaf_page->KeyAt(0);
    // new_page_guard.Drop();
    // fmt::print("after split\n");
    // static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page)->PrintKey();
    // static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(new_leaf_page)->PrintKey();

    InsertToParent(leaf_page, new_leaf_page, risen_key, ctx);
  }
  
  //ctx.Drop(ctx.write_set_);
  
  //std::scoped_lock lk(print_m);
  //fmt::println("Inserted {}", key.ToString());
  //auto graph = DrawBPlusTree();
  //fmt::println("{}", graph);

  Drop(ctx.write_set_);
  
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertToParent(BPlusTreePage* old_node, BPlusTreePage* new_node, const KeyType &key, Context& ctx) -> void {
  WritePageGuard parent_guard;
  
  auto new_page_id = new_node->GetPageId();
  auto old_page_id = old_node->GetPageId();

  if (old_node->IsRootPage()) {
    
    auto new_root_page_id = bpm_->NewPage();

    // needs to change before we call WritePage on new root page id because either one of these two nodes/pages might be 
    // evicted to disk when WritePage is called
    new_node->SetParentPageId(new_root_page_id);
    old_node->SetParentPageId(new_root_page_id);
    
    parent_guard = bpm_->WritePage(new_root_page_id);
    auto root_page = parent_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    
    root_page->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_, 2);
  
    header_page_id_ = new_root_page_id;

    root_page->SetKeyAt(1, key);
    root_page->SetValueAt(0, old_page_id);
    root_page->SetValueAt(1, new_page_id);

    ctx.write_set_.push_front(std::move(parent_guard));
    
    //fmt::println("root changed! old root: {} new root: {}", old_page_id, new_root_page_id);
    //root_page->PrintKey();
    
    return;
  }

  auto parent_page_id = old_node->GetParentPageId();
  //std::cout << "looking for page " << old_node->GetParentPageId() << std::endl;

  auto page = ctx.FindLatchedPage(parent_page_id);
  //std::cout << "found page " << old_node->GetParentPageId() << std::endl;
  auto parent_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page);
  //if (parent_page == nullptr) {
  //  parent_guard = bpm_->WritePage(parent_page_id);
    //new_node->SetParentPageId(old_node->GetParentPageId());
  //  parent_page = parent_guard.AsMut<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
  //}

  // std::cout << old_node->GetParentPageId() << std::endl;
  // static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(old_node)->PrintKey();
  // static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(new_node)->PrintKey();
  // parent_page->PrintKey();
 
  if (parent_page->GetSize() < internal_max_size_) {
    parent_page->Insert(key, new_page_id, comparator_);
    return;
  }

  parent_page->Insert(key, new_page_id, comparator_);
  
  auto parent_sibling_page = Split(parent_page, ctx);
  

  auto parent_sibling_internal_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent_sibling_page);
  auto risen_key = parent_sibling_internal_page->KeyAt(1);
  parent_sibling_internal_page->RemoveFirstKey();
  

  InsertToParent(parent_page, parent_sibling_internal_page, risen_key, ctx);

  //fmt::println("<InsertToParent(page id={})", old_node->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(BPlusTreePage *page, Context& ctx) -> BPlusTreePage* {
  // using MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  //fmt::println(">split(page id={})", page->GetPageId());
  page_id_t new_page_id = bpm_->NewPage();
  WritePageGuard new_page_guard = bpm_->WritePage(new_page_id);
  BPlusTreePage* new_page = new_page_guard.AsMut<BPlusTreePage>();
  if (page->IsLeafPage()) {
    B_PLUS_TREE_LEAF_PAGE_TYPE *cur_leaf_page = static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);
    B_PLUS_TREE_LEAF_PAGE_TYPE *new_leaf_page = static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(new_page);
    new_leaf_page->Init(new_page_id, cur_leaf_page->GetParentPageId(), leaf_max_size_, 0,
                        cur_leaf_page->GetNextPageId());
    // new_leaf_page->SetNextPageId(cur_leaf_page->GetNextPageId());
    cur_leaf_page->SetNextPageId(new_page_id);
    cur_leaf_page->MoveHalfTo(new_leaf_page);
  } else {
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *cur_internal_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page);
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *new_internal_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(new_page);
    new_internal_page->Init(new_page_id, cur_internal_page->GetParentPageId(), internal_max_size_);
    cur_internal_page->MoveHalfTo(new_internal_page, bpm_, ctx);
  }
  //ctx.write_set_.push_back(std::move(new_page_guard));
  //fmt::println("<split(page id={})", page->GetPageId());
  return new_page;
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
  root_page_id_latch_.lock();
  if (header_page_id_ == INVALID_PAGE_ID || IsEmpty()) {
    //ctx.Drop(ctx.write_set_);
    root_page_id_latch_.unlock();
    //Drop(ctx.write_set_);
    return;
  }
  //std::cout << "Remove" << std::endl;
  auto leaf_page = FindLeafPageForWrite(key, ctx, Operation::DELETE);
  if (leaf_page == nullptr) {
    //ctx.Drop(ctx.write_set_);
    root_page_id_latch_.unlock();
    //Drop(ctx.write_set_);
    return;
  }
  //std::cout << "leaf page " << leaf_page_id << std::endl;
  leaf_page->Remove(key, comparator_);
  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    //ctx.Drop(ctx.write_set_);
    Drop(ctx.write_set_);
    return;
  }
  JoinOrRedistribute(leaf_page, ctx);
  //ctx.Drop(ctx.write_set_);
  
  Drop(ctx.write_set_);

  /*std::scoped_lock lk(print_m);
  fmt::println("Removed {}", key.ToString());
  auto graph = DrawBPlusTree();
  fmt::println("{}", graph);*/
}
  

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::JoinOrRedistribute(BPlusTreePage* page, Context& ctx) {
  //BPlusTreePage *page = page_guard.AsMut<BPlusTreePage>();
  if (page->IsRootPage()) {
    if (!page->IsLeafPage() && page->GetSize() == 1) {
      ctx.write_set_.pop_front();
      auto* root_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page);
      auto child_page_id = root_page->ValueAt(0);

      BPlusTreePage* child_page = nullptr;
      child_page = ctx.FindLatchedPage(child_page_id);
      if (!child_page) {
        auto child_page_guard = bpm_->WritePage(child_page_id);
        child_page = child_page_guard.template AsMut<BPlusTreePage>();
        child_page->SetParentPageId(INVALID_PAGE_ID);
        ctx.write_set_.push_front(std::move(child_page_guard));
      }

      header_page_id_ = child_page->GetPageId();
      child_page->SetParentPageId(INVALID_PAGE_ID);
    }
    else if (page->GetSize() == 0 && page->IsLeafPage()) {
      //fmt::println("Empty leaf page, remove header page");
      auto header_page = ctx.FindLatchedPage(header_page_id_);
      auto root_page = reinterpret_cast<BPlusTreeHeaderPage*>(header_page);
      root_page->root_page_id_ = INVALID_PAGE_ID;
    }
    return;
  }

  if (page->GetSize() >= page->GetMinSize()) {
    return;
  }

  page_id_t parent_page_id = page->GetParentPageId();
  auto* parent = ctx.FindLatchedPage(parent_page_id);
  auto *parent_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent);
  //if (!parent_page) {
  //  std::cout << "null pointer!" << std::endl;
  //}
  int index = parent_page->ValueIndex(page->GetPageId());
  
  //parent_page->PrintKey();

  // check whether this parent has only one child, tihs situation should not be allowed, but in case.
  //if (CheckIfOnlyChild(index, parent_page)) {
  //  fmt::println("only child for page {}", parent_page_id);
  //  return;
  //}

  if (index >= 0 && index != parent_page->GetSize() - 1) {
    page_id_t sibling_page_id = parent_page->ValueAt(index + 1);
    //fmt::println("index: {} page_id: {} sibling_page: {}", index, page->GetPageId(), sibling_page_id);
    BPlusTreePage* sibling_page = nullptr;
    sibling_page = ctx.FindLatchedPage(sibling_page_id);
    if (!sibling_page) {
      WritePageGuard sibling_page_guard = bpm_->WritePage(sibling_page_id);
      sibling_page = sibling_page_guard.AsMut<BPlusTreePage>();
      ctx.write_set_.push_back(std::move(sibling_page_guard));
    }
    
    if (sibling_page->GetSize() > sibling_page->GetMinSize()) {
      // redistribute
      Redistribute(page, sibling_page, parent_page, index, false, ctx);
      return;
    }

    // We have to coalesce
    Coalesce(page, sibling_page, parent_page, index + 1, ctx);
    return;
  }
  if (index == parent_page->GetSize() - 1) {
    page_id_t sibling_page_id = parent_page->ValueAt(index - 1);

    BPlusTreePage* sibling_page = nullptr;
    sibling_page = ctx.FindLatchedPage(sibling_page_id);
    if (!sibling_page) {
      WritePageGuard sibling_page_guard = bpm_->WritePage(sibling_page_id);
      sibling_page = sibling_page_guard.AsMut<BPlusTreePage>();
      ctx.write_set_.push_back(std::move(sibling_page_guard));
    }
    if (sibling_page->GetSize() > sibling_page->GetMinSize()) {
      // redistribute
      Redistribute(page, sibling_page, parent_page, index, true, ctx);
      return;
    }
    Coalesce(sibling_page, page, parent_page, index, ctx);
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
void BPLUSTREE_TYPE::Coalesce(BPlusTreePage* page, BPlusTreePage* sibling_page, BPlusTreePage* parent, int index, Context& ctx) {
  MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE* parent_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent);
  const KeyType pull_down_key = parent_page->KeyAt(index);
  //BPlusTreePage *page = guard.AsMut<BPlusTreePage>();
  if (page->IsLeafPage()) {
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_sibling_page = static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(sibling_page);
    //leaf_sibling_page->PrintKey();
    leaf_sibling_page->MoveAllTo(leaf_page);
    //leaf_page->PrintKey();
  }
  else {
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page);
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_sibling_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(sibling_page);
    //internal_sibling_page->PrintKey();
    internal_sibling_page->MoveAllTo(internal_page, pull_down_key, bpm_, ctx);
    
    //internal_page->PrintKey();
  }
  //std::cout << "before" << std::endl;
  //parent_page->PrintKey();
  parent_page->Remove(index);
  //std::cout << "index: " << index << std::endl;
  //std::cout << "after" << std::endl;
  //parent_page->PrintKey();
  //parent_page->PrintKey();
  JoinOrRedistribute(parent_page, ctx);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(BPlusTreePage* page, BPlusTreePage* sibling_page, BPlusTreePage* parent, int index, bool from_prev, Context& ctx) {
  //BPlusTreePage *page = guard.AsMut<BPlusTreePage>();
  if (page->IsLeafPage()) {
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_sibling_page = static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(sibling_page);
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE* parent_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent);
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
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page);
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_sibling_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(sibling_page);
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE* parent_page = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent);
    if (!from_prev){
      auto pull_down_key = parent_page->KeyAt(index + 1);
      parent_page->SetKeyAt(index+1, internal_sibling_page->KeyAt(1));
      internal_sibling_page->MoveFirstToEnd(internal_page, pull_down_key, bpm_, ctx);
    }
    else {
      auto pull_down_key = parent_page->KeyAt(index);
      parent_page->SetKeyAt(index, internal_sibling_page->KeyAt(internal_sibling_page->GetSize()-1));
      internal_sibling_page->MoveLastToBegin(internal_page, pull_down_key, bpm_, ctx);
    }
    
  }
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
  root_page_id_latch_.lock_shared();
  if (IsEmpty()) {
    root_page_id_latch_.unlock_shared();
    //fmt::println("Empty tree, return invalid page id");
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, bpm_);
  }
  //if (header_page_id_ == INVALID_PAGE_ID ){
  //  root_page_id_latch_.unlock_shared();
  //  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, bpm_);
  //}
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  const BPlusTreePage *page = guard.As<BPlusTreePage>();
  while (page->IsLeafPage() == false) {
    const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *internal_page =
        guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto page_id = internal_page->ValueAt(0);

    if (internal_page->GetPageId() == header_page_id_) {
      root_page_id_latch_.unlock_shared();
    }
    guard = bpm_->ReadPage(page_id);
    page = guard.As<BPlusTreePage>();
  }

  if (page->IsRootPage()) {
    root_page_id_latch_.unlock_shared();
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
  root_page_id_latch_.lock_shared();
  Context ctx;
  auto leaf_page = FindLeafPageForRead(key, ctx);
  if (leaf_page == nullptr) {
    Drop(ctx.read_set_);
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, bpm_);
  }
  
  int index = leaf_page->KeyIndex(key, comparator_);
  if (index == leaf_page->GetSize() || comparator_(leaf_page->KeyAt(index), key) != 0) {
    Drop(ctx.read_set_);
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, bpm_);
  }
  Drop(ctx.read_set_);
  return INDEXITERATOR_TYPE(leaf_page->GetPageId(), bpm_, index);
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
