// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SHIM_CLIENT_DIRECTORY_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SHIM_CLIENT_DIRECTORY_H

#include "google/cloud/version.h"
#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace shim {

/// Layer 6 (Shim): per-connection wait-free directory mapping
/// `{table_name, app_profile_id, permission}` to the `Client` shards serving
/// it.
///
/// `Find()` is wait-free (no locks, no allocation, no atomic RMWs). `Insert()`
/// requires serialized writers and enforces `kDefaultMaxEntries = 256` bounded
/// capacity (`SESSION_CLIENT_SPEC.md #1`), returning `nullptr` when full so
/// callers fall back cleanly to `classic_connection_`.
template <typename Value>
class ClientDirectory {
 public:
  static constexpr std::size_t kDefaultMaxEntries = 256;

  struct Entry {
    std::string table_name;
    std::string app_profile_id;
    int permission;
    Value value;
    std::size_t hash;
  };

  explicit ClientDirectory(std::size_t max_entries = kDefaultMaxEntries)
      : max_entries_(max_entries) {
    Publish(kInitialCapacity);
  }
  ClientDirectory(ClientDirectory const&) = delete;
  ClientDirectory& operator=(ClientDirectory const&) = delete;

  /// Wait-free lookup. Returns nullptr when absent.
  Entry const* Find(std::string_view table_name, int permission,
                    std::string_view app_profile_id = "default") const {
    if (app_profile_id.empty()) app_profile_id = "default";
    auto const h = Hash(table_name, app_profile_id, permission);
    Table const* t = current_.load(std::memory_order_acquire);
    for (std::size_t i = h & t->mask;; i = (i + 1) & t->mask) {
      Entry const* e = t->slots[i].load(std::memory_order_acquire);
      if (e == nullptr) return nullptr;
      if (e->hash == h && e->permission == permission &&
          e->table_name == table_name && e->app_profile_id == app_profile_id) {
        return e;
      }
    }
  }

  /// Inserts a new entry. Callers must serialize writers and must not insert
  /// a key that is already present. Returns `nullptr` if `size() >=
  /// max_entries()`.
  Entry const* Insert(std::string table_name, int permission, Value value,
                      std::string app_profile_id = "default") {
    if (entries_.size() >= max_entries_) {
      return nullptr;
    }
    if (app_profile_id.empty()) app_profile_id = "default";
    auto entry = std::make_unique<Entry>(
        Entry{std::move(table_name), std::move(app_profile_id), permission,
              std::move(value), std::size_t{0}});
    entry->hash =
        Hash(entry->table_name, entry->app_profile_id, entry->permission);
    if ((entries_.size() + 1) * 2 > live_->slots.size()) {
      Publish(live_->slots.size() * 2);
    }
    Entry const* raw = entry.get();
    entries_.push_back(std::move(entry));
    Place(*live_, raw);
    return raw;
  }

  /// Visits every entry. Safe to call concurrently with `Find()`; callers
  /// must serialize it with `Insert()`.
  template <typename F>
  void ForEach(F&& f) const {
    for (auto const& e : entries_) f(*e);
  }

  std::size_t size() const { return entries_.size(); }
  std::size_t max_entries() const { return max_entries_; }

  /// Number of hash slots in the live table (for tests).
  std::size_t capacity() const { return current_.load()->slots.size(); }

  /// Drops every entry. Only safe when no `Find()` can run concurrently.
  void Clear() {
    tables_.clear();
    entries_.clear();
    Publish(kInitialCapacity);
  }

 private:
  static constexpr std::size_t kInitialCapacity = 16;

  struct Table {
    explicit Table(std::size_t capacity) : slots(capacity), mask(capacity - 1) {
      for (auto& s : slots) s.store(nullptr, std::memory_order_relaxed);
    }
    std::vector<std::atomic<Entry const*>> slots;
    std::size_t mask;
  };

  static std::size_t Hash(std::string_view table_name,
                          std::string_view app_profile_id, int permission) {
    auto h = std::hash<std::string_view>{}(table_name);
    h ^= std::hash<std::string_view>{}(app_profile_id) + 0x9e3779b97f4a7c15ULL +
         (h << 6) + (h >> 2);
    h ^= static_cast<std::size_t>(permission) + 0x9e3779b97f4a7c15ULL +
         (h << 6) + (h >> 2);
    return h;
  }

  static void Place(Table& t, Entry const* e) {
    for (std::size_t i = e->hash & t.mask;; i = (i + 1) & t.mask) {
      if (t.slots[i].load(std::memory_order_relaxed) == nullptr) {
        t.slots[i].store(e, std::memory_order_release);
        return;
      }
    }
  }

  void Publish(std::size_t capacity) {
    auto t = std::make_unique<Table>(capacity);
    for (auto const& e : entries_) Place(*t, e.get());
    live_ = t.get();
    current_.store(live_, std::memory_order_release);
    tables_.push_back(std::move(t));
  }

  std::size_t max_entries_;
  std::atomic<Table const*> current_{nullptr};
  Table* live_ = nullptr;
  std::vector<std::unique_ptr<Table>> tables_;
  std::vector<std::unique_ptr<Entry>> entries_;
};

}  // namespace shim
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SHIM_CLIENT_DIRECTORY_H
