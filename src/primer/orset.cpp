#include "primer/orset.h"
#include <algorithm>
#include <string>
#include <vector>
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

template <typename T>
auto ORSet<T>::Contains(const T &elem) const -> bool {
  // TODO(student): Implement this
  for (auto iter = e_.begin(); iter != e_.end(); iter++) {
    if ((*iter).first == elem) {
      return true;
    }
  }
  return false;
}

template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  // TODO(student): Implement this
  if (t_.count(std::make_pair(elem, uid)) == 0) {
    e_.insert(std::make_pair(elem, uid));
  }
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  // TODO(student): Implement this
  std::set<std::pair<T, bustub::uid_t>> r;
  std::set<std::pair<T, bustub::uid_t>> e_sub;
  std::set<std::pair<T, bustub::uid_t>> t_sub;
  for (auto iter = e_.begin(); iter != e_.end(); iter++) {
    if ((*iter).first == elem) {
      r.insert(*iter);
    }
  }
  set_difference(e_.begin(), e_.end(), r.begin(), r.end(), inserter(e_sub, e_sub.begin()));
  e_ = e_sub;
  set_union(t_.begin(), t_.end(), r.begin(), r.end(), inserter(t_sub, t_sub.begin()));
  t_ = t_sub;
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  // TODO(student): Implement this
  std::set<std::pair<T, bustub::uid_t>> tmp_1;
  std::set<std::pair<T, bustub::uid_t>> tmp_2;
  std::set<std::pair<T, bustub::uid_t>> tmp_3;
  std::set<std::pair<T, bustub::uid_t>> tmp_4;
  set_difference(e_.begin(), e_.end(), other.t_.begin(), other.t_.end(), inserter(tmp_1, tmp_1.begin()));
  set_difference(other.e_.begin(), other.e_.end(), t_.begin(), t_.end(), inserter(tmp_2, tmp_2.begin()));
  set_union(tmp_1.begin(), tmp_1.end(), tmp_2.begin(), tmp_2.end(), inserter(tmp_3, tmp_3.begin()));
  e_ = tmp_3;
  set_union(t_.begin(), t_.end(), other.t_.begin(), other.t_.end(), inserter(tmp_4, tmp_4.begin()));
  t_ = tmp_4;
}

template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  // TODO(student): Implement this
  std::vector<T> element_tmp;
  for (auto iter = e_.begin(); iter != e_.end(); iter++) {
    element_tmp.push_back((*iter).first);
  }
  return element_tmp;
}

template <typename T>
auto ORSet<T>::ToString() const -> std::string {
  auto elements = Elements();
  std::sort(elements.begin(), elements.end());
  return fmt::format("{{{}}}", fmt::join(elements, ", "));
}

template class ORSet<int>;
template class ORSet<std::string>;

}  // namespace bustub
