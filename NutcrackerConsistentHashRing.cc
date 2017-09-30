#include "NutcrackerConsistentHashRing.hh"

#include <inttypes.h>
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <phosg/Hash.hh>
#include <phosg/Strings.hh>
#include <string>
#include <vector>

using namespace std;



NutcrackerConsistentHashRing::Point::Point(uint32_t index, uint32_t value) : index(index),
    value(value) { }



#define KETAMA_POINTS_PER_SERVER    160
#define KETAMA_POINTS_PER_HASH      4
#define KETAMA_MAX_HOSTLEN          256

NutcrackerConsistentHashRing::NutcrackerConsistentHashRing(
    const vector<Host>& hosts) : ConsistentHashRing(hosts) {
  if (this->hosts.empty()) {
    throw invalid_argument("no hosts in continuum");
  }

  uint64_t total_weight = this->hosts.size();

  for (size_t host_index = 0; host_index < this->hosts.size(); host_index++) {
    const auto& host = this->hosts[host_index];

    float pct = 1.0 / (float)total_weight;
    size_t points_per_host = (size_t)((floorf((float) (pct * KETAMA_POINTS_PER_SERVER / 4 * (float)this->hosts.size() + 0.0000000001))) * 4);

    for (size_t point_index = 0; point_index <= (points_per_host / KETAMA_POINTS_PER_HASH) - 1; point_index++) {
      char point_data[KETAMA_MAX_HOSTLEN];
      size_t point_data_size = snprintf(point_data, KETAMA_MAX_HOSTLEN,
          "%s-%zu", host.name.c_str(), point_index);
      string hash = md5(point_data, point_data_size);

      for (size_t x = 0; x < KETAMA_POINTS_PER_HASH; x++) {
        uint32_t value = (static_cast<uint32_t>(hash[3 + x * 4] & 0xFF) << 24) |
                         (static_cast<uint32_t>(hash[2 + x * 4] & 0xFF) << 16) |
                         (static_cast<uint32_t>(hash[1 + x * 4] & 0xFF) << 8) |
                         (static_cast<uint32_t>(hash[0 + x * 4] & 0xFF));
        this->points.emplace_back(host_index, value);
      }
    }
  }

  qsort(this->points.data(), this->points.size(), sizeof(this->points[0]),
        [](const void* t1, const void* t2) -> int {
    const Point* ct1 = reinterpret_cast<const Point*>(t1);
    const Point* ct2 = reinterpret_cast<const Point*>(t2);
    if (ct1->value == ct2->value) {
      return 0;
    } else if (ct1->value > ct2->value) {
      return 1;
    } else {
      return -1;
    }
  });
}

uint64_t NutcrackerConsistentHashRing::host_id_for_key(const void* key,
    int64_t size) const {
  // TODO: use std::lower_bound here instead of manual binary search

  uint32_t hash32 = fnv1a64(key, size);

  const Point* left = this->points.data();
  const Point* right = left + this->points.size();

  while (left < right) {
    const Point* middle = left + (right - left) / 2;
    if (middle->value < hash32) {
      left = middle + 1;
    } else {
      right = middle;
    }
  }

  if (right == this->points.data() + this->points.size()) {
    return this->points[0].index;
  }
  return right->index;
}
