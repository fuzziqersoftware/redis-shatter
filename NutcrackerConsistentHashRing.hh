#pragma once

#include <stdint.h>

#include <phosg/ConsistentHashRing.hh>
#include <string>
#include <unordered_map>
#include <vector>


// this isn't the same as phosg's ConsistentHashRing; this one is designed to
// mirror the implementation in twemproxy/nutcracker so that redis-shatter can
// be used alongside it

class NutcrackerConsistentHashRing : public ConsistentHashRing {
public:
  NutcrackerConsistentHashRing() = delete;
  NutcrackerConsistentHashRing(const std::vector<Host>& hosts);
  virtual ~NutcrackerConsistentHashRing() = default;

  virtual uint64_t host_id_for_key(const void* key, int64_t size) const;

protected:
  struct Point {
    uint32_t index;
    uint32_t value;

    Point(uint32_t index, uint32_t hash);
  };

  std::vector<Point> points;
};
