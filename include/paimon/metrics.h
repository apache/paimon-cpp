/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "paimon/type_fwd.h"

namespace paimon {

/// Statistics snapshot for a histogram metric.
///
/// Note: percentile values are estimated from internal buckets.
struct PAIMON_EXPORT HistogramStats {
    uint64_t count = 0;
    double sum = 0;
    double min = 0;
    double max = 0;
    double average = 0;
    double p50 = 0;
    double p90 = 0;
    double p95 = 0;
    double p99 = 0;
    double p999 = 0;
    double stddev = 0;
};

/// Abstract interface for collecting and managing performance metrics in Paimon operations.
class PAIMON_EXPORT Metrics {
 public:
    virtual ~Metrics() = default;

    /// Set the value of a specific counter metric.
    virtual void SetCounter(const std::string& metric_name, uint64_t metric_value) = 0;

    /// Get the current value of a specific counter metric.
    virtual Result<uint64_t> GetCounter(const std::string& metric_name) const = 0;

    /// Get all counter metrics as a map.
    virtual std::map<std::string, uint64_t> GetAllCounters() const = 0;

    /// Add a sample to a histogram metric.
    virtual void ObserveHistogram(const std::string& metric_name, double value) = 0;

    /// Get histogram statistics snapshot.
    virtual Result<HistogramStats> GetHistogramStats(const std::string& metric_name) const = 0;

    /// Get all histogram statistics snapshots.
    virtual std::map<std::string, HistogramStats> GetAllHistogramStats() const = 0;

    /// Set the value of a specific gauge metric.
    virtual void SetGauge(const std::string& metric_name, double metric_value) = 0;

    /// Get the current value of a specific gauge metric.
    virtual Result<double> GetGauge(const std::string& metric_name) const = 0;

    /// Get all gauge metrics as a map.
    virtual std::map<std::string, double> GetAllGauges() const = 0;

    /// Merge metrics from another Metrics instance into this one.
    virtual void Merge(const std::shared_ptr<Metrics>& other) = 0;

    /// Convert all metrics to a JSON string representation.
    virtual std::string ToString() const = 0;
};

}  // namespace paimon
