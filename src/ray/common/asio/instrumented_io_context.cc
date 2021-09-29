// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/common/asio/instrumented_io_context.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <utility>

#include "ray/stats/metric.h"

DEFINE_stats(operation_count, "operation count", ("Method"), (), ray::stats::GAUGE);
DEFINE_stats(operation_run_time_ms, "operation execution time", ("Method"), (),
             ray::stats::GAUGE);
DEFINE_stats(operation_queue_time_ms, "operation queuing time", ("Method"), (),
             ray::stats::GAUGE);
DEFINE_stats(operation_active_count, "activate operation number", ("Method"), (),
             ray::stats::GAUGE);
namespace {

/// A helper for creating a snapshot view of the global stats.
/// This acquires a reader lock on the provided global stats, and creates a
/// lockless copy of the stats.
GlobalStats to_global_stats_view(std::shared_ptr<GuardedGlobalStats> stats) {
  absl::MutexLock lock(&(stats->mutex));
  return GlobalStats(stats->stats);
}

/// A helper for creating a snapshot view of the stats for a handler.
/// This acquires a lock on the provided guarded handler stats, and creates a
/// lockless copy of the stats.
HandlerStats to_handler_stats_view(std::shared_ptr<GuardedHandlerStats> stats) {
  absl::MutexLock lock(&(stats->mutex));
  return HandlerStats(stats->stats);
}

/// A helper for converting a duration into a human readable string, such as "5.346 ms".
std::string to_human_readable(double duration) {
  static const std::array<std::string, 4> to_unit{{"ns", "us", "ms", "s"}};
  size_t idx = std::min(to_unit.size() - 1,
                        static_cast<size_t>(std::log(duration) / std::log(1000)));
  double new_duration = duration / std::pow(1000, idx);
  std::stringstream result;
  result << std::fixed << std::setprecision(3) << new_duration << " " << to_unit[idx];
  return result.str();
}

/// A helper for converting a duration into a human readable string, such as "5.346 ms".
std::string to_human_readable(int64_t duration) {
  return to_human_readable(static_cast<double>(duration));
}

}  // namespace

void instrumented_io_context::post(std::function<void()> handler, const std::string name,
                                   std::function<void()> callback) {
  if (!RayConfig::instance().event_stats()) {
    return boost::asio::io_context::post(std::move(handler));
  }
  const auto stats_handle = RecordStart(name);
  // References are only invalidated upon deletion of the corresponding item from the
  // table, which we won't do until this io_context is deleted. Provided that
  // GuardedHandlerStats synchronizes internal access, we can concurrently write to the
  // handler stats it->second from multiple threads without acquiring a table-level
  // readers lock in the callback.
  boost::asio::io_context::post([handler = std::move(handler),
                                 stats_handle = std::move(stats_handle),
                                 callback = std::move(callback)]() {
    RecordExecution(handler, std::move(stats_handle), callback);
  });
}

void instrumented_io_context::post(std::function<void()> handler,
                                   std::shared_ptr<StatsHandle> stats_handle,
                                   std::function<void()> callback) {
  if (!RayConfig::instance().event_stats()) {
    return boost::asio::io_context::post(std::move(handler));
  }
  // Reset the handle start time, so that we effectively measure the queueing
  // time only and not the time delay from RecordStart().
  // TODO(ekl) it would be nice to track this delay too,.
  stats_handle->ZeroAccumulatedQueuingDelay();
  boost::asio::io_context::post([handler = std::move(handler),
                                 stats_handle = std::move(stats_handle),
                                 callback = std::move(callback)]() {
    RecordExecution(handler, std::move(stats_handle), callback);
  });
}

void instrumented_io_context::dispatch(std::function<void()> handler,
                                       const std::string name,
                                       std::function<void()> callback) {
  if (!RayConfig::instance().event_stats()) {
    return boost::asio::io_context::post(std::move(handler));
  }
  const auto stats_handle = RecordStart(name);
  // References are only invalidated upon deletion of the corresponding item from the
  // table, which we won't do until this io_context is deleted. Provided that
  // GuardedHandlerStats synchronizes internal access, we can concurrently write to the
  // handler stats it->second from multiple threads without acquiring a table-level
  // readers lock in the callback.
  boost::asio::io_context::dispatch([handler = std::move(handler),
                                     stats_handle = std::move(stats_handle),
                                     callback = std::move(callback)]() {
    RecordExecution(handler, std::move(stats_handle), callback);
  });
}

std::shared_ptr<StatsHandle> instrumented_io_context::RecordStart(
    const std::string &name, int64_t expected_queueing_delay_ns) {
  auto stats = GetOrCreate(name);
  int64_t curr_count = 0;
  {
    absl::MutexLock lock(&(stats->mutex));
    stats->stats.cum_count++;
    curr_count = ++stats->stats.curr_count;
  }
  STATS_operation_count.Record(curr_count, name);
  STATS_operation_active_count.Record(curr_count, name);
  return std::make_shared<StatsHandle>(
      name, absl::GetCurrentTimeNanos() + expected_queueing_delay_ns, stats,
      global_stats_);
}

void instrumented_io_context::RecordExecution(const std::function<void()> &fn,
                                              std::shared_ptr<StatsHandle> handle,
                                              const std::function<void()> &callback) {
  int64_t start_execution = absl::GetCurrentTimeNanos();
  // Update running count
  {
    auto &stats = handle->handler_stats;
    absl::MutexLock lock(&(stats->mutex));
    stats->stats.running_count++;
  }
  // Execute actual handler.
  fn();
  int64_t end_execution = absl::GetCurrentTimeNanos();
  // Update execution time stats.
  const auto execution_time_ns = end_execution - start_execution;
  // Update handler-specific stats.
  STATS_operation_run_time_ms.Record(execution_time_ns / 1000000, handle->handler_name);
  {
    auto &stats = handle->handler_stats;
    absl::MutexLock lock(&(stats->mutex));
    // Handler-specific execution stats.
    stats->stats.cum_execution_time += execution_time_ns;
    // Handler-specific current count.
    stats->stats.curr_count--;
    STATS_operation_active_count.Record(stats->stats.curr_count, handle->handler_name);
    // Handler-specific running count.
    stats->stats.running_count--;
  }
  // Update global stats.
  const auto queue_time_ns = start_execution - handle->start_time;
  STATS_operation_queue_time_ms.Record(queue_time_ns / 1000000, handle->handler_name);
  {
    auto global_stats = handle->global_stats;
    absl::MutexLock lock(&(global_stats->mutex));
    // Global queue stats.
    global_stats->stats.cum_queue_time += queue_time_ns;
    if (global_stats->stats.min_queue_time > queue_time_ns) {
      global_stats->stats.min_queue_time = queue_time_ns;
    }
    if (global_stats->stats.max_queue_time < queue_time_ns) {
      global_stats->stats.max_queue_time = queue_time_ns;
    }
  }
  if (callback) {
    callback();
  }
  handle->execution_recorded = true;
}

std::shared_ptr<GuardedHandlerStats> instrumented_io_context::GetOrCreate(
    const std::string &name) {
  // Get this handler's stats.
  std::shared_ptr<GuardedHandlerStats> result;
  mutex_.ReaderLock();
  auto it = post_handler_stats_.find(name);
  if (it == post_handler_stats_.end()) {
    mutex_.ReaderUnlock();
    // Lock the table until we have added the entry. We use try_emplace and handle a
    // failed insertion in case the item was added before we acquire the writers lock;
    // this allows the common path, in which the handler already exists in the hash table,
    // to only require the readers lock.
    absl::WriterMutexLock lock(&mutex_);
    const auto pair =
        post_handler_stats_.try_emplace(name, std::make_shared<GuardedHandlerStats>());
    if (pair.second) {
      it = pair.first;
    } else {
      it = post_handler_stats_.find(name);
      // If try_emplace failed to insert the item, the item is guaranteed to exist in
      // the table.
      RAY_CHECK(it != post_handler_stats_.end());
    }
    result = it->second;
  } else {
    result = it->second;
    mutex_.ReaderUnlock();
  }
  return result;
}

GlobalStats instrumented_io_context::get_global_stats() const {
  return to_global_stats_view(global_stats_);
}

absl::optional<HandlerStats> instrumented_io_context::get_handler_stats(
    const std::string &handler_name) const {
  absl::ReaderMutexLock lock(&mutex_);
  auto it = post_handler_stats_.find(handler_name);
  if (it == post_handler_stats_.end()) {
    return {};
  }
  return to_handler_stats_view(it->second);
}

std::vector<std::pair<std::string, HandlerStats>>
instrumented_io_context::get_handler_stats() const {
  // We lock the stats table while copying the table into a vector.
  absl::ReaderMutexLock lock(&mutex_);
  std::vector<std::pair<std::string, HandlerStats>> stats;
  stats.reserve(post_handler_stats_.size());
  std::transform(
      post_handler_stats_.begin(), post_handler_stats_.end(), std::back_inserter(stats),
      [](const std::pair<std::string, std::shared_ptr<GuardedHandlerStats>> &p) {
        return std::make_pair(p.first, to_handler_stats_view(p.second));
      });
  return stats;
}

std::string instrumented_io_context::StatsString() const {
  if (!RayConfig::instance().event_stats()) {
    return "Stats collection disabled, turn on "
           "event_stats "
           "flag to enable event loop stats collection";
  }
  auto stats = get_handler_stats();
  // Sort stats by cumulative count, outside of the table lock.
  sort(stats.begin(), stats.end(),
       [](const std::pair<std::string, HandlerStats> &a,
          const std::pair<std::string, HandlerStats> &b) {
         return a.second.cum_count > b.second.cum_count;
       });
  int64_t cum_count = 0;
  int64_t curr_count = 0;
  int64_t cum_execution_time = 0;
  std::stringstream handler_stats_stream;
  for (const auto &entry : stats) {
    cum_count += entry.second.cum_count;
    curr_count += entry.second.curr_count;
    cum_execution_time += entry.second.cum_execution_time;
    handler_stats_stream << "\n\t" << entry.first << " - " << entry.second.cum_count
                         << " total (" << entry.second.curr_count << " active";
    if (entry.second.running_count > 0) {
      handler_stats_stream << ", " << entry.second.running_count << " running";
    }
    handler_stats_stream << "), CPU time: mean = "
                         << to_human_readable(entry.second.cum_execution_time /
                                              static_cast<double>(entry.second.cum_count))
                         << ", total = "
                         << to_human_readable(entry.second.cum_execution_time);
  }
  const auto global_stats = get_global_stats();
  std::stringstream stats_stream;
  stats_stream << "\nGlobal stats: " << cum_count << " total (" << curr_count
               << " active)";
  stats_stream << "\nQueueing time: mean = "
               << to_human_readable(global_stats.cum_queue_time /
                                    static_cast<double>(cum_count))
               << ", max = " << to_human_readable(global_stats.max_queue_time)
               << ", min = " << to_human_readable(global_stats.min_queue_time)
               << ", total = " << to_human_readable(global_stats.cum_queue_time);
  stats_stream << "\nExecution time:  mean = "
               << to_human_readable(cum_execution_time / static_cast<double>(cum_count))
               << ", total = " << to_human_readable(cum_execution_time);
  stats_stream << "\nHandler stats:";
  stats_stream << handler_stats_stream.rdbuf();
  return stats_stream.str();
}
