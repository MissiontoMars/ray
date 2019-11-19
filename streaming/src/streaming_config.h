#ifndef RAY_STREAMING_CONFIG_H
#define RAY_STREAMING_CONFIG_H

#include <cstdint>
#include <string>

#include "format/streaming_generated.h"

namespace ray {
namespace streaming {

class StreamingConfig {
 public:
  static uint64_t TIME_WAIT_UINT;
  static uint32_t DEFAULT_STREAMING_RING_BUFFER_CAPACITY;
  static uint32_t DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL;
  static const uint32_t STRAMING_MESSGAE_BUNDLE_MAX_SIZE;
  static uint32_t DEFAULT_STREAMING_EVENT_DRIVEN_FLOWCONTROL_INTERVAL;

  /* Reference PR : https://github.com/apache/arrow/pull/2522
   * py-module import c++-python extension with static std::string will
   * crash becase of double free, double-linked or corruption (randomly).
   * So replace std::string by enum (uint32).
   */
 private:
  uint32_t streaming_ring_buffer_capacity = DEFAULT_STREAMING_RING_BUFFER_CAPACITY;

  uint32_t streaming_empty_message_time_interval =
      DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL;

  streaming::fbs::StreamingRole streaming_role = streaming::fbs::StreamingRole::Operator;

  std::string streaming_job_name = "DEFAULT_JOB_NAME";

  std::string streaming_op_name = "DEFAULT_OP_NAME";

  std::string streaming_worker_name = "DEFAULT_WORKER_NAME";

  std::string streaming_task_job_id = "ffffffff";

  std::string queue_type = "streaming_queue";

 public:
  const std::string &GetStreamingTaskJobId() const;

  void SetStreamingTaskJobId(const std::string &streaming_task_job_id);

  const std::string &GetStreaming_worker_name() const;

  void SetStreamingWorkerName(const std::string &streaming_worker_name);

  const std::string &GetStreamingOpName() const;

  void SetStreamingOpName(const std::string &streaming_op_name);

  uint32_t GetStreamingEmptyMessageTimeInterval() const;

  void SetStreamingEmptyMessageTimeInterval(
      uint32_t streaming_empty_message_time_interval);

  uint32_t GetStreamingRingBufferCapacity() const;

  void SetStreamingRingBufferCapacity(uint32_t streaming_ring_buffer_capacity);

  void ReloadProperty(const streaming::fbs::StreamingConfigKey &key, uint32_t value);

  void ReloadProperty(const streaming::fbs::StreamingConfigKey &key,
                      const std::string &value);

  streaming::fbs::StreamingRole GetStreamingRole() const;

  void SetStreamingRole(streaming::fbs::StreamingRole streaming_role);

  const std::string &GetStreamingJobName() const;

  void SetStreamingJobName(const std::string &streaming_job_name);

  const std::string &GetQueueType() const;

  void SetQueueType(const std::string &queue_type);
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_CONFIG_H
