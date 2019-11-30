#ifndef _STREAMING_QUEUE_MESSAGE_H_
#define _STREAMING_QUEUE_MESSAGE_H_

#include "ray/common/id.h"
#include "ray/common/buffer.h"
#include "protobuf/streaming_queue.pb.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

/// Base class of all message classes.
/// All payloads transferred through direct actor call are packed into a unified package, 
/// consisting of protobuf-formatted metadata and data, including data and control messages.
/// These message classes wrap the package defined in protobuf/streaming_queue.proto respectively.
class Message {
 public:
  /// Construct a Message instance.
  /// \param[in] actor_id ActorID of message sender
  /// \param[in] peer_actor_id ActorID of message receiver
  /// \param[in] queue_id queue id to identify which queue the message sent to
  /// \param[in] buffer an optional param, a chunk of data to send.
  Message(const ActorID &actor_id, const ActorID &peer_actor_id, const ObjectID &queue_id,
          std::shared_ptr<LocalMemoryBuffer> buffer = nullptr)
      : actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        queue_id_(queue_id),
        buffer_(buffer) {}
  Message() {}
  virtual ~Message() {}
  ActorID ActorId() { return actor_id_; }
  ActorID PeerActorId() { return peer_actor_id_; }
  ObjectID QueueId() { return queue_id_; }
  std::shared_ptr<LocalMemoryBuffer> Buffer() { return buffer_; }

  /// Pack all meta data and data to a LocalMemoryBuffer, which can be send through direct actor call
  std::unique_ptr<LocalMemoryBuffer> ToBytes();

  virtual queue::protobuf::StreamingQueueMessageType Type() = 0;

  /// All subclass should implement `ToProtobuf` to serialize its own protobuf data
  virtual void ToProtobuf(std::string *output) = 0;
 protected:
  ActorID actor_id_;
  ActorID peer_actor_id_;
  ObjectID queue_id_;
  std::shared_ptr<LocalMemoryBuffer> buffer_;

 public:
  /// A magic number to identify a valid message.
  static const uint32_t MagicNum;
};

/// Wrap StreamingQueueDataMsg
class DataMessage : public Message {
 public:
  DataMessage(const ActorID &actor_id, const ActorID &peer_actor_id, ObjectID queue_id,
              uint64_t seq_id, std::shared_ptr<LocalMemoryBuffer> buffer, bool raw)
      : Message(actor_id, peer_actor_id, queue_id, buffer), seq_id_(seq_id), raw_(raw) {}
  virtual ~DataMessage() {}

  static std::shared_ptr<DataMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  uint64_t SeqId() { return seq_id_; }
  bool IsRaw() { return raw_; }
  queue::protobuf::StreamingQueueMessageType Type() { return type_; }

 private:
  uint64_t seq_id_;
  bool raw_;

  const queue::protobuf::StreamingQueueMessageType type_ = 
      queue::protobuf::StreamingQueueMessageType::StreamingQueueDataMsgType;
};

/// Wrap StreamingQueueNotificationMsg
class NotificationMessage : public Message {
 public:
  NotificationMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                      const ObjectID &queue_id, uint64_t seq_id)
      : Message(actor_id, peer_actor_id, queue_id), seq_id_(seq_id) {}

  virtual ~NotificationMessage() {}

  static std::shared_ptr<NotificationMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);

  uint64_t SeqId() { return seq_id_; }
  queue::protobuf::StreamingQueueMessageType Type() { return type_; }

 private:
  uint64_t seq_id_;
  const queue::protobuf::StreamingQueueMessageType type_ = 
      queue::protobuf::StreamingQueueMessageType::StreamingQueueNotificationMsgType;
};

/// Wrap StreamingQueueCheckMsg
class CheckMessage : public Message {
 public:
  CheckMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
               const ObjectID &queue_id)
      : Message(actor_id, peer_actor_id, queue_id) {}
  virtual ~CheckMessage() {}

  static std::shared_ptr<CheckMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);

  queue::protobuf::StreamingQueueMessageType Type() { return type_; }

 private:
  const queue::protobuf::StreamingQueueMessageType type_ = 
      queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckMsgType;
};

/// Wrap StreamingQueueCheckRspMsg
class CheckRspMessage : public Message {
 public:
  CheckRspMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                  const ObjectID &queue_id, queue::protobuf::StreamingQueueError err_code)
      : Message(actor_id, peer_actor_id, queue_id), err_code_(err_code) {}
  virtual ~CheckRspMessage() {}

  static std::shared_ptr<CheckRspMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  queue::protobuf::StreamingQueueMessageType Type() { return type_; }
  queue::protobuf::StreamingQueueError Error() { return err_code_; }

 private:
  queue::protobuf::StreamingQueueError err_code_;
  const queue::protobuf::StreamingQueueMessageType type_ = 
      queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckRspMsgType;
};

/// Wrap StreamingQueueTestInitMsg
class TestInitMessage : public Message {
 public:
  TestInitMessage(const queue::protobuf::StreamingQueueTestRole role,
              const ActorID &actor_id, const ActorID &peer_actor_id,
              const std::string actor_handle_serialized, 
              const std::vector<ObjectID> &queue_ids, const std::vector<ObjectID> &rescale_queue_ids,
              std::string test_suite_name, std::string test_name,
              uint64_t param)
      : Message(actor_id, peer_actor_id, queue_ids[0]),
  actor_handle_serialized_(actor_handle_serialized), queue_ids_(queue_ids), rescale_queue_ids_(rescale_queue_ids),
  role_(role), test_suite_name_(test_suite_name), test_name_(test_name), param_(param) {}
  virtual ~TestInitMessage() {}

  static std::shared_ptr<TestInitMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  queue::protobuf::StreamingQueueMessageType Type() { return type_; }
  std::string ActorHandleSerialized() { return actor_handle_serialized_; }
  queue::protobuf::StreamingQueueTestRole Role() { return role_; }
  std::vector<ObjectID> QueueIds() { return queue_ids_; }
  std::vector<ObjectID> RescaleQueueIds() { return rescale_queue_ids_; }
  std::string TestSuiteName() { return test_suite_name_; }
  std::string TestName() { return test_name_;}
  uint64_t Param() { return param_; }

  std::string ToString() {
    std::ostringstream os;
    os << "actor_handle_serialized: " << actor_handle_serialized_;
    os << " actor_id: " << ActorId();
    os << " peer_actor_id: " << PeerActorId();
    os << " queue_ids:[";
    for (auto &qid : queue_ids_) {
      os << qid << ",";
    }
    os << "], rescale_queue_ids:[";
    for (auto &qid : rescale_queue_ids_) {
      os << qid << ",";
    }
    os << "],";
    os << " role:" << queue::protobuf::StreamingQueueTestRole_Name(role_);
    os << " suite_name: " << test_suite_name_;
    os << " test_name: " << test_name_;
    os << " param: " << param_;
    return os.str();
  }
 private:
  const queue::protobuf::StreamingQueueMessageType type_ = 
      queue::protobuf::StreamingQueueMessageType::StreamingQueueTestInitMsgType;
  std::string actor_handle_serialized_;
  std::vector<ObjectID> queue_ids_;
  std::vector<ObjectID> rescale_queue_ids_;
  queue::protobuf::StreamingQueueTestRole role_;
  std::string test_suite_name_;
  std::string test_name_;
  uint64_t param_;
};

/// Wrap StreamingQueueTestCheckStatusRspMsg
class TestCheckStatusRspMsg : public Message {
 public:
  TestCheckStatusRspMsg(const std::string test_name, bool status)
      : test_name_(test_name), status_(status) {}
  virtual ~TestCheckStatusRspMsg() {}

  static std::shared_ptr<TestCheckStatusRspMsg> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  queue::protobuf::StreamingQueueMessageType Type() { return type_; }
  std::string TestName() { return test_name_; }
  bool Status() { return status_; }

 private:
  const queue::protobuf::StreamingQueueMessageType type_ = 
      queue::protobuf::StreamingQueueMessageType::StreamingQueueTestCheckStatusRspMsgType;
  std::string test_name_;
  bool status_;
};

}
}
#endif
