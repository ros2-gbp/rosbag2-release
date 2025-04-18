// Copyright 2018 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ROSBAG2_STORAGE__SERIALIZED_BAG_MESSAGE_HPP_
#define ROSBAG2_STORAGE__SERIALIZED_BAG_MESSAGE_HPP_

#include <memory>
#include <string>

#include "rcutils/types/uint8_array.h"
#include "rcutils/time.h"

namespace rosbag2_storage
{

/// \brief A serialized message that is stored in a bag file.
struct SerializedBagMessage
{
  /// \brief Serialized data of the message.
  std::shared_ptr<rcutils_uint8_array_t> serialized_data;

  /// \brief Nanosecond timestamp when this message was received.
  rcutils_time_point_value_t recv_timestamp;

  /// \brief Nanosecond timestamp when this message was published. If not available,
  /// this will be set to recv_timestamp.
  rcutils_time_point_value_t send_timestamp;

  /// \brief Name of the topic this message was published on.
  std::string topic_name;

  /// \brief An optional sequence number of the message. If non-zero, sequence numbers should be
  /// unique per channel (per topic and per publisher) and increasing over time.
  uint32_t sequence_number = 0;
};

/// \brief A shared pointer to a SerializedBagMessage.
typedef std::shared_ptr<SerializedBagMessage> SerializedBagMessageSharedPtr;

/// \brief A const shared pointer to a SerializedBagMessage.
typedef std::shared_ptr<const SerializedBagMessage> SerializedBagMessageConstSharedPtr;

}  // namespace rosbag2_storage

#endif  // ROSBAG2_STORAGE__SERIALIZED_BAG_MESSAGE_HPP_
