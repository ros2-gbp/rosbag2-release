// Copyright 2018-2021, Bosch Software Innovations GmbH.
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

#include <chrono>
#include <iostream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#ifdef _WIN32
#include <time.h>
#endif

#include "rosbag2_cpp/action_utils.hpp"
#include "rosbag2_cpp/service_utils.hpp"
#include "rosbag2_storage/bag_metadata.hpp"

#include "action_info.hpp"
#include "format_bag_metadata.hpp"
#include "service_event_info.hpp"

namespace
{

void indent(std::stringstream & info_stream, int number_of_spaces)
{
  info_stream << std::string(number_of_spaces, ' ');
}

std::unordered_map<std::string, std::string> format_duration(
  std::chrono::high_resolution_clock::duration duration)
{
  std::unordered_map<std::string, std::string> formatted_duration;
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
  auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);
  auto nanoseconds_from_seconds = std::chrono::duration_cast<std::chrono::nanoseconds>(seconds);
  std::stringstream fractional_seconds_ss;
  fractional_seconds_ss << std::setw(9) << std::setfill('0') <<
    (nanoseconds - nanoseconds_from_seconds).count();
  std::time_t std_time_point = seconds.count();
  tm time;
#ifdef _WIN32
  localtime_s(&time, &std_time_point);
#else
  localtime_r(&std_time_point, &time);
#endif
  std::stringstream formatted_date;
  std::stringstream formatted_time;
  formatted_date << std::put_time(&time, "%b %e %Y");
  formatted_time << std::put_time(&time, "%H:%M:%S") << "." << fractional_seconds_ss.str();
  formatted_duration["date"] = formatted_date.str();
  formatted_duration["time"] = formatted_time.str();
  formatted_duration["time_in_sec"] = std::to_string(seconds.count()) + "." +
    fractional_seconds_ss.str();

  return formatted_duration;
}

std::string format_time_point(
  std::chrono::high_resolution_clock::duration duration)
{
  auto formatted_duration = format_duration(duration);
  return formatted_duration["date"] + " " + formatted_duration["time"] +
         " (" + formatted_duration["time_in_sec"] + ")";
}

std::string format_file_size(uint64_t file_size)
{
  double size = static_cast<double>(file_size);
  static const char * units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
  double reference_number_bytes = 1024;
  int index = 0;
  while (size >= reference_number_bytes && index < 4) {
    size /= reference_number_bytes;
    index++;
  }

  std::stringstream rounded_size;
  int size_format_precision = index == 0 ? 0 : 1;
  rounded_size << std::setprecision(size_format_precision) << std::fixed << size;
  return rounded_size.str() + " " + units[index];
}

void format_file_paths(
  const std::vector<std::string> & paths,
  std::stringstream & info_stream,
  int indentation_spaces)
{
  if (paths.empty()) {
    info_stream << std::endl;
    return;
  }

  info_stream << paths[0] << std::endl;
  size_t number_of_files = paths.size();
  for (size_t i = 1; i < number_of_files; i++) {
    indent(info_stream, indentation_spaces);
    info_stream << paths[i] << std::endl;
  }
}

void format_topics_with_type(
  const std::vector<rosbag2_storage::TopicInformation> & topics,
  const std::unordered_map<std::string, uint64_t> & messages_size,
  bool verbose,
  std::stringstream & info_stream,
  int indentation_spaces,
  const rosbag2_py::InfoSortingMethod sort_method = rosbag2_py::InfoSortingMethod::NAME)
{
  if (topics.empty()) {
    info_stream << std::endl;
    return;
  }

  auto print_topic_info =
    [&info_stream, &messages_size, verbose](const rosbag2_storage::TopicInformation & ti) -> void {
      info_stream << "Topic: " << ti.topic_metadata.name << " | ";
      info_stream << "Type: " << ti.topic_metadata.type << " | ";
      info_stream << "Count: " << ti.message_count << " | ";
      if (verbose) {
        uint64_t topic_size = 0;
        auto topic_size_iter = messages_size.find(ti.topic_metadata.name);
        if (topic_size_iter != messages_size.end()) {
          topic_size = topic_size_iter->second;
        }
        info_stream << "Size Contribution: " << format_file_size(topic_size) << " | ";
      }
      info_stream << "Serialization Format: " << ti.topic_metadata.serialization_format;
      info_stream << std::endl;
    };

  std::vector<size_t> sorted_idx = rosbag2_py::generate_sorted_idx(topics, sort_method);

  size_t number_of_topics = topics.size();
  size_t i = 0;
  // Find first topic which is unrelated to service or action.
  while (i < number_of_topics &&
    (rosbag2_cpp::is_service_event_topic(
      topics[sorted_idx[i]].topic_metadata.name,
      topics[sorted_idx[i]].topic_metadata.type) ||
    rosbag2_cpp::is_topic_belong_to_action(
      topics[sorted_idx[i]].topic_metadata.name,
      topics[sorted_idx[i]].topic_metadata.type)))
  {
    i++;
  }

  if (i == number_of_topics) {
    info_stream << std::endl;
    return;
  }

  print_topic_info(topics[sorted_idx[i]]);
  for (size_t j = ++i; j < number_of_topics; ++j) {
    if (rosbag2_cpp::is_service_event_topic(
        topics[sorted_idx[j]].topic_metadata.name, topics[sorted_idx[j]].topic_metadata.type) ||
      rosbag2_cpp::is_topic_belong_to_action(
        topics[sorted_idx[j]].topic_metadata.name, topics[sorted_idx[j]].topic_metadata.type))
    {
      continue;
    }
    indent(info_stream, indentation_spaces);
    print_topic_info(topics[sorted_idx[j]]);
  }
}

using ServiceInfoList = std::vector<std::shared_ptr<rosbag2_py::ServiceEventInformation>>;
using ActionInfoList = std::vector<std::shared_ptr<rosbag2_py::ActionInformation>>;
using ActionInfoMap =
  std::unordered_map<std::string, std::shared_ptr<rosbag2_py::ActionInformation>>;

void filter_service_and_action_info(
  const std::vector<rosbag2_storage::TopicInformation> & topics_with_message_count,
  ServiceInfoList & service_info_list,
  size_t & total_service_event_msg_count,
  ActionInfoList & action_info_list,
  size_t & total_action_msg_count)
{
  total_service_event_msg_count = 0;
  total_action_msg_count = 0;

  ActionInfoMap action_info_map;

  for (auto & topic : topics_with_message_count) {
    const auto action_interface_type =
      rosbag2_cpp::get_action_interface_type(topic.topic_metadata.name);
    if (rosbag2_cpp::is_topic_belong_to_action(action_interface_type, topic.topic_metadata.type)) {
      auto action_name =
        rosbag2_cpp::action_interface_name_to_action_name(topic.topic_metadata.name);

      if (action_info_map.find(action_name) == action_info_map.end()) {
        action_info_map[action_name] = std::make_shared<rosbag2_py::ActionInformation>();
        action_info_map[action_name]->action_metadata.name = action_name;
        action_info_map[action_name]->action_metadata.serialization_format =
          topic.topic_metadata.serialization_format;
      }

      // If the cancel_goal or status message is processed first, it can result in the type
      // being empty. So If the type is empty, it will be updated with subsequent messages.
      if (action_info_map[action_name]->action_metadata.type.empty()) {
        action_info_map[action_name]->action_metadata.type =
          rosbag2_cpp::get_action_type_for_info(topic.topic_metadata.type);
      }

      switch (action_interface_type) {
        case rosbag2_cpp::ActionInterfaceType::SendGoalEvent:
          action_info_map[action_name]->send_goal_event_message_count = topic.message_count;
          break;
        case rosbag2_cpp::ActionInterfaceType::CancelGoalEvent:
          action_info_map[action_name]->cancel_goal_event_message_count = topic.message_count;
          break;
        case rosbag2_cpp::ActionInterfaceType::GetResultEvent:
          action_info_map[action_name]->get_result_event_message_count = topic.message_count;
          break;
        case rosbag2_cpp::ActionInterfaceType::Feedback:
          action_info_map[action_name]->feedback_message_count = topic.message_count;
          break;
        case rosbag2_cpp::ActionInterfaceType::Status:
          action_info_map[action_name]->status_message_count = topic.message_count;
          break;
        default:  // Never go here
          throw std::out_of_range("Invalid action interface type");
      }
      total_action_msg_count += topic.message_count;
      continue;
    }

    if (rosbag2_cpp::is_service_event_topic(
        topic.topic_metadata.name, topic.topic_metadata.type))
    {
      auto service_info = std::make_shared<rosbag2_py::ServiceEventInformation>();
      service_info->service_metadata.name =
        rosbag2_cpp::service_event_topic_name_to_service_name(topic.topic_metadata.name);
      service_info->service_metadata.type =
        rosbag2_cpp::service_event_topic_type_to_service_type(topic.topic_metadata.type);
      service_info->service_metadata.serialization_format =
        topic.topic_metadata.serialization_format;
      service_info->event_message_count = topic.message_count;
      total_service_event_msg_count += topic.message_count;
      service_info_list.emplace_back(service_info);
    }
  }

  // Convert action_info_map to action_info_list
  for (auto & action_info : action_info_map) {
    action_info_list.emplace_back(action_info.second);
  }
}

void format_service_with_type(
  const std::vector<std::shared_ptr<rosbag2_py::ServiceEventInformation>> & services,
  const std::unordered_map<std::string, uint64_t> & messages_size,
  bool verbose,
  std::stringstream & info_stream,
  int indentation_spaces,
  const rosbag2_py::InfoSortingMethod sort_method = rosbag2_py::InfoSortingMethod::NAME)
{
  if (services.empty()) {
    info_stream << std::endl;
    return;
  }

  auto print_service_info =
    [&info_stream, &messages_size, verbose](
    const std::shared_ptr<rosbag2_py::ServiceEventInformation> & si) -> void {
      info_stream << "Service: " << si->service_metadata.name << " | ";
      info_stream << "Type: " << si->service_metadata.type << " | ";
      info_stream << "Event Count: " << si->event_message_count << " | ";
      if (verbose) {
        uint64_t service_size = 0;
        auto service_size_iter = messages_size.find(
          rosbag2_cpp::service_name_to_service_event_topic_name(si->service_metadata.name));
        if (service_size_iter != messages_size.end()) {
          service_size = service_size_iter->second;
        }
        info_stream << "Size Contribution: " << format_file_size(service_size) << " | ";
      }
      info_stream << "Serialization Format: " << si->service_metadata.serialization_format;
      info_stream << std::endl;
    };

  std::vector<size_t> sorted_idx = rosbag2_py::generate_sorted_idx(services, sort_method);

  print_service_info(services[sorted_idx[0]]);
  auto number_of_services = services.size();
  for (size_t j = 1; j < number_of_services; ++j) {
    indent(info_stream, indentation_spaces);
    print_service_info(services[sorted_idx[j]]);
  }
}

void format_action_with_type(
  const std::vector<std::shared_ptr<rosbag2_py::ActionInformation>> & actions,
  std::stringstream & info_stream,
  const rosbag2_py::InfoSortingMethod sort_method = rosbag2_py::InfoSortingMethod::NAME)
{
  info_stream << std::endl;
  if (actions.empty()) {
    return;
  }

  auto print_action_info =
    [&info_stream](const std::shared_ptr<rosbag2_py::ActionInformation> & ai) -> void {
      info_stream << "  Action: " << ai->action_metadata.name << " | ";
      info_stream << "Type: " << ai->action_metadata.type << " | ";
      info_stream << "Topics: 2" << " | ";
      info_stream << "Service: 3" << " | ";
      info_stream << "Serialization Format: "
                  << ai->action_metadata.serialization_format << std::endl;
      info_stream << "    Topic: feedback | Count: " << ai->feedback_message_count << std::endl;
      info_stream << "    Topic: status | Count: " << ai->status_message_count << std::endl;
      info_stream << "    Service: send_goal | Event Count: "
                  << ai->get_result_event_message_count << std::endl;
      info_stream << "    Service: cancel_goal | Event Count: "
                  << ai->cancel_goal_event_message_count << std::endl;
      info_stream << "    Service: get_result | Event Count: "
                  << ai->get_result_event_message_count;
    };

  std::vector<size_t> sorted_idx = rosbag2_py::generate_sorted_idx(actions, sort_method);

  print_action_info(actions[sorted_idx[0]]);
  auto number_of_services = actions.size();
  for (size_t j = 1; j < number_of_services; ++j) {
    info_stream << std::endl;
    print_action_info(actions[sorted_idx[j]]);
  }
}

}  // namespace

namespace rosbag2_py
{

std::string format_bag_meta_data(
  const rosbag2_storage::BagMetadata & metadata,
  const std::unordered_map<std::string, uint64_t> & messages_size,
  bool verbose,
  bool only_topic,
  const InfoSortingMethod sort_method)
{
  auto start_time = metadata.starting_time.time_since_epoch();
  auto end_time = start_time + metadata.duration;
  std::stringstream info_stream;
  int indentation_spaces = 19;  // The longest info field (Topics with Type:) plus one space.
  std::string ros_distro = metadata.ros_distro;
  if (ros_distro.empty()) {
    ros_distro = "unknown";
  }

  ServiceInfoList service_info_list;
  size_t total_service_event_msg_count = 0;
  ActionInfoList action_info_list;
  size_t total_action_msg_count = 0;

  filter_service_and_action_info(
    metadata.topics_with_message_count,
    service_info_list,
    total_service_event_msg_count,
    action_info_list,
    total_action_msg_count);

  info_stream << std::endl;
  info_stream << "Files:             ";
  format_file_paths(metadata.relative_file_paths, info_stream, indentation_spaces);
  info_stream << "Bag size:          " << format_file_size(
    metadata.bag_size) << std::endl;
  info_stream << "Storage id:        " << metadata.storage_identifier << std::endl;
  info_stream << "ROS Distro:        " << ros_distro << std::endl;
  info_stream << "Duration:          " << format_duration(
    metadata.duration)["time_in_sec"] << "s" << std::endl;
  info_stream << "Start:             " << format_time_point(start_time) <<
    std::endl;
  info_stream << "End:               " << format_time_point(end_time) << std::endl;
  info_stream << "Messages:          "
              << metadata.message_count - total_service_event_msg_count - total_action_msg_count
              << std::endl;
  info_stream << "Topic information: ";
  format_topics_with_type(
    metadata.topics_with_message_count,
    messages_size, verbose, info_stream,
    indentation_spaces,
    sort_method);

  if (!only_topic) {
    info_stream << "Services:          " << service_info_list.size() << std::endl;
    info_stream << "Service information: ";
    if (!service_info_list.empty()) {
      format_service_with_type(
        service_info_list,
        messages_size,
        verbose,
        info_stream,
        indentation_spaces + 2,
        sort_method);
    } else {
      info_stream << std::endl;
    }
    info_stream << "Actions:           " << action_info_list.size() << std::endl;
    info_stream << "Action information: ";
    if (!action_info_list.empty()) {
      format_action_with_type(
        action_info_list,
        info_stream,
        sort_method);
    }
  }

  return info_stream.str();
}

}  // namespace rosbag2_py
