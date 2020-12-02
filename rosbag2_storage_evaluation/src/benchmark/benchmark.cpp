/*
 *  Copyright (c) 2018,  Bosch Software Innovations GmbH.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include "benchmark/benchmark.h"

#include <fstream>

void ros2bag::write_csv_file(
  std::string const & file_name, Benchmark const & benchmark, bool with_header)
{
  std::ofstream file;
  if (with_header) {
    std::remove(file_name.c_str());
  }
  file.open(file_name, std::ofstream::out | std::ofstream::app);
  benchmark.write_csv(file, with_header);
  file.close();
}
