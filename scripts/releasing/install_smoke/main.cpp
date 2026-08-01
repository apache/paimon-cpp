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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cstdint>
#include <limits>
#include <string>

#include "paimon/status.h"
#include "paimon/utils/special_field_ids.h"

int main() {
    const paimon::Status status = paimon::Status::Invalid("install smoke test");
    const int32_t* row_id = &paimon::SpecialFieldIds::ROW_ID;
    const bool valid_status =
        !status.ok() && status.ToString().find("install smoke test") != std::string::npos;
    const bool valid_row_id = *row_id == std::numeric_limits<int32_t>::max() - 5;
    return valid_status && valid_row_id ? 0 : 1;
}
