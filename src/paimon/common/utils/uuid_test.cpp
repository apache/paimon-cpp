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

#include "paimon/common/utils/uuid.h"

#include <string>

#include "gtest/gtest.h"

namespace paimon::test {

// Test case: Successfully generate a valid UUID
TEST(UUIDTest, GenerateValidUUID) {
    std::string output;
    bool result = UUID::Generate(&output);

    // Ensure that the result is true (UUID is valid)
    EXPECT_TRUE(result);

    // Ensure that the generated UUID has 36 characters
    EXPECT_EQ(output.size(), 36);

    // Ensure the UUID contains dashes in the correct positions
    EXPECT_TRUE(output.find('-') == 8 || output.find('-') == 13 || output.find('-') == 18 ||
                output.find('-') == 23);
}

}  // namespace paimon::test
