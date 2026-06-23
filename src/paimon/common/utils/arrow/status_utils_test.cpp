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

#include "paimon/common/utils/arrow/status_utils.h"

#include "gtest/gtest.h"

namespace paimon::test {

TEST(StatusUtilsTest, ToArrowStatus) {
    ASSERT_EQ(arrow::Status::OK(), ToArrowStatus(Status::OK()));
    std::string msg = "msg";
    ASSERT_EQ(arrow::Status::OutOfMemory(msg), ToArrowStatus(Status::OutOfMemory(msg)));
    ASSERT_EQ(arrow::Status::KeyError(msg), ToArrowStatus(Status::KeyError(msg)));
    ASSERT_EQ(arrow::Status::TypeError(msg), ToArrowStatus(Status::TypeError(msg)));
    ASSERT_EQ(arrow::Status::Invalid(msg), ToArrowStatus(Status::Invalid(msg)));
    ASSERT_EQ(arrow::Status::IOError(msg), ToArrowStatus(Status::IOError(msg)));
    ASSERT_EQ(arrow::Status::CapacityError(msg), ToArrowStatus(Status::CapacityError(msg)));
    ASSERT_EQ(arrow::Status::IndexError(msg), ToArrowStatus(Status::IndexError(msg)));
    ASSERT_EQ(arrow::Status::UnknownError(msg), ToArrowStatus(Status::UnknownError(msg)));
    ASSERT_EQ(arrow::Status::NotImplemented(msg), ToArrowStatus(Status::NotImplemented(msg)));
    ASSERT_EQ(arrow::Status::SerializationError(msg),
              ToArrowStatus(Status::SerializationError(msg)));
    ASSERT_EQ(arrow::Status::Invalid(msg), ToArrowStatus(Status::Invalid(msg)));
}

TEST(StatusUtilsTest, ToPaimonStatus) {
    ASSERT_EQ(Status::OK(), ToPaimonStatus(arrow::Status::OK()));
    std::string msg = "msg";
    ASSERT_EQ(Status::OutOfMemory(msg), ToPaimonStatus(arrow::Status::OutOfMemory(msg)));
    ASSERT_EQ(Status::KeyError(msg), ToPaimonStatus(arrow::Status::KeyError(msg)));
    ASSERT_EQ(Status::TypeError(msg), ToPaimonStatus(arrow::Status::TypeError(msg)));
    ASSERT_EQ(Status::Invalid(msg), ToPaimonStatus(arrow::Status::Invalid(msg)));
    ASSERT_EQ(Status::IOError(msg), ToPaimonStatus(arrow::Status::IOError(msg)));
    ASSERT_EQ(Status::CapacityError(msg), ToPaimonStatus(arrow::Status::CapacityError(msg)));
    ASSERT_EQ(Status::IndexError(msg), ToPaimonStatus(arrow::Status::IndexError(msg)));
    ASSERT_EQ(Status::UnknownError(msg), ToPaimonStatus(arrow::Status::UnknownError(msg)));
    ASSERT_EQ(Status::NotImplemented(msg), ToPaimonStatus(arrow::Status::NotImplemented(msg)));
    ASSERT_EQ(Status::SerializationError(msg),
              ToPaimonStatus(arrow::Status::SerializationError(msg)));
    ASSERT_EQ(Status::Invalid(msg), ToPaimonStatus(arrow::Status::Invalid(msg)));
}

}  // namespace paimon::test
