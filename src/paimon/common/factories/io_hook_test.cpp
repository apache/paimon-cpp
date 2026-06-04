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

#include "paimon/common/factories/io_hook.h"

#include <stdexcept>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(IOHookTest, TestReturnErrorMode) {
    auto hook = IOHook::GetInstance();
    hook->Reset(0, IOHook::Mode::RETURN_ERROR);
    ASSERT_NOK(hook->Try("path"));
    ASSERT_NOK(hook->Try("path"));
    ASSERT_EQ(2, hook->IOCount());
    hook->Clear();
}

TEST(IOHookTest, TestSilentMode) {
    auto hook = IOHook::GetInstance();
    hook->Reset(0, IOHook::Mode::SILENT);
    ASSERT_OK(hook->Try("path"));
    ASSERT_OK(hook->Try("path"));
    ASSERT_EQ(2, hook->IOCount());
    hook->Clear();
}

TEST(IOHookTest, TestSingleton) {
    auto hook = IOHook::GetInstance();
    auto hook2 = IOHook::GetInstance();
    ASSERT_EQ(hook, hook2);
    hook->Clear();
}

TEST(IOHookTest, TestThrowExceptionMode) {
    auto hook = IOHook::GetInstance();
    hook->Reset(0, IOHook::Mode::THROW_EXCEPTION);
    auto Try = [hook]() {
        auto s = hook->Try("path");
        (void)s;
    };
    EXPECT_THROW(Try(), std::runtime_error);
    EXPECT_THROW(Try(), std::runtime_error);
    ASSERT_EQ(2, hook->IOCount());
    hook->Clear();
}

}  // namespace paimon::test
