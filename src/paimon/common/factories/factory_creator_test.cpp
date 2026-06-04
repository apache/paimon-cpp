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

#include "paimon/factories/factory_creator.h"

#include <algorithm>
#include <new>

#include "gtest/gtest.h"
#include "paimon/factories/factory.h"

namespace paimon::test {

class MockFactory : public Factory {
 public:
    explicit MockFactory(const std::string& name) : name_(name) {}
    const char* Identifier() const override {
        return "mock";
    }
    std::string GetName() const {
        return name_;
    }

 private:
    std::string name_;
};

class FactoryCreatorTest : public ::testing::Test {
 protected:
    void SetUp() override {
        factory_creator_ = FactoryCreator::GetInstance();
    }

    void TearDown() override {
        factory_creator_->~FactoryCreator();
        new (factory_creator_) FactoryCreator();
    }

    FactoryCreator* factory_creator_;
};

TEST_F(FactoryCreatorTest, RegisterAndCreateFactory) {
    auto factory1 = new MockFactory("Factory1");
    auto factory2 = new MockFactory("Factory2");

    factory_creator_->Register("type1", factory1);
    factory_creator_->Register("type2", factory2);

    Factory* created_factory1 = factory_creator_->Create("type1");
    Factory* created_factory2 = factory_creator_->Create("type2");

    ASSERT_NE(created_factory1, nullptr);
    ASSERT_NE(created_factory2, nullptr);

    EXPECT_EQ(static_cast<MockFactory*>(created_factory1)->GetName(), "Factory1");
    EXPECT_EQ(static_cast<MockFactory*>(created_factory2)->GetName(), "Factory2");
}

TEST_F(FactoryCreatorTest, GetRegisteredType) {
    auto factory1 = new MockFactory("Factory1");
    auto factory2 = new MockFactory("Factory2");

    factory_creator_->Register("type1", factory1);
    factory_creator_->Register("type2", factory2);

    std::vector<std::string> registered_types = factory_creator_->GetRegisteredType();
    EXPECT_EQ(registered_types.size(), 2);
    EXPECT_NE(std::find(registered_types.begin(), registered_types.end(), "type1"),
              registered_types.end());
    EXPECT_NE(std::find(registered_types.begin(), registered_types.end(), "type2"),
              registered_types.end());
}

TEST_F(FactoryCreatorTest, CreateNonExistentFactory) {
    Factory* created_factory = factory_creator_->Create("nonexistent");
    EXPECT_EQ(created_factory, nullptr);
}

}  // namespace paimon::test
