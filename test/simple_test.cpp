//
// Created by gtoscano on 3/31/23.
//

#include "land_scenario.h"

#include <gtest/gtest.h>

class MyTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    virtual void TearDown() {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }
};

TEST_F(MyTest, MyVectorTest) {
    std::vector<int> v = {1, 2, 3, 4};
    ASSERT_EQ(v.size(), 4);
    ASSERT_EQ(v[2], 3);
}

TEST_F(MyTest, MyVectorTest2) {
    std::vector<int> v = {1, 2, 3, 4};
    ASSERT_EQ(v.size(), 3);
    ASSERT_EQ(v[2], 3);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
