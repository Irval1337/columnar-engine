#include <gtest/gtest.h>
#include "columnar/add.h"

TEST(Bruh, AddWorks) {
    EXPECT_EQ(columnar::Add(1, 2), 3);
}