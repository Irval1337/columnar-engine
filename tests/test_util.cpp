#include <gtest/gtest.h>
#include <util/string_arena.h>

#include <string>
#include <string_view>
#include <vector>

using namespace columnar;  // NOLINT

TEST(StringArenaTest, EmptyInternReturnsEmpty) {
    util::StringArena arena;
    auto view = arena.Intern("");
    EXPECT_TRUE(view.empty());
    EXPECT_EQ(arena.BytesUsed(), 0u);
    EXPECT_EQ(arena.BytesReserved(), 0u);
}

TEST(StringArenaTest, InternedStringIsACopy) {
    util::StringArena arena;
    std::string source = "hello world";
    auto view = arena.Intern(source);
    EXPECT_EQ(view, "hello world");
    EXPECT_NE(view.data(), source.data());
    source[0] = 'X';
    EXPECT_EQ(view, "hello world");
}

TEST(StringArenaTest, ManyInternsKeepPriorPointersStable) {
    util::StringArena arena;
    std::vector<std::string_view> views;
    views.reserve(10'000);
    for (int i = 0; i < 10'000; ++i) {
        views.push_back(arena.Intern("entry_" + std::to_string(i)));
    }
    for (int i = 0; i < 10'000; ++i) {
        EXPECT_EQ(views[i], "entry_" + std::to_string(i));
    }
}

TEST(StringArenaTest, OversizedInternGetsItsOwnChunk) {
    util::StringArena arena;
    std::string big(1 << 20, 'a');
    auto view = arena.Intern(big);
    EXPECT_EQ(view.size(), big.size());
    EXPECT_EQ(view, big);
    auto small = arena.Intern("tail");
    EXPECT_EQ(small, "tail");
}

TEST(StringArenaTest, ResetReclaimsAndAllowsReuse) {
    util::StringArena arena;
    auto a = arena.Intern("first");
    EXPECT_GT(arena.BytesUsed(), 0u);
    arena.Reset();
    EXPECT_EQ(arena.BytesUsed(), 0u);
    EXPECT_EQ(arena.BytesReserved(), 0u);
    auto b = arena.Intern("second");
    EXPECT_EQ(b, "second");
    (void)a;
}

TEST(StringArenaTest, MoveTransfersOwnership) {
    util::StringArena a;
    auto view = a.Intern("payload");
    util::StringArena b(std::move(a));
    EXPECT_EQ(view, "payload");
    EXPECT_EQ(b.BytesUsed(), 7u);
}

TEST(StringArenaTest, MovedFromArenaCanBeReused) {
    util::StringArena a;
    auto original = a.Intern("payload");
    util::StringArena b(std::move(a));

    auto reused = a.Intern("fresh");

    EXPECT_EQ(original, "payload");
    EXPECT_EQ(reused, "fresh");
    EXPECT_EQ(b.BytesUsed(), 7u);
    EXPECT_EQ(a.BytesUsed(), 5u);
}
