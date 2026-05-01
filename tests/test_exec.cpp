#include <gtest/gtest.h>
#include <bruh/bruh.h>
#include <exec/clickbench.h>

#include <sstream>
#include <string>
#include <string_view>
#include <utility>

using namespace columnar;  // NOLINT

namespace {
core::Schema ClickBenchMiniSchema() {
    return core::Schema({core::Field("AdvEngineID", core::DataType::Int64),
                         core::Field("ResolutionWidth", core::DataType::Int64),
                         core::Field("UserID", core::DataType::Int64),
                         core::Field("SearchPhrase", core::DataType::String),
                         core::Field("EventDate", core::DataType::String)});
}

void AppendRow(core::Batch& batch, int64_t adv, int64_t width, int64_t user,
               std::string_view search_phrase, std::string_view event_date) {
    batch.ColumnAt(0).AppendFromString(std::to_string(adv));
    batch.ColumnAt(1).AppendFromString(std::to_string(width));
    batch.ColumnAt(2).AppendFromString(std::to_string(user));
    batch.ColumnAt(3).AppendFromString(search_phrase);
    batch.ColumnAt(4).AppendFromString(event_date);
}

std::stringstream MakeClickBenchMiniFile() {
    auto schema = ClickBenchMiniSchema();
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        core::Batch batch(schema);
        AppendRow(batch, 0, 100, 10, "alpha", "2013-07-02");
        AppendRow(batch, 1, 200, 10, "beta", "2013-07-01");
        AppendRow(batch, 2, 300, 20, "alpha", "2013-07-03");
        writer.Write(batch);
        writer.Flush();
    }
    ss.seekg(0);
    return ss;
}

core::Batch RunMiniQuery(size_t query_id) {
    auto ss = MakeClickBenchMiniFile();
    auto batches = exec::ExecuteClickBenchQuery(ss, query_id);
    EXPECT_EQ(batches.size(), 1);
    return std::move(batches[0]);
}
}  // namespace

TEST(BruhBatchReader, ReadProjectedRowGroup) {
    auto ss = MakeClickBenchMiniFile();
    bruh::BruhBatchReader reader(ss);

    auto projected = reader.ReadRowGroup(0, std::vector<std::string>{"UserID", "SearchPhrase"});
    EXPECT_EQ(projected.ColumnsCount(), 2);
    EXPECT_EQ(projected.RowsCount(), 3);
    EXPECT_EQ(projected.GetSchema().GetFields()[0].name, "UserID");
    EXPECT_EQ(projected.GetSchema().GetFields()[1].name, "SearchPhrase");
    EXPECT_EQ(projected.ColumnAt(0).GetAsString(2), "20");
    EXPECT_EQ(projected.ColumnAt(1).GetAsString(1), "beta");
}

TEST(BruhBatchReader, ReadZeroColumnsReturnsEmptyBatch) {
    auto ss = MakeClickBenchMiniFile();
    bruh::BruhBatchReader reader(ss);

    auto projected = reader.ReadRowGroup(0, std::vector<size_t>{});
    EXPECT_EQ(projected.ColumnsCount(), 0);
    EXPECT_EQ(projected.RowsCount(), 0);
}

TEST(ClickBenchQueries, CountStar) {
    auto result = RunMiniQuery(0);
    EXPECT_EQ(result.GetSchema().GetFields()[0].name, "count");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "3");
}

TEST(ClickBenchQueries, FilteredCount) {
    auto result = RunMiniQuery(1);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "2");
}

TEST(ClickBenchQueries, SumCountAvg) {
    auto result = RunMiniQuery(2);
    EXPECT_EQ(result.ColumnsCount(), 3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "3");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "3");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(0), "200.000000");
}

TEST(ClickBenchQueries, AvgUserId) {
    auto result = RunMiniQuery(3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "13.333333");
}

TEST(ClickBenchQueries, CountDistinctUserId) {
    auto result = RunMiniQuery(4);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "2");
}

TEST(ClickBenchQueries, CountDistinctSearchPhrase) {
    auto result = RunMiniQuery(5);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "2");
}

TEST(ClickBenchQueries, MinMaxEventDate) {
    auto result = RunMiniQuery(6);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "2013-07-01");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "2013-07-03");
}

TEST(ClickBenchQueries, UnsupportedQueryThrows) {
    auto ss = MakeClickBenchMiniFile();
    EXPECT_THROW(exec::ExecuteClickBenchQuery(ss, 7), std::runtime_error);
}
