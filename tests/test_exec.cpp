#include <gtest/gtest.h>
#include <bruh/bruh.h>
#include <exec/clickbench.h>
#include <exec/operator.h>

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
                         core::Field("RegionID", core::DataType::Int64),
                         core::Field("SearchPhrase", core::DataType::String),
                         core::Field("EventDate", core::DataType::Date),
                         core::Field("URL", core::DataType::String),
                         core::Field("MobilePhoneModel", core::DataType::String),
                         core::Field("Title", core::DataType::String),
                         core::Field("EventTime", core::DataType::Timestamp),
                         core::Field("CounterID", core::DataType::Int64),
                         core::Field("DontCountHits", core::DataType::Int64),
                         core::Field("IsRefresh", core::DataType::Int64)});
}

void AppendRow(core::Batch& batch, int64_t adv, int64_t width, int64_t user, int64_t region,
               std::string_view search_phrase, std::string_view event_date, std::string_view url,
               std::string_view mobile_phone_model, std::string_view title,
               std::string_view event_time, int64_t counter_id, int64_t dont_count_hits,
               int64_t is_refresh) {
    batch.ColumnAt(0).AppendFromString(std::to_string(adv));
    batch.ColumnAt(1).AppendFromString(std::to_string(width));
    batch.ColumnAt(2).AppendFromString(std::to_string(user));
    batch.ColumnAt(3).AppendFromString(std::to_string(region));
    batch.ColumnAt(4).AppendFromString(search_phrase);
    batch.ColumnAt(5).AppendFromString(event_date);
    batch.ColumnAt(6).AppendFromString(url);
    batch.ColumnAt(7).AppendFromString(mobile_phone_model);
    batch.ColumnAt(8).AppendFromString(title);
    batch.ColumnAt(9).AppendFromString(event_time);
    batch.ColumnAt(10).AppendFromString(std::to_string(counter_id));
    batch.ColumnAt(11).AppendFromString(std::to_string(dont_count_hits));
    batch.ColumnAt(12).AppendFromString(std::to_string(is_refresh));
}

std::stringstream MakeClickBenchMiniFile() {
    auto schema = ClickBenchMiniSchema();
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        core::Batch batch(schema);
        AppendRow(batch, 0, 100, 10, 1, "alpha", "2013-07-02", "example.com", "iPhone", "Example",
                  "2013-07-02 10:00:00", 62, 0, 0);
        AppendRow(batch, 1, 200, 10, 1, "beta", "2013-07-01", "google.com", "Pixel", "Google News",
                  "2013-07-01 09:00:00", 62, 0, 0);
        AppendRow(batch, 2, 300, 20, 2, "alpha", "2013-07-03", "https://google.org/path", "iPhone",
                  "Other", "2013-07-03 08:00:00", 63, 0, 0);
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

core::Batch RunPlan(std::shared_ptr<exec::Operator> plan) {
    auto ss = MakeClickBenchMiniFile();
    bruh::BruhBatchReader reader(ss);
    auto batches = exec::Execute(reader, std::move(plan));
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
    EXPECT_EQ(result.ColumnAt(2).GetAsString(0), "200");
}

TEST(ClickBenchQueries, AvgUserId) {
    auto result = RunMiniQuery(3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "13");
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
    EXPECT_THROW(exec::ExecuteClickBenchQuery(ss, 11), std::runtime_error);
}

TEST(ProjectOperator, RenameColumn) {
    auto plan = exec::MakeProject(
        exec::MakeScan(),
        {exec::ProjectionUnit{exec::MakeColumnExpr("UserID", core::DataType::Int64), "u"}});
    auto result = RunPlan(plan);
    ASSERT_EQ(result.ColumnsCount(), 1);
    ASSERT_EQ(result.RowsCount(), 3);
    EXPECT_EQ(result.GetSchema().GetFields()[0].name, "u");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "10");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "10");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "20");
}

TEST(ProjectOperator, ConstantColumn) {
    auto plan = exec::MakeProject(
        exec::MakeScan(),
        {exec::ProjectionUnit{exec::MakeConst(static_cast<int64_t>(42)), "answer"}});
    auto result = RunPlan(plan);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "42");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "42");
}

TEST(HashAggregation, GroupByIntKey) {
    auto plan = exec::MakeHashAggregation(exec::MakeScan(),
                                          exec::MakeColumnExpr("UserID", core::DataType::Int64),
                                          "UserID", {exec::Count("count")});
    auto result = RunPlan(plan);
    ASSERT_EQ(result.RowsCount(), 2);
    int64_t total = 0;
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        auto user = result.ColumnAt(0).GetAsString(row);
        auto count = result.ColumnAt(1).GetAsString(row);
        total += std::stoll(count);
        if (user == "10") {
            EXPECT_EQ(count, "2");
        } else if (user == "20") {
            EXPECT_EQ(count, "1");
        } else {
            FAIL() << "unexpected key: " << user;
        }
    }
    EXPECT_EQ(total, 3);
}

TEST(HashAggregation, GroupByStringKey) {
    auto plan = exec::MakeHashAggregation(
        exec::MakeScan(), exec::MakeColumnExpr("SearchPhrase", core::DataType::String),
        "SearchPhrase", {exec::Count("count")});
    auto result = RunPlan(plan);
    ASSERT_EQ(result.RowsCount(), 2);
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        auto key = result.ColumnAt(0).GetAsString(row);
        auto count = result.ColumnAt(1).GetAsString(row);
        if (key == "alpha") {
            EXPECT_EQ(count, "2");
        } else if (key == "beta") {
            EXPECT_EQ(count, "1");
        } else {
            FAIL() << "unexpected key: " << key;
        }
    }
}

TEST(HashAggregation, MixedAggregates) {
    auto plan = exec::MakeHashAggregation(
        exec::MakeScan(), exec::MakeColumnExpr("UserID", core::DataType::Int64), "UserID",
        {exec::Sum(exec::MakeColumnExpr("AdvEngineID", core::DataType::Int64), "sum"),
         exec::Avg(exec::MakeColumnExpr("ResolutionWidth", core::DataType::Int64), "avg"),
         exec::Min(exec::MakeColumnExpr("ResolutionWidth", core::DataType::Int64), "min"),
         exec::Max(exec::MakeColumnExpr("ResolutionWidth", core::DataType::Int64), "max"),
         exec::Distinct(exec::MakeColumnExpr("SearchPhrase", core::DataType::String), "distinct")});
    auto result = RunPlan(plan);
    ASSERT_EQ(result.RowsCount(), 2);
    ASSERT_EQ(result.ColumnsCount(), 6);
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        auto user = result.ColumnAt(0).GetAsString(row);
        if (user == "10") {
            EXPECT_EQ(result.ColumnAt(1).GetAsString(row), "1");
            EXPECT_EQ(result.ColumnAt(2).GetAsString(row), "150");
            EXPECT_EQ(result.ColumnAt(3).GetAsString(row), "100");
            EXPECT_EQ(result.ColumnAt(4).GetAsString(row), "200");
            EXPECT_EQ(result.ColumnAt(5).GetAsString(row), "2");
        } else if (user == "20") {
            EXPECT_EQ(result.ColumnAt(1).GetAsString(row), "2");
            EXPECT_EQ(result.ColumnAt(2).GetAsString(row), "300");
            EXPECT_EQ(result.ColumnAt(3).GetAsString(row), "300");
            EXPECT_EQ(result.ColumnAt(4).GetAsString(row), "300");
            EXPECT_EQ(result.ColumnAt(5).GetAsString(row), "1");
        } else {
            FAIL() << "unexpected key: " << user;
        }
    }
}

TEST(TopNOperator, SortDescLimit) {
    auto plan = exec::MakeTopN(
        exec::MakeScan(),
        {exec::SortUnit{exec::MakeColumnExpr("AdvEngineID", core::DataType::Int64), false}}, 2);
    auto result = RunPlan(plan);
    ASSERT_EQ(result.RowsCount(), 2);
    auto idx = result.GetSchema().GetIndex("AdvEngineID");
    EXPECT_EQ(result.ColumnAt(idx).GetAsString(0), "2");
    EXPECT_EQ(result.ColumnAt(idx).GetAsString(1), "1");
}

TEST(TopNOperator, SortAscNoLimit) {
    auto plan = exec::MakeTopN(
        exec::MakeScan(),
        {exec::SortUnit{exec::MakeColumnExpr("ResolutionWidth", core::DataType::Int64), true}});
    auto result = RunPlan(plan);
    ASSERT_EQ(result.RowsCount(), 3);
    auto idx = result.GetSchema().GetIndex("ResolutionWidth");
    EXPECT_EQ(result.ColumnAt(idx).GetAsString(0), "100");
    EXPECT_EQ(result.ColumnAt(idx).GetAsString(1), "200");
    EXPECT_EQ(result.ColumnAt(idx).GetAsString(2), "300");
}

TEST(TopNOperator, MultiKeySort) {
    auto plan = exec::MakeTopN(
        exec::MakeScan(),
        {exec::SortUnit{exec::MakeColumnExpr("SearchPhrase", core::DataType::String), true},
         exec::SortUnit{exec::MakeColumnExpr("AdvEngineID", core::DataType::Int64), false}});
    auto result = RunPlan(plan);
    ASSERT_EQ(result.RowsCount(), 3);
    auto adv = result.GetSchema().GetIndex("AdvEngineID");
    auto phrase = result.GetSchema().GetIndex("SearchPhrase");
    EXPECT_EQ(result.ColumnAt(phrase).GetAsString(0), "alpha");
    EXPECT_EQ(result.ColumnAt(adv).GetAsString(0), "2");
    EXPECT_EQ(result.ColumnAt(phrase).GetAsString(1), "alpha");
    EXPECT_EQ(result.ColumnAt(adv).GetAsString(1), "0");
    EXPECT_EQ(result.ColumnAt(phrase).GetAsString(2), "beta");
    EXPECT_EQ(result.ColumnAt(adv).GetAsString(2), "1");
}

TEST(ClickBenchQueries, Q7GroupByCount) {
    auto result = RunMiniQuery(7);
    ASSERT_EQ(result.RowsCount(), 2);
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "1");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(1), "1");
}

TEST(ClickBenchQueries, Q8DistinctUserPerRegion) {
    auto result = RunMiniQuery(8);
    ASSERT_EQ(result.RowsCount(), 2);
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "1");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(1), "1");
}

TEST(ClickBenchQueries, Q12FilteredCountPerSearchPhrase) {
    auto result = RunMiniQuery(12);
    ASSERT_EQ(result.RowsCount(), 2);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "alpha");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "2");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "beta");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(1), "1");
}

TEST(ClickBenchQueries, Q15CountPerUser) {
    auto result = RunMiniQuery(15);
    ASSERT_EQ(result.RowsCount(), 2);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "10");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "2");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "20");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(1), "1");
}

TEST(ClickBenchQueries, Q19EmptyMatch) {
    auto ss = MakeClickBenchMiniFile();
    auto batches = exec::ExecuteClickBenchQuery(ss, 19);
    size_t total_rows = 0;
    for (auto& batch : batches) {
        total_rows += batch.RowsCount();
    }
    EXPECT_EQ(total_rows, 0);
}

TEST(ClickBenchQueries, Q20UrlContainsGoogle) {
    auto result = RunMiniQuery(20);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "2");
}

TEST(ClickBenchQueries, Q10DistinctUserPerMobileModel) {
    auto result = RunMiniQuery(10);
    ASSERT_EQ(result.RowsCount(), 2);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "iPhone");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "2");
}

TEST(ClickBenchQueries, Q13DistinctUserPerSearchPhrase) {
    auto result = RunMiniQuery(13);
    ASSERT_EQ(result.RowsCount(), 2);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "alpha");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "2");
}

TEST(ClickBenchQueries, Q21SearchPhraseMinUrlCount) {
    auto result = RunMiniQuery(21);
    ASSERT_EQ(result.RowsCount(), 2);
    EXPECT_EQ(result.ColumnsCount(), 3);
}

TEST(ClickBenchQueries, Q22TitleGoogleWithoutDotGoogle) {
    auto result = RunMiniQuery(22);
    ASSERT_EQ(result.RowsCount(), 1);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "beta");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "google.com");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(0), "Google News");
    EXPECT_EQ(result.ColumnAt(3).GetAsString(0), "1");
    EXPECT_EQ(result.ColumnAt(4).GetAsString(0), "1");
}

TEST(ClickBenchQueries, Q24SearchPhraseByEventTime) {
    auto result = RunMiniQuery(24);
    ASSERT_EQ(result.ColumnsCount(), 1);
    ASSERT_EQ(result.RowsCount(), 3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "beta");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "alpha");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "alpha");
}

TEST(ClickBenchQueries, Q25SearchPhraseBySearchPhrase) {
    auto result = RunMiniQuery(25);
    ASSERT_EQ(result.ColumnsCount(), 1);
    ASSERT_EQ(result.RowsCount(), 3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "alpha");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "alpha");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "beta");
}

TEST(ClickBenchQueries, Q26SearchPhraseByEventTimeAndSearchPhrase) {
    auto result = RunMiniQuery(26);
    ASSERT_EQ(result.ColumnsCount(), 1);
    ASSERT_EQ(result.RowsCount(), 3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "beta");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "alpha");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "alpha");
}

TEST(ClickBenchQueries, Q33CountPerUrl) {
    auto result = RunMiniQuery(33);
    EXPECT_EQ(result.RowsCount(), 3);
    EXPECT_EQ(result.ColumnsCount(), 2);
}

TEST(ClickBenchQueries, Q36PageViewsPerUrl) {
    auto result = RunMiniQuery(36);
    EXPECT_EQ(result.RowsCount(), 2);
    EXPECT_EQ(result.GetSchema().GetFields()[1].name, "PageViews");
}

TEST(ClickBenchQueries, Q37PageViewsPerTitle) {
    auto result = RunMiniQuery(37);
    EXPECT_EQ(result.RowsCount(), 2);
    EXPECT_EQ(result.GetSchema().GetFields()[1].name, "PageViews");
}
