#include <gtest/gtest.h>
#include <bruh/bruh.h>
#include <core/columns/dictionary_string_column.h>
#include <core/columns/string_column.h>
#include <core/columns/timestamp_column.h>
#include <exec/clickbench.h>
#include <exec/kernel.h>
#include <exec/operator.h>

#include <cstring>
#include <set>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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
                         core::Field("IsRefresh", core::DataType::Int64),
                         core::Field("MobilePhone", core::DataType::Int64),
                         core::Field("SearchEngineID", core::DataType::Int64),
                         core::Field("ClientIP", core::DataType::Int64),
                         core::Field("WatchID", core::DataType::Int64),
                         core::Field("IsLink", core::DataType::Int64),
                         core::Field("IsDownload", core::DataType::Int64),
                         core::Field("URLHash", core::DataType::Int64),
                         core::Field("RefererHash", core::DataType::Int64),
                         core::Field("Referer", core::DataType::String),
                         core::Field("WindowClientWidth", core::DataType::Int64),
                         core::Field("WindowClientHeight", core::DataType::Int64),
                         core::Field("TraficSourceID", core::DataType::Int64)});
}

struct Row {
    int64_t adv, width, user, region;
    std::string_view search_phrase, event_date, url, mobile_phone_model, title, event_time;
    int64_t counter_id, dont_count_hits, is_refresh, mobile_phone, search_engine_id, client_ip,
        watch_id, is_link, is_download, url_hash, referer_hash;
    std::string_view referer;
    int64_t window_client_width, window_client_height, trafic_source_id;
};

void AppendRow(core::Batch& batch, const Row& r) {
    batch.ColumnAt(0).AppendFromString(std::to_string(r.adv));
    batch.ColumnAt(1).AppendFromString(std::to_string(r.width));
    batch.ColumnAt(2).AppendFromString(std::to_string(r.user));
    batch.ColumnAt(3).AppendFromString(std::to_string(r.region));
    batch.ColumnAt(4).AppendFromString(r.search_phrase);
    batch.ColumnAt(5).AppendFromString(r.event_date);
    batch.ColumnAt(6).AppendFromString(r.url);
    batch.ColumnAt(7).AppendFromString(r.mobile_phone_model);
    batch.ColumnAt(8).AppendFromString(r.title);
    batch.ColumnAt(9).AppendFromString(r.event_time);
    batch.ColumnAt(10).AppendFromString(std::to_string(r.counter_id));
    batch.ColumnAt(11).AppendFromString(std::to_string(r.dont_count_hits));
    batch.ColumnAt(12).AppendFromString(std::to_string(r.is_refresh));
    batch.ColumnAt(13).AppendFromString(std::to_string(r.mobile_phone));
    batch.ColumnAt(14).AppendFromString(std::to_string(r.search_engine_id));
    batch.ColumnAt(15).AppendFromString(std::to_string(r.client_ip));
    batch.ColumnAt(16).AppendFromString(std::to_string(r.watch_id));
    batch.ColumnAt(17).AppendFromString(std::to_string(r.is_link));
    batch.ColumnAt(18).AppendFromString(std::to_string(r.is_download));
    batch.ColumnAt(19).AppendFromString(std::to_string(r.url_hash));
    batch.ColumnAt(20).AppendFromString(std::to_string(r.referer_hash));
    batch.ColumnAt(21).AppendFromString(r.referer);
    batch.ColumnAt(22).AppendFromString(std::to_string(r.window_client_width));
    batch.ColumnAt(23).AppendFromString(std::to_string(r.window_client_height));
    batch.ColumnAt(24).AppendFromString(std::to_string(r.trafic_source_id));
}

std::string MakeClickBenchMiniFile() {
    auto schema = ClickBenchMiniSchema();
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        core::Batch batch(schema);
        AppendRow(batch, {0,
                          100,
                          10,
                          1,
                          "alpha",
                          "2013-07-02",
                          "example.com",
                          "iPhone",
                          "Example",
                          "2013-07-02 10:00:00",
                          62,
                          0,
                          0,
                          1,
                          2,
                          1000,
                          7,
                          1,
                          0,
                          111,
                          222,
                          "http://www.referrer.example/page",
                          1920,
                          1080,
                          -1});
        AppendRow(batch, {1,
                          200,
                          10,
                          1,
                          "beta",
                          "2013-07-01",
                          "google.com",
                          "Pixel",
                          "Google News",
                          "2013-07-01 09:00:00",
                          62,
                          0,
                          0,
                          1,
                          3,
                          1000,
                          8,
                          0,
                          0,
                          111,
                          222,
                          "https://news.google.com/feed",
                          1366,
                          768,
                          6});
        AppendRow(batch, {2,
                          300,
                          20,
                          2,
                          "alpha",
                          "2013-07-03",
                          "https://google.org/path",
                          "iPhone",
                          "Other",
                          "2013-07-03 08:00:00",
                          63,
                          0,
                          0,
                          2,
                          2,
                          2000,
                          8,
                          1,
                          1,
                          333,
                          222,
                          "example.org",
                          1920,
                          1080,
                          2});
        writer.Write(batch);
        writer.Flush();
    }
    return ss.str();
}

util::ByteView AsBytes(const std::string& s) {
    return util::ByteView{reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

core::Batch RunMiniQuery(size_t query_id) {
    auto buf = MakeClickBenchMiniFile();
    auto batches = exec::ExecuteClickBenchQuery(AsBytes(buf), query_id);
    EXPECT_EQ(batches.size(), 1);
    return std::move(batches[0]);
}

core::Batch RunPlan(std::shared_ptr<exec::Operator> plan) {
    auto buf = MakeClickBenchMiniFile();
    bruh::BruhBatchReader reader(AsBytes(buf));
    auto batches = exec::Execute(reader, std::move(plan));
    EXPECT_EQ(batches.size(), 1);
    return std::move(batches[0]);
}

core::Batch RunPlanOnBatch(const core::Batch& batch, std::shared_ptr<exec::Operator> plan) {
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    bruh::BruhBatchWriter writer(ss, batch.GetSchema());
    writer.Write(batch);
    writer.Flush();
    auto buf = ss.str();
    bruh::BruhBatchReader reader(AsBytes(buf));
    auto batches = exec::Execute(reader, std::move(plan));
    EXPECT_EQ(batches.size(), 1);
    return std::move(batches[0]);
}

std::unique_ptr<core::DictionaryStringColumn> MakeDictionaryStringColumn(
    std::vector<std::string_view> dict_values,
    std::vector<uint32_t> ids) {
    std::vector<char> data;
    std::vector<size_t> offsets;
    offsets.reserve(dict_values.size() + 1);
    offsets.push_back(0);
    for (auto value : dict_values) {
        data.insert(data.end(), value.begin(), value.end());
        offsets.push_back(data.size());
    }
    return std::make_unique<core::DictionaryStringColumn>(
        std::move(data), std::move(offsets), std::move(ids), util::BitVector(), false);
}

size_t TotalRows(const std::vector<core::Batch>& batches) {
    size_t total = 0;
    for (auto& batch : batches) {
        total += batch.RowsCount();
    }
    return total;
}
}  // namespace

TEST(BruhBatchReader, ReadProjectedRowGroup) {
    auto buf = MakeClickBenchMiniFile();
    bruh::BruhBatchReader reader(AsBytes(buf));

    auto projected = reader.ReadRowGroup(0, std::vector<std::string>{"UserID", "SearchPhrase"});
    EXPECT_EQ(projected.ColumnsCount(), 2);
    EXPECT_EQ(projected.RowsCount(), 3);
    EXPECT_EQ(projected.GetSchema().GetFields()[0].name, "UserID");
    EXPECT_EQ(projected.GetSchema().GetFields()[1].name, "SearchPhrase");
    EXPECT_EQ(projected.ColumnAt(0).GetAsString(2), "20");
    EXPECT_EQ(projected.ColumnAt(1).GetAsString(1), "beta");
}

TEST(BruhBatchReader, ReadZeroColumnsReturnsEmptyBatch) {
    auto buf = MakeClickBenchMiniFile();
    bruh::BruhBatchReader reader(AsBytes(buf));

    auto projected = reader.ReadRowGroup(0, std::vector<size_t>{});
    EXPECT_EQ(projected.ColumnsCount(), 0);
    EXPECT_EQ(projected.RowsCount(), 0);
}

TEST(Execution, FilterSkipsRowGroupsByStatistics) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions options;
        options.compression = util::Compression::None;
        options.encoding = core::Encoding::Plain;
        bruh::BruhBatchWriter writer(ss, schema, options);

        core::Batch skipped(schema);
        skipped.ColumnAt(0).AppendFromString("1");
        skipped.ColumnAt(0).AppendFromString("2");
        writer.Write(skipped);

        core::Batch matched(schema);
        matched.ColumnAt(0).AppendFromString("5");
        matched.ColumnAt(0).AppendFromString("5");
        writer.Write(matched);
        writer.Flush();
    }

    auto buf = ss.str();
    bruh::BruhBatchReader metadata_reader(AsBytes(buf));
    auto& chunk = metadata_reader.GetMetaData().row_groups[0].columns[0];
    int64_t poison = 5;
    auto offset = static_cast<size_t>(chunk.offset);
    std::memcpy(buf.data() + offset, &poison, sizeof(poison));
    std::memcpy(buf.data() + offset + sizeof(poison), &poison, sizeof(poison));

    bruh::BruhBatchReader reader(AsBytes(buf));
    auto condition = exec::MakeBinary(exec::BinaryFunction::Equal,
                                      exec::MakeColumnExpr("x", core::DataType::Int64),
                                      exec::MakeConst(static_cast<int64_t>(5)));
    auto batches = exec::Execute(reader, exec::MakeFilter(exec::MakeScan(), std::move(condition)));

    EXPECT_EQ(TotalRows(batches), 2);
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

TEST(FilterOperator, FusedAndInAndContains) {
    auto traffic = exec::MakeColumnExpr("TraficSourceID", core::DataType::Int64);
    auto in_traffic = exec::MakeBinary(
        exec::BinaryFunction::Or,
        exec::MakeBinary(exec::BinaryFunction::Equal, traffic, exec::MakeConst(int64_t{-1})),
        exec::MakeBinary(exec::BinaryFunction::Equal, traffic, exec::MakeConst(int64_t{6})));
    auto condition = exec::MakeBinary(
        exec::BinaryFunction::And,
        exec::MakeBinary(
            exec::BinaryFunction::And,
            exec::MakeBinary(exec::BinaryFunction::Equal,
                             exec::MakeColumnExpr("CounterID", core::DataType::Int64),
                             exec::MakeConst(int64_t{62})),
            std::move(in_traffic)),
        exec::MakeContains(exec::MakeColumnExpr("URL", core::DataType::String), "google"));
    auto plan = exec::MakeProject(
        exec::MakeFilter(exec::MakeScan(), std::move(condition)),
        {exec::ProjectionUnit{exec::MakeColumnExpr("URL", core::DataType::String), "URL"}});
    auto result = RunPlan(plan);
    ASSERT_EQ(result.RowsCount(), 1);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "google.com");
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
    auto buf = MakeClickBenchMiniFile();
    EXPECT_THROW(exec::ExecuteClickBenchQuery(AsBytes(buf), 99), std::runtime_error);
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

TEST(GlobalAggregation, FilteredReductionsUseSelection) {
    auto width = exec::MakeColumnExpr("ResolutionWidth", core::DataType::Int64);
    auto filter = exec::MakeFilter(
        exec::MakeScan(),
        exec::MakeBinary(exec::BinaryFunction::Equal,
                         exec::MakeColumnExpr("CounterID", core::DataType::Int64),
                         exec::MakeConst(static_cast<int64_t>(62))));
    auto plan = exec::MakeGlobalAggregation(
        std::move(filter), {exec::Sum(width, "sum_width"), exec::Count("c"),
                            exec::Min(width, "min_width"), exec::Max(width, "max_width")});
    auto result = RunPlan(plan);
    ASSERT_EQ(result.ColumnsCount(), 4);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "300");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "2");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(0), "100");
    EXPECT_EQ(result.ColumnAt(3).GetAsString(0), "200");
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

TEST(HashAggregation, GroupByDictionaryStringKey) {
    core::Schema schema({core::Field("URL", core::DataType::String)});
    core::Batch batch(schema);
    auto& col = batch.ColumnAt(0);
    std::vector<std::string> values = {
        "https://example.com/repeated/path",
        "https://google.com/repeated/path",
        "https://yandex.ru/repeated/path"};
    for (size_t i = 0; i < 300; ++i) {
        col.AppendFromString(values[i % values.size()]);
    }

    auto plan = exec::MakeHashAggregation(
        exec::MakeScan(), exec::MakeColumnExpr("URL", core::DataType::String),
        "URL", {exec::Count("count")});
    auto result = RunPlanOnBatch(batch, plan);
    ASSERT_EQ(result.RowsCount(), 3);
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        EXPECT_EQ(result.ColumnAt(1).GetAsString(row), "100");
    }
}

TEST(HashAggregation, GroupByBoolKey) {
    core::Schema schema(
        {core::Field("flag", core::DataType::Bool), core::Field("value", core::DataType::Int64)});
    core::Batch batch(schema);
    batch.ColumnAt(0).AppendFromString("true");
    batch.ColumnAt(1).AppendFromString("10");
    batch.ColumnAt(0).AppendFromString("false");
    batch.ColumnAt(1).AppendFromString("20");
    batch.ColumnAt(0).AppendFromString("true");
    batch.ColumnAt(1).AppendFromString("30");

    auto plan = exec::MakeHashAggregation(
        exec::MakeScan(), exec::MakeColumnExpr("flag", core::DataType::Bool), "flag",
        {exec::Count("count"),
         exec::Sum(exec::MakeColumnExpr("value", core::DataType::Int64), "sum")});
    auto result = RunPlanOnBatch(batch, plan);
    ASSERT_EQ(result.RowsCount(), 2);
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        auto key = result.ColumnAt(0).GetAsString(row);
        if (key == "1") {
            EXPECT_EQ(result.ColumnAt(1).GetAsString(row), "2");
            EXPECT_EQ(result.ColumnAt(2).GetAsString(row), "40");
        } else if (key == "0") {
            EXPECT_EQ(result.ColumnAt(1).GetAsString(row), "1");
            EXPECT_EQ(result.ColumnAt(2).GetAsString(row), "20");
        } else {
            FAIL() << "unexpected key: " << key;
        }
    }
}

TEST(HashAggregation, EmitsGroupsInFirstSeenOrder) {
    core::Schema schema(
        {core::Field("key", core::DataType::Int64), core::Field("value", core::DataType::Int64)});
    core::Batch batch(schema);
    for (auto [key, value] : std::vector<std::pair<int64_t, int64_t>>{
             {3, 10}, {1, 20}, {2, 30}, {3, 40}}) {
        batch.ColumnAt(0).AppendFromString(std::to_string(key));
        batch.ColumnAt(1).AppendFromString(std::to_string(value));
    }

    auto plan = exec::MakeHashAggregation(
        exec::MakeScan(), exec::MakeColumnExpr("key", core::DataType::Int64), "key",
        {exec::Count("count"),
         exec::Sum(exec::MakeColumnExpr("value", core::DataType::Int64), "sum")});
    auto result = RunPlanOnBatch(batch, plan);
    ASSERT_EQ(result.RowsCount(), 3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "3");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "2");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(0), "50");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "1");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(1), "1");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(1), "20");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "2");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(2), "1");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(2), "30");
}

TEST(HashAggregation, SumDoubleUsesWideAccumulator) {
    core::Schema schema(
        {core::Field("key", core::DataType::Int64), core::Field("value", core::DataType::Double)});
    core::Batch batch(schema);
    for (std::string_view value : {"10000000000000000", "1", "-10000000000000000"}) {
        batch.ColumnAt(0).AppendFromString("1");
        batch.ColumnAt(1).AppendFromString(value);
    }

    auto plan = exec::MakeHashAggregation(
        exec::MakeScan(), exec::MakeColumnExpr("key", core::DataType::Int64), "key",
        {exec::Sum(exec::MakeColumnExpr("value", core::DataType::Double), "sum")});
    auto result = RunPlanOnBatch(batch, plan);
    ASSERT_EQ(result.RowsCount(), 1);
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "1.000000");
}

TEST(HashAggregation, CompositeKeyMergesDuplicateGroups) {
    struct Row {
        int64_t user;
        std::string_view phrase;
        int64_t value;
    };

    core::Schema schema({core::Field("user", core::DataType::Int64),
                         core::Field("phrase", core::DataType::String),
                         core::Field("value", core::DataType::Int64)});
    core::Batch batch(schema);
    for (auto row : std::vector<Row>{{1, "alpha", 10}, {2, "beta", 5}, {1, "alpha", 7}}) {
        batch.ColumnAt(0).AppendFromString(std::to_string(row.user));
        batch.ColumnAt(1).AppendFromString(std::string(row.phrase));
        batch.ColumnAt(2).AppendFromString(std::to_string(row.value));
    }

    auto plan = exec::MakeHashAggregation(
        exec::MakeScan(),
        {exec::ProjectionUnit{exec::MakeColumnExpr("user", core::DataType::Int64), "user"},
         exec::ProjectionUnit{exec::MakeColumnExpr("phrase", core::DataType::String), "phrase"}},
        {exec::Count("count"), exec::Sum(exec::MakeColumnExpr("value", core::DataType::Int64),
                                         "sum")});
    auto result = RunPlanOnBatch(batch, plan);
    ASSERT_EQ(result.RowsCount(), 2);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "1");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "alpha");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(0), "2");
    EXPECT_EQ(result.ColumnAt(3).GetAsString(0), "17");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "2");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(1), "beta");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(1), "1");
    EXPECT_EQ(result.ColumnAt(3).GetAsString(1), "5");
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

TEST(TopNOperator, TiesKeepInputOrder) {
    core::Schema schema(
        {core::Field("id", core::DataType::Int64), core::Field("score", core::DataType::Int64)});
    core::Batch batch(schema);
    for (auto [id, score] : std::vector<std::pair<int64_t, int64_t>>{{10, 5}, {11, 5}, {12, 5}}) {
        batch.ColumnAt(0).AppendFromString(std::to_string(id));
        batch.ColumnAt(1).AppendFromString(std::to_string(score));
    }

    auto id_expr = exec::MakeColumnExpr("id", core::DataType::Int64);
    auto plan = exec::MakeTopN(
        exec::MakeScan(),
        {exec::SortUnit{exec::MakeBinary(exec::BinaryFunction::Minus, id_expr, id_expr), false}},
        2);
    auto result = RunPlanOnBatch(batch, plan);
    ASSERT_EQ(result.RowsCount(), 2);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "10");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "11");
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
    auto buf = MakeClickBenchMiniFile();
    auto batches = exec::ExecuteClickBenchQuery(AsBytes(buf), 19);
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
    ASSERT_EQ(result.ColumnsCount(), 2);
    ASSERT_EQ(result.RowsCount(), 3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "beta");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "2013-07-01 09:00:00");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "alpha");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(1), "2013-07-02 10:00:00");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "alpha");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(2), "2013-07-03 08:00:00");
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
    ASSERT_EQ(result.ColumnsCount(), 2);
    ASSERT_EQ(result.RowsCount(), 3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "beta");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "2013-07-01 09:00:00");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "alpha");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(1), "2013-07-02 10:00:00");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "alpha");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(2), "2013-07-03 08:00:00");
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

TEST(HashAggregation, GroupByTwoKeys) {
    auto plan = exec::MakeHashAggregation(
        exec::MakeScan(),
        {exec::ProjectionUnit{exec::MakeColumnExpr("UserID", core::DataType::Int64), "UserID"},
         exec::ProjectionUnit{exec::MakeColumnExpr("SearchPhrase", core::DataType::String),
                              "SearchPhrase"}},
        {exec::Count("count"),
         exec::Sum(exec::MakeColumnExpr("ResolutionWidth", core::DataType::Int64), "sum")});
    auto result = RunPlan(plan);
    ASSERT_EQ(result.ColumnsCount(), 4);
    ASSERT_EQ(result.RowsCount(), 3);
    std::set<std::string> seen;
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        auto user = result.ColumnAt(0).GetAsString(row);
        auto phrase = result.ColumnAt(1).GetAsString(row);
        auto count = result.ColumnAt(2).GetAsString(row);
        auto sum = result.ColumnAt(3).GetAsString(row);
        seen.insert(user + "/" + phrase);
        EXPECT_EQ(count, "1");
        if (user == "10" && phrase == "alpha") {
            EXPECT_EQ(sum, "100");
        } else if (user == "10" && phrase == "beta") {
            EXPECT_EQ(sum, "200");
        } else if (user == "20" && phrase == "alpha") {
            EXPECT_EQ(sum, "300");
        } else {
            FAIL() << "unexpected key: " << user << "/" << phrase;
        }
    }
    EXPECT_EQ(seen, (std::set<std::string>{"10/alpha", "10/beta", "20/alpha"}));
}

TEST(ClickBenchQueries, Q11DistinctUserPerPhoneAndModel) {
    auto result = RunMiniQuery(11);
    ASSERT_EQ(result.ColumnsCount(), 3);
    ASSERT_EQ(result.RowsCount(), 3);
    std::set<std::string> seen;
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        seen.insert(result.ColumnAt(0).GetAsString(row) + "/" +
                    result.ColumnAt(1).GetAsString(row));
        EXPECT_EQ(result.ColumnAt(2).GetAsString(row), "1");
    }
    EXPECT_EQ(seen, (std::set<std::string>{"1/iPhone", "1/Pixel", "2/iPhone"}));
}

TEST(ClickBenchQueries, Q14CountPerEngineAndPhrase) {
    auto result = RunMiniQuery(14);
    ASSERT_EQ(result.RowsCount(), 2);
    ASSERT_EQ(result.ColumnsCount(), 3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "2");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "alpha");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(0), "2");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(1), "1");
}

TEST(ClickBenchQueries, Q16CountPerUserAndPhrase) {
    auto result = RunMiniQuery(16);
    ASSERT_EQ(result.RowsCount(), 3);
    ASSERT_EQ(result.ColumnsCount(), 3);
    std::set<std::string> seen;
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        seen.insert(result.ColumnAt(0).GetAsString(row) + "/" +
                    result.ColumnAt(1).GetAsString(row));
        EXPECT_EQ(result.ColumnAt(2).GetAsString(row), "1");
    }
    EXPECT_EQ(seen, (std::set<std::string>{"10/alpha", "10/beta", "20/alpha"}));
}

TEST(ClickBenchQueries, Q17CountPerUserAndPhraseLimit) {
    auto result = RunMiniQuery(17);
    ASSERT_EQ(result.RowsCount(), 3);
    ASSERT_EQ(result.ColumnsCount(), 3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "10");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "alpha");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "10");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(1), "beta");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "20");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(2), "alpha");
}

TEST(ClickBenchQueries, Q29SumResolutionWidthShifted) {
    auto result = RunMiniQuery(29);
    ASSERT_EQ(result.ColumnsCount(), 90);
    ASSERT_EQ(result.RowsCount(), 1);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "600");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "603");
    EXPECT_EQ(result.ColumnAt(89).GetAsString(0), "867");
}

TEST(ClickBenchQueries, Q30CountPerEngineAndClientIp) {
    auto result = RunMiniQuery(30);
    ASSERT_EQ(result.ColumnsCount(), 5);
    ASSERT_EQ(result.RowsCount(), 3);
    std::set<std::string> seen;
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        auto engine = result.ColumnAt(0).GetAsString(row);
        auto client_ip = result.ColumnAt(1).GetAsString(row);
        seen.insert(engine + "/" + client_ip);
        EXPECT_EQ(result.ColumnAt(2).GetAsString(row), "1");
        EXPECT_EQ(result.ColumnAt(3).GetAsString(row), "0");
        if (engine == "2" && client_ip == "1000") {
            EXPECT_EQ(result.ColumnAt(4).GetAsString(row), "100");
        } else if (engine == "3" && client_ip == "1000") {
            EXPECT_EQ(result.ColumnAt(4).GetAsString(row), "200");
        } else if (engine == "2" && client_ip == "2000") {
            EXPECT_EQ(result.ColumnAt(4).GetAsString(row), "300");
        } else {
            FAIL() << "unexpected key: " << engine << "/" << client_ip;
        }
    }
    EXPECT_EQ(seen, (std::set<std::string>{"2/1000", "3/1000", "2/2000"}));
}

TEST(ClickBenchQueries, Q31CountPerWatchIdAndClientIpWithPhrase) {
    auto result = RunMiniQuery(31);
    ASSERT_EQ(result.ColumnsCount(), 5);
    ASSERT_EQ(result.RowsCount(), 3);
    std::set<std::string> seen;
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        auto watch_id = result.ColumnAt(0).GetAsString(row);
        auto client_ip = result.ColumnAt(1).GetAsString(row);
        seen.insert(watch_id + "/" + client_ip);
        EXPECT_EQ(result.ColumnAt(2).GetAsString(row), "1");
        EXPECT_EQ(result.ColumnAt(3).GetAsString(row), "0");
    }
    EXPECT_EQ(seen, (std::set<std::string>{"7/1000", "8/1000", "8/2000"}));
}

TEST(ClickBenchQueries, Q32CountPerWatchIdAndClientIp) {
    auto result = RunMiniQuery(32);
    ASSERT_EQ(result.ColumnsCount(), 5);
    ASSERT_EQ(result.RowsCount(), 3);
    std::set<std::string> seen;
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        seen.insert(result.ColumnAt(0).GetAsString(row) + "/" +
                    result.ColumnAt(1).GetAsString(row));
        EXPECT_EQ(result.ColumnAt(2).GetAsString(row), "1");
    }
    EXPECT_EQ(seen, (std::set<std::string>{"7/1000", "8/1000", "8/2000"}));
}

TEST(ClickBenchQueries, Q34ConstKeyGroupByUrl) {
    auto result = RunMiniQuery(34);
    ASSERT_EQ(result.ColumnsCount(), 3);
    ASSERT_EQ(result.RowsCount(), 3);
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        EXPECT_EQ(result.ColumnAt(0).GetAsString(row), "1");
        EXPECT_EQ(result.ColumnAt(2).GetAsString(row), "1");
    }
}

TEST(ClickBenchQueries, Q35GroupByClientIpArithmetic) {
    auto result = RunMiniQuery(35);
    ASSERT_EQ(result.RowsCount(), 2);
    ASSERT_EQ(result.ColumnsCount(), 5);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "1000");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(0), "999");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(0), "998");
    EXPECT_EQ(result.ColumnAt(3).GetAsString(0), "997");
    EXPECT_EQ(result.ColumnAt(4).GetAsString(0), "2");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "2000");
    EXPECT_EQ(result.ColumnAt(1).GetAsString(1), "1999");
    EXPECT_EQ(result.ColumnAt(4).GetAsString(1), "1");
}

TEST(TopNOperator, OffsetWithLimit) {
    auto plan = exec::MakeTopN(
        exec::MakeScan(),
        {exec::SortUnit{exec::MakeColumnExpr("ResolutionWidth", core::DataType::Int64), true}},
        /*limit=*/1, /*offset=*/1);
    auto result = RunPlan(plan);
    ASSERT_EQ(result.RowsCount(), 1);
    auto idx = result.GetSchema().GetIndex("ResolutionWidth");
    EXPECT_EQ(result.ColumnAt(idx).GetAsString(0), "200");
}

TEST(TopNOperator, OffsetWithoutLimit) {
    auto plan = exec::MakeTopN(
        exec::MakeScan(),
        {exec::SortUnit{exec::MakeColumnExpr("ResolutionWidth", core::DataType::Int64), true}},
        std::nullopt, /*offset=*/2);
    auto result = RunPlan(plan);
    ASSERT_EQ(result.RowsCount(), 1);
    auto idx = result.GetSchema().GetIndex("ResolutionWidth");
    EXPECT_EQ(result.ColumnAt(idx).GetAsString(0), "300");
}

TEST(TopNOperator, OffsetBeyondEndIsEmpty) {
    auto plan = exec::MakeTopN(
        exec::MakeScan(),
        {exec::SortUnit{exec::MakeColumnExpr("ResolutionWidth", core::DataType::Int64), true}},
        /*limit=*/2, /*offset=*/5);
    auto result = RunPlan(plan);
    EXPECT_EQ(result.RowsCount(), 0);
}

TEST(Expression, StrLength) {
    auto plan = exec::MakeProject(
        exec::MakeScan(),
        {exec::ProjectionUnit{
            exec::MakeFunction(exec::ScalarFunction::Length,
                               exec::MakeColumnExpr("URL", core::DataType::String)),
            "len"}});
    auto result = RunPlan(plan);
    ASSERT_EQ(result.RowsCount(), 3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "11");  // example.com
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "10");  // google.com
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "23");  // https://google.org/path
}

TEST(Expression, CaseWhen) {
    auto plan = exec::MakeProject(
        exec::MakeScan(),
        {exec::ProjectionUnit{
            exec::MakeCase(exec::MakeBinary(exec::BinaryFunction::Equal,
                                            exec::MakeColumnExpr("UserID", core::DataType::Int64),
                                            exec::MakeConst(static_cast<int64_t>(10))),
                           exec::MakeColumnExpr("URL", core::DataType::String),
                           exec::MakeConst(std::string("other"))),
            "src"}});
    auto result = RunPlan(plan);
    ASSERT_EQ(result.RowsCount(), 3);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "example.com");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "google.com");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "other");
}

TEST(Expression, RegexReplaceExtractsHost) {
    core::Schema schema({core::Field("URL", core::DataType::String)});
    core::Batch batch(schema);
    batch.ColumnAt(0).AppendFromString("example.com");
    batch.ColumnAt(0).AppendFromString("https://www.google.org/path");
    batch.ColumnAt(0).AppendFromString("http://state=19945206/a\nb");
    batch.ColumnAt(0).AppendFromString("http://state=19945206/a\rb");

    auto plan = exec::MakeProject(
        exec::MakeScan(),
        {exec::ProjectionUnit{
            exec::MakeRegexReplace(exec::MakeColumnExpr("URL", core::DataType::String),
                                   R"(^https?://(?:www\.)?([^/]+)/.*$)", R"(\1)"),
            "host"}});
    auto result = RunPlanOnBatch(batch, plan);
    ASSERT_EQ(result.RowsCount(), 4);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "example.com");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "google.org");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "http://state=19945206/a\nb");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(3), "state=19945206");
}

TEST(Expression, PrefixCaptureHandlesGenericPrefixes) {
    core::Schema schema({core::Field("Value", core::DataType::String)});
    core::Batch batch(schema);
    batch.ColumnAt(0).AppendFromString("prefix-alpha|tail");
    batch.ColumnAt(0).AppendFromString("prefix-v2-beta|tail");
    batch.ColumnAt(0).AppendFromString("prefix-|tail");
    batch.ColumnAt(0).AppendFromString("prefix-alpha|tail\nmore");
    batch.ColumnAt(0).AppendFromString("other-alpha|tail");

    auto plan = exec::MakeProject(
        exec::MakeScan(),
        {exec::ProjectionUnit{
            exec::MakePrefixCapture(exec::MakeColumnExpr("Value", core::DataType::String),
                                    {"prefix-v2-", "prefix-"}, '|'),
            "extracted"}});
    auto result = RunPlanOnBatch(batch, plan);
    ASSERT_EQ(result.RowsCount(), 5);
    EXPECT_EQ(result.ColumnAt(0).GetAsString(0), "alpha");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(1), "beta");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(2), "prefix-|tail");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(3), "prefix-alpha|tail\nmore");
    EXPECT_EQ(result.ColumnAt(0).GetAsString(4), "other-alpha|tail");
}

TEST(Kernel, TimestampAndLengthFunctions) {
    core::TimestampColumn ts(false);
    ts.AppendFromString("2013-07-14 12:34:56");
    ts.AppendFromString("2013-07-14 00:00:05");
    auto minutes = exec::kernel::ExtractMinute(ts);
    ASSERT_EQ(minutes->Size(), 2);
    EXPECT_EQ(minutes->GetAsString(0), "34");
    EXPECT_EQ(minutes->GetAsString(1), "0");
    auto truncated = exec::kernel::TruncMinute(ts);
    EXPECT_EQ(truncated->GetAsString(0), "2013-07-14 12:34:00");
    EXPECT_EQ(truncated->GetAsString(1), "2013-07-14 00:00:00");

    core::StringColumn s(false);
    s.AppendFromString("abc");
    s.AppendFromString("");
    auto lengths = exec::kernel::StrLength(s);
    ASSERT_EQ(lengths->Size(), 2);
    EXPECT_EQ(lengths->GetAsString(0), "3");
    EXPECT_EQ(lengths->GetAsString(1), "0");
}

TEST(Kernel, DictionaryStringKernelsReuseDictionary) {
    auto dict = MakeDictionaryStringColumn(
        {"https://www.google.com/path", "http://example.org/a", "plain"},
        {0, 1, 0, 2});

    auto contains = exec::kernel::StrContains(*dict, "google", false);
    ASSERT_EQ(contains->Size(), 4);
    EXPECT_EQ(contains->GetAsString(0), "true");
    EXPECT_EQ(contains->GetAsString(1), "false");
    EXPECT_EQ(contains->GetAsString(2), "true");
    EXPECT_EQ(contains->GetAsString(3), "false");

    auto lengths = exec::kernel::StrLength(*dict);
    EXPECT_EQ(lengths->GetAsString(0), "27");
    EXPECT_EQ(lengths->GetAsString(2), "27");

    auto captured = exec::kernel::PrefixCapture(*dict, {"https://", "http://"}, '/');
    ASSERT_NE(dynamic_cast<core::DictionaryStringColumn*>(captured.get()), nullptr);
    EXPECT_EQ(captured->GetAsString(0), "www.google.com");
    EXPECT_EQ(captured->GetAsString(1), "example.org");
    EXPECT_EQ(captured->GetAsString(2), "www.google.com");
    EXPECT_EQ(captured->GetAsString(3), "plain");
}

TEST(ClickBenchQueries, Q18ExtractMinutePerUserAndPhrase) {
    auto result = RunMiniQuery(18);
    ASSERT_EQ(result.ColumnsCount(), 4);
    ASSERT_EQ(result.RowsCount(), 3);
    for (size_t row = 0; row < result.RowsCount(); ++row) {
        EXPECT_EQ(result.ColumnAt(1).GetAsString(row), "0");  // all fixture timestamps are :00
        EXPECT_EQ(result.ColumnAt(3).GetAsString(row), "1");
    }
}

TEST(ClickBenchQueries, Q23WatchIdEventTimeUrlTitle) {
    auto result = RunMiniQuery(23);
    ASSERT_EQ(result.RowsCount(), 2);
    ASSERT_EQ(result.ColumnsCount(), 4);
    EXPECT_EQ(result.GetSchema().GetFields()[0].name, "WatchID");
    EXPECT_EQ(result.GetSchema().GetFields()[1].name, "EventTime");
    EXPECT_EQ(result.GetSchema().GetFields()[2].name, "URL");
    EXPECT_EQ(result.GetSchema().GetFields()[3].name, "Title");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(0), "google.com");
    EXPECT_EQ(result.ColumnAt(2).GetAsString(1), "https://google.org/path");
}

TEST(ClickBenchQueries, HavingAndDateQueriesRunOnMiniData) {
    for (size_t query : {27, 28, 40, 41, 42}) {
        auto buf = MakeClickBenchMiniFile();
        EXPECT_EQ(TotalRows(exec::ExecuteClickBenchQuery(AsBytes(buf), query)), 0u)
            << "query " << query;
    }
    {
        auto buf = MakeClickBenchMiniFile();
        EXPECT_EQ(TotalRows(exec::ExecuteClickBenchQuery(AsBytes(buf), 38)), 1u);
    }
    {
        auto buf = MakeClickBenchMiniFile();
        EXPECT_EQ(TotalRows(exec::ExecuteClickBenchQuery(AsBytes(buf), 39)), 2u);
    }
}
