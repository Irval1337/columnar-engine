#include <exec/clickbench.h>

#include <bruh/bruh_batch_reader.h>
#include <util/date_time.h>
#include <util/macro.h>

#include <string_view>
#include <utility>

namespace columnar::exec {
namespace {
std::shared_ptr<Expression> ColumnRef(const core::Schema& schema, std::string name) {
    auto* field = schema.FindField(name);
    if (field == nullptr) {
        THROW_RUNTIME_ERROR("Unknown field: " + name);
    }
    return MakeColumnExpr(std::move(name), field->type);
}

std::shared_ptr<Expression> And(std::shared_ptr<Expression> lhs, std::shared_ptr<Expression> rhs) {
    return MakeBinary(BinaryFunction::And, std::move(lhs), std::move(rhs));
}

std::shared_ptr<Expression> NotEmpty(const core::Schema& schema, std::string name) {
    auto column = ColumnRef(schema, std::move(name));
    return MakeBinary(BinaryFunction::NotEqual, std::move(column), MakeConst(std::string()));
}

std::shared_ptr<Expression> DateConst(std::string_view date) {
    return MakeConst(static_cast<int64_t>(util::ParseDate(date)));
}
}  // namespace

std::shared_ptr<Operator> QueryMaker::Make(size_t query_id) const {
    switch (query_id) {
        case 0:
            return MakeQ0();
        case 1:
            return MakeQ1();
        case 2:
            return MakeQ2();
        case 3:
            return MakeQ3();
        case 4:
            return MakeQ4();
        case 5:
            return MakeQ5();
        case 6:
            return MakeQ6();
        case 7:
            return MakeQ7();
        case 8:
            return MakeQ8();
        case 9:
            return MakeQ9();
        case 10:
            return MakeQ10();
        case 12:
            return MakeQ12();
        case 13:
            return MakeQ13();
        case 15:
            return MakeQ15();
        case 19:
            return MakeQ19();
        case 20:
            return MakeQ20();
        case 21:
            return MakeQ21();
        case 22:
            return MakeQ22();
        case 24:
            return MakeQ24();
        case 25:
            return MakeQ25();
        case 26:
            return MakeQ26();
        case 33:
            return MakeQ33();
        case 36:
            return MakeQ36();
        case 37:
            return MakeQ37();
        default:
            THROW_RUNTIME_ERROR("Unsupported ClickBench query");
    }
}

std::shared_ptr<Operator> QueryMaker::MakeQ0() const {
    // SELECT COUNT(*) FROM hits;
    return MakeCountTable("count");
}

std::shared_ptr<Operator> QueryMaker::MakeQ1() const {
    // SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;
    return MakeGlobalAggregation(
        MakeFilter(MakeScan(),
                   MakeBinary(BinaryFunction::NotEqual, ColumnRef(table_schema_, "AdvEngineID"),
                              MakeConst(static_cast<int64_t>(0)))),
        {Count("count")});
}

std::shared_ptr<Operator> QueryMaker::MakeQ2() const {
    // SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;
    return MakeGlobalAggregation(
        MakeScan(), {Sum(ColumnRef(table_schema_, "AdvEngineID"), "sum"), Count("count"),
                     Avg(ColumnRef(table_schema_, "ResolutionWidth"), "avg")});
}

std::shared_ptr<Operator> QueryMaker::MakeQ3() const {
    // SELECT AVG(UserID) FROM hits;
    return MakeGlobalAggregation(MakeScan(), {Avg(ColumnRef(table_schema_, "UserID"), "avg")});
}

std::shared_ptr<Operator> QueryMaker::MakeQ4() const {
    // SELECT COUNT(DISTINCT UserID) FROM hits;
    return MakeGlobalAggregation(MakeScan(),
                                 {Distinct(ColumnRef(table_schema_, "UserID"), "distinct")});
}

std::shared_ptr<Operator> QueryMaker::MakeQ5() const {
    // SELECT COUNT(DISTINCT SearchPhrase) FROM hits;
    return MakeGlobalAggregation(MakeScan(),
                                 {Distinct(ColumnRef(table_schema_, "SearchPhrase"), "distinct")});
}

std::shared_ptr<Operator> QueryMaker::MakeQ6() const {
    // SELECT MIN(EventDate), MAX(EventDate) FROM hits;
    return MakeGlobalAggregation(MakeScan(), {Min(ColumnRef(table_schema_, "EventDate"), "min"),
                                              Max(ColumnRef(table_schema_, "EventDate"), "max")});
}

std::shared_ptr<Operator> QueryMaker::MakeQ7() const {
    // SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0
    // GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;
    auto agg = MakeHashAggregation(
        MakeFilter(MakeScan(),
                   MakeBinary(BinaryFunction::NotEqual, ColumnRef(table_schema_, "AdvEngineID"),
                              MakeConst(static_cast<int64_t>(0)))),
        ColumnRef(table_schema_, "AdvEngineID"), "AdvEngineID", {Count("count")});
    return MakeTopN(std::move(agg),
                    {SortUnit{MakeColumnExpr("count", core::DataType::Int64), false}});
}

std::shared_ptr<Operator> QueryMaker::MakeQ8() const {
    // SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits
    // GROUP BY RegionID ORDER BY u DESC LIMIT 10;
    auto agg = MakeHashAggregation(MakeScan(), ColumnRef(table_schema_, "RegionID"), "RegionID",
                                   {Distinct(ColumnRef(table_schema_, "UserID"), "u")});
    return MakeTopN(std::move(agg), {SortUnit{MakeColumnExpr("u", core::DataType::Int64), false}},
                    10);
}

std::shared_ptr<Operator> QueryMaker::MakeQ9() const {
    // SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth),
    // COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;
    auto agg = MakeHashAggregation(MakeScan(), ColumnRef(table_schema_, "RegionID"), "RegionID",
                                   {Sum(ColumnRef(table_schema_, "AdvEngineID"), "sum"), Count("c"),
                                    Avg(ColumnRef(table_schema_, "ResolutionWidth"), "avg"),
                                    Distinct(ColumnRef(table_schema_, "UserID"), "distinct")});
    return MakeTopN(std::move(agg), {SortUnit{MakeColumnExpr("c", core::DataType::Int64), false}},
                    10);
}

std::shared_ptr<Operator> QueryMaker::MakeQ10() const {
    // SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits
    // WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
    auto agg =
        MakeHashAggregation(MakeFilter(MakeScan(), NotEmpty(table_schema_, "MobilePhoneModel")),
                            ColumnRef(table_schema_, "MobilePhoneModel"), "MobilePhoneModel",
                            {Distinct(ColumnRef(table_schema_, "UserID"), "u")});
    return MakeTopN(std::move(agg), {SortUnit{MakeColumnExpr("u", core::DataType::Int64), false}},
                    10);
}

std::shared_ptr<Operator> QueryMaker::MakeQ12() const {
    // SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> ''
    // GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
    auto agg = MakeHashAggregation(
        MakeFilter(MakeScan(),
                   MakeBinary(BinaryFunction::NotEqual, ColumnRef(table_schema_, "SearchPhrase"),
                              MakeConst(std::string()))),
        ColumnRef(table_schema_, "SearchPhrase"), "SearchPhrase", {Count("c")});
    return MakeTopN(std::move(agg), {SortUnit{MakeColumnExpr("c", core::DataType::Int64), false}},
                    10);
}

std::shared_ptr<Operator> QueryMaker::MakeQ13() const {
    // SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> ''
    // GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
    auto agg = MakeHashAggregation(MakeFilter(MakeScan(), NotEmpty(table_schema_, "SearchPhrase")),
                                   ColumnRef(table_schema_, "SearchPhrase"), "SearchPhrase",
                                   {Distinct(ColumnRef(table_schema_, "UserID"), "u")});
    return MakeTopN(std::move(agg), {SortUnit{MakeColumnExpr("u", core::DataType::Int64), false}},
                    10);
}

std::shared_ptr<Operator> QueryMaker::MakeQ15() const {
    // SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;
    auto agg = MakeHashAggregation(MakeScan(), ColumnRef(table_schema_, "UserID"), "UserID",
                                   {Count("count")});
    return MakeTopN(std::move(agg),
                    {SortUnit{MakeColumnExpr("count", core::DataType::Int64), false}}, 10);
}

std::shared_ptr<Operator> QueryMaker::MakeQ19() const {
    // SELECT UserID FROM hits WHERE UserID = 435090932899640449;
    auto user_id = ColumnRef(table_schema_, "UserID");
    auto filter =
        MakeFilter(MakeScan(), MakeBinary(BinaryFunction::Equal, user_id,
                                          MakeConst(static_cast<int64_t>(435090932899640449LL))));
    return MakeProject(std::move(filter),
                       {ProjectionUnit{ColumnRef(table_schema_, "UserID"), "UserID"}});
}

std::shared_ptr<Operator> QueryMaker::MakeQ20() const {
    // SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';
    return MakeGlobalAggregation(
        MakeFilter(MakeScan(), MakeContains(ColumnRef(table_schema_, "URL"), "google")),
        {Count("count")});
}

std::shared_ptr<Operator> QueryMaker::MakeQ21() const {
    // SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits
    // WHERE URL LIKE '%google%' AND SearchPhrase <> ''
    // GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
    auto filter =
        MakeFilter(MakeScan(), And(MakeContains(ColumnRef(table_schema_, "URL"), "google"),
                                   NotEmpty(table_schema_, "SearchPhrase")));
    auto agg = MakeHashAggregation(std::move(filter), ColumnRef(table_schema_, "SearchPhrase"),
                                   "SearchPhrase",
                                   {Min(ColumnRef(table_schema_, "URL"), "min_url"), Count("c")});
    return MakeTopN(std::move(agg), {SortUnit{MakeColumnExpr("c", core::DataType::Int64), false}},
                    10);
}

std::shared_ptr<Operator> QueryMaker::MakeQ22() const {
    // SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID)
    // FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%'
    // AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
    auto condition = And(And(MakeContains(ColumnRef(table_schema_, "Title"), "Google"),
                             MakeContains(ColumnRef(table_schema_, "URL"), ".google.", true)),
                         NotEmpty(table_schema_, "SearchPhrase"));
    auto agg = MakeHashAggregation(MakeFilter(MakeScan(), std::move(condition)),
                                   ColumnRef(table_schema_, "SearchPhrase"), "SearchPhrase",
                                   {Min(ColumnRef(table_schema_, "URL"), "min_url"),
                                    Min(ColumnRef(table_schema_, "Title"), "min_title"), Count("c"),
                                    Distinct(ColumnRef(table_schema_, "UserID"), "distinct_u")});
    return MakeTopN(std::move(agg), {SortUnit{MakeColumnExpr("c", core::DataType::Int64), false}},
                    10);
}

std::shared_ptr<Operator> QueryMaker::MakeQ24() const {
    // SELECT SearchPhrase, toDateTime(EventTime) AS EventTime FROM hits
    // WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;
    auto top = MakeTopN(MakeFilter(MakeScan(), NotEmpty(table_schema_, "SearchPhrase")),
                        {SortUnit{ColumnRef(table_schema_, "EventTime"), true}}, 10);
    return MakeProject(std::move(top),
                       {ProjectionUnit{ColumnRef(table_schema_, "SearchPhrase"), "SearchPhrase"}});
}

std::shared_ptr<Operator> QueryMaker::MakeQ25() const {
    // SELECT SearchPhrase FROM hits WHERE SearchPhrase <> ''
    // ORDER BY SearchPhrase LIMIT 10;
    auto top = MakeTopN(MakeFilter(MakeScan(), NotEmpty(table_schema_, "SearchPhrase")),
                        {SortUnit{ColumnRef(table_schema_, "SearchPhrase"), true}}, 10);
    return MakeProject(std::move(top),
                       {ProjectionUnit{ColumnRef(table_schema_, "SearchPhrase"), "SearchPhrase"}});
}

std::shared_ptr<Operator> QueryMaker::MakeQ26() const {
    // SELECT SearchPhrase, toDateTime(EventTime) AS EventTime FROM hits
    // WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;
    auto top = MakeTopN(MakeFilter(MakeScan(), NotEmpty(table_schema_, "SearchPhrase")),
                        {SortUnit{ColumnRef(table_schema_, "EventTime"), true},
                         SortUnit{ColumnRef(table_schema_, "SearchPhrase"), true}},
                        10);
    return MakeProject(std::move(top),
                       {ProjectionUnit{ColumnRef(table_schema_, "SearchPhrase"), "SearchPhrase"}});
}

std::shared_ptr<Operator> QueryMaker::MakeQ33() const {
    // SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;
    auto agg =
        MakeHashAggregation(MakeScan(), ColumnRef(table_schema_, "URL"), "URL", {Count("c")});
    return MakeTopN(std::move(agg), {SortUnit{MakeColumnExpr("c", core::DataType::Int64), false}},
                    10);
}

std::shared_ptr<Operator> QueryMaker::MakeQ36() const {
    // SELECT URL, COUNT(*) AS PageViews FROM hits
    // WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31'
    // AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> ''
    // GROUP BY URL ORDER BY PageViews DESC LIMIT 10;
    auto condition = And(
        And(And(And(And(MakeBinary(BinaryFunction::Equal, ColumnRef(table_schema_, "CounterID"),
                                   MakeConst(static_cast<int64_t>(62))),
                        MakeBinary(BinaryFunction::GreaterOrEqual,
                                   ColumnRef(table_schema_, "EventDate"), DateConst("2013-07-01"))),
                    MakeBinary(BinaryFunction::LessOrEqual, ColumnRef(table_schema_, "EventDate"),
                               DateConst("2013-07-31"))),
                MakeBinary(BinaryFunction::Equal, ColumnRef(table_schema_, "DontCountHits"),
                           MakeConst(static_cast<int64_t>(0)))),
            MakeBinary(BinaryFunction::Equal, ColumnRef(table_schema_, "IsRefresh"),
                       MakeConst(static_cast<int64_t>(0)))),
        NotEmpty(table_schema_, "URL"));
    auto agg = MakeHashAggregation(MakeFilter(MakeScan(), std::move(condition)),
                                   ColumnRef(table_schema_, "URL"), "URL", {Count("PageViews")});
    return MakeTopN(std::move(agg),
                    {SortUnit{MakeColumnExpr("PageViews", core::DataType::Int64), false}}, 10);
}

std::shared_ptr<Operator> QueryMaker::MakeQ37() const {
    // SELECT Title, COUNT(*) AS PageViews FROM hits
    // WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31'
    // AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> ''
    // GROUP BY Title ORDER BY PageViews DESC LIMIT 10;
    auto condition = And(
        And(And(And(And(MakeBinary(BinaryFunction::Equal, ColumnRef(table_schema_, "CounterID"),
                                   MakeConst(static_cast<int64_t>(62))),
                        MakeBinary(BinaryFunction::GreaterOrEqual,
                                   ColumnRef(table_schema_, "EventDate"), DateConst("2013-07-01"))),
                    MakeBinary(BinaryFunction::LessOrEqual, ColumnRef(table_schema_, "EventDate"),
                               DateConst("2013-07-31"))),
                MakeBinary(BinaryFunction::Equal, ColumnRef(table_schema_, "DontCountHits"),
                           MakeConst(static_cast<int64_t>(0)))),
            MakeBinary(BinaryFunction::Equal, ColumnRef(table_schema_, "IsRefresh"),
                       MakeConst(static_cast<int64_t>(0)))),
        NotEmpty(table_schema_, "Title"));
    auto agg =
        MakeHashAggregation(MakeFilter(MakeScan(), std::move(condition)),
                            ColumnRef(table_schema_, "Title"), "Title", {Count("PageViews")});
    return MakeTopN(std::move(agg),
                    {SortUnit{MakeColumnExpr("PageViews", core::DataType::Int64), false}}, 10);
}

std::vector<core::Batch> ExecuteClickBenchQuery(std::istream& is, size_t query_id) {
    bruh::BruhBatchReader reader(is);
    auto plan = QueryMaker(reader.GetSchema()).Make(query_id);
    return Execute(reader, plan);
}
}  // namespace columnar::exec
