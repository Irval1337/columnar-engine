#include <exec/clickbench.h>

#include <bruh/bruh_batch_reader.h>
#include <util/macro.h>

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
    return MakeGlobalAggregation(
        MakeScan(), {Distinct(ColumnRef(table_schema_, "SearchPhrase"), "distinct")});
}

std::shared_ptr<Operator> QueryMaker::MakeQ6() const {
    // SELECT MIN(EventDate), MAX(EventDate) FROM hits;
    return MakeGlobalAggregation(MakeScan(),
                                 {Min(ColumnRef(table_schema_, "EventDate"), "min"),
                                  Max(ColumnRef(table_schema_, "EventDate"), "max")});
}

std::vector<core::Batch> ExecuteClickBenchQuery(std::istream& is, size_t query_id) {
    bruh::BruhBatchReader reader(is);
    auto plan = QueryMaker(reader.GetSchema()).Make(query_id);
    return Execute(reader, plan);
}
}  // namespace columnar::exec
