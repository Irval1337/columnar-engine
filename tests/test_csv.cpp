#include <gtest/gtest.h>
#include <csv/csv.h>
#include <core/schema.h>
#include <core/field.h>
#include <sstream>

using namespace columnar;  // NOLINT

// (@Irval1337) TODO: optimize this function and make it useful for streams
std::string NormalizeCSV(std::string_view s) {
    std::string result;
    for (char c : s) {
        if (c != '\n' && c != '\r' && c != '"') {
            result += c;
        }
    }
    return result;
}

TEST(CSVRowReader, Basic) {
    std::istringstream in("a,b,c\n1,2,3");
    csv::CSVRowReader reader(in);

    auto row = reader.ReadRow();
    ASSERT_TRUE(row);
    EXPECT_EQ(row->size(), 3);
    EXPECT_EQ((*row)[0], "a");
    EXPECT_EQ((*row)[2], "c");

    row = reader.ReadRow();
    ASSERT_TRUE(row);
    EXPECT_EQ((*row)[0], "1");

    EXPECT_FALSE(reader.ReadRow());
}

TEST(CSVRowReader, NewLineAtEnd) {
    std::istringstream in("a,b,c\n1,2,3\n");
    csv::CSVRowReader reader(in);

    auto row = reader.ReadRow();
    ASSERT_TRUE(row);
    EXPECT_EQ(row->size(), 3);
    EXPECT_EQ((*row)[0], "a");
    EXPECT_EQ((*row)[2], "c");

    row = reader.ReadRow();
    ASSERT_TRUE(row);
    EXPECT_EQ((*row)[0], "1");

    EXPECT_FALSE(reader.ReadRow());
}

TEST(CSVRowReader, Quoted) {
    std::istringstream in("\"hello world\",\"this is \"\"test\"\"\"");
    csv::CSVRowReader reader(in);

    auto row = reader.ReadRow();
    ASSERT_TRUE(row);
    EXPECT_EQ((*row)[0], "hello world");
    EXPECT_EQ((*row)[1], "this is \"test\"");
}

TEST(CSVRowReader, NewlineInQuotes) {
    std::istringstream in("\"1\n2\",aboba");
    csv::CSVRowReader reader(in);

    auto row = reader.ReadRow();
    ASSERT_TRUE(row);
    EXPECT_EQ((*row)[0], "1\n2");
    EXPECT_EQ((*row)[1], "aboba");
}

TEST(CSVRowReader, EmptyFields) {
    std::istringstream in(",bruhdb,");
    csv::CSVRowReader reader(in);

    auto row = reader.ReadRow();
    ASSERT_TRUE(row);
    EXPECT_EQ(row->size(), 3);
    EXPECT_EQ((*row)[0], "");
    EXPECT_EQ((*row)[1], "bruhdb");
    EXPECT_EQ((*row)[2], "");
}

TEST(CSVRowReader, CustomDelimiter) {
    std::istringstream in("a;b;c\n");
    csv::CSVRowReader reader(in, {.delimiter = ';'});

    auto row = reader.ReadRow();
    ASSERT_TRUE(row);
    EXPECT_EQ(row->size(), 3);
    EXPECT_EQ((*row)[1], "b");
}

TEST(SchemaReader, ReadWrite) {
    std::istringstream in("id,int64\nname,string,nullable\nmoney,double");
    auto schema = csv::SchemaManager::ReadFromStream(in);

    EXPECT_EQ(schema.FieldsCount(), 3);
    EXPECT_EQ(schema.GetFields()[0].name, "id");
    EXPECT_EQ(schema.GetFields()[0].type, core::DataType::Int64);
    EXPECT_FALSE(schema.GetFields()[0].nullable);
    EXPECT_EQ(schema.GetFields()[1].name, "name");
    EXPECT_TRUE(schema.GetFields()[1].nullable);
    EXPECT_EQ(schema.GetFields()[2].name, "money");
    EXPECT_EQ(schema.GetFields()[2].type, core::DataType::Double);
    EXPECT_FALSE(schema.GetFields()[2].nullable);

    std::ostringstream out;
    csv::SchemaManager::WriteToStream(out, schema);

    EXPECT_EQ(NormalizeCSV(out.str()),
              NormalizeCSV("id,int64\nname,string,nullable\nmoney,double"));
}

TEST(CSVBatchReader, ReadBatches) {
    core::Schema schema({core::Field("x", core::DataType::Int64),
                         core::Field("y", core::DataType::String),
                         core::Field("z", core::DataType::Double)});

    std::istringstream in("1,hello,0.1\n2,world,0.2\n3,test,0.3");
    csv::CSVBatchReader reader(in, schema, {.batch_rows_size = 2});

    auto b = reader.ReadNext();
    ASSERT_TRUE(b);
    EXPECT_EQ(b->ColumnAt(2).GetAsString(0), "0.100000");
    EXPECT_EQ(b->ColumnAt(2).GetAsString(1), "0.200000");
    EXPECT_EQ(b->RowsCount(), 2);

    b = reader.ReadNext();
    ASSERT_TRUE(b);
    EXPECT_EQ(b->RowsCount(), 1);
    EXPECT_EQ(b->ColumnAt(2).GetAsString(0), "0.300000");

    EXPECT_FALSE(reader.ReadNext());
}

TEST(CSVBatchReader, BoolColumn) {
    core::Schema schema(
        {core::Field("is_gay", core::DataType::Bool), core::Field("name", core::DataType::String)});
    std::istringstream in("true,first\nfalse,second\n1,third\n0,aboba");
    csv::CSVBatchReader reader(in, schema, {});

    auto batch = reader.ReadNext();
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->RowsCount(), 4);
    EXPECT_EQ(batch->ColumnAt(0).GetAsString(0), "true");
    EXPECT_EQ(batch->ColumnAt(0).GetAsString(1), "false");
    EXPECT_EQ(batch->ColumnAt(0).GetAsString(2), "true");
    EXPECT_EQ(batch->ColumnAt(0).GetAsString(3), "false");
}

TEST(CSVBatchWriter, WriteBatch) {
    core::Schema schema({core::Field("a", core::DataType::Int64),
                         core::Field("b", core::DataType::String),
                         core::Field("c", core::DataType::Bool)});
    core::Batch batch(schema);
    batch.ColumnAt(0).AppendFromString("42");
    batch.ColumnAt(1).AppendFromString("hello");
    batch.ColumnAt(2).AppendFromString("true");
    batch.ColumnAt(0).AppendFromString("1337");
    batch.ColumnAt(1).AppendFromString("world");
    batch.ColumnAt(2).AppendFromString("false");

    std::ostringstream out;
    csv::CSVBatchWriter writer(out, {});
    writer.Write(batch);
    writer.Flush();

    EXPECT_EQ(NormalizeCSV(out.str()), NormalizeCSV("42,hello,true\n1337,world,false\n"));
}

TEST(CSVBatchWriter, QuotesAndCommas) {
    core::Schema schema({core::Field("t", core::DataType::String)});

    core::Batch batch(schema);
    batch.ColumnAt(0).AppendFromString("king,,, arthur");
    batch.ColumnAt(0).AppendFromString("came \"a lot\"");

    std::ostringstream out;
    csv::CSVBatchWriter writer(out, {});
    writer.Write(batch);
    writer.Flush();

    EXPECT_EQ(out.str(), "\"king,,, arthur\"\n\"came \"\"a lot\"\"\"\n");
}

TEST(CSVFullInterface, DataIsNotChanged) {
    core::Schema schema(
        {core::Field("id", core::DataType::Int64), core::Field("name", core::DataType::String)});
    std::string str = "1,a\n2,b";

    std::istringstream in(str);
    csv::CSVBatchReader reader(in, schema, {});
    auto batch = reader.ReadNext();
    ASSERT_TRUE(batch);

    std::ostringstream out;
    csv::CSVBatchWriter writer(out, {});
    writer.Write(*batch);
    writer.Flush();

    EXPECT_EQ(NormalizeCSV(out.str()), NormalizeCSV(str));
}

TEST(SchemaReader, InvalidDataType) {
    std::istringstream in("a,kamzoner");
    EXPECT_THROW(csv::SchemaManager::ReadFromStream(in), std::runtime_error);
}

TEST(CSVBatchReader, InvalidNumber) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::istringstream in("hello?");
    csv::CSVBatchReader reader(in, schema, {});

    EXPECT_THROW(reader.ReadNext(), std::runtime_error);
}

TEST(CSVBatchReader, InvalidRowsSize) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::istringstream in("1\n2,3\n4");
    csv::CSVBatchReader reader(in, schema, {});

    EXPECT_THROW(reader.ReadNext(), std::runtime_error);
}

TEST(CSVBatchReader, SkipHeader) {
    core::Schema schema(
        {core::Field("x", core::DataType::Int64), core::Field("y", core::DataType::String)});
    std::istringstream in("x,y\n1,hello\n2,world");
    csv::CSVBatchReader reader(in, schema, {.has_header = true});

    auto batch = reader.ReadNext();
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->RowsCount(), 2);
    EXPECT_EQ(batch->ColumnAt(0).GetAsString(0), "1");
    EXPECT_EQ(batch->ColumnAt(0).GetAsString(1), "2");
    EXPECT_EQ(batch->ColumnAt(1).GetAsString(0), "hello");
    EXPECT_EQ(batch->ColumnAt(1).GetAsString(1), "world");
}

TEST(CSVBatchWriter, WriteHeader) {
    core::Schema schema(
        {core::Field("a", core::DataType::Int64), core::Field("b", core::DataType::String)});
    std::ostringstream out;
    csv::CSVBatchWriter writer(out, {.has_header = true});

    core::Batch b1(schema);
    b1.ColumnAt(0).AppendFromString("1");
    b1.ColumnAt(1).AppendFromString("x");
    writer.Write(b1);
    core::Batch b2(schema);
    b2.ColumnAt(0).AppendFromString("2");
    b2.ColumnAt(1).AppendFromString("y");
    writer.Write(b2);

    EXPECT_EQ(NormalizeCSV(out.str()), NormalizeCSV("a,b\n1,x\n2,y\n"));
}

TEST(CSVFullInterface, NullAndEmptyString) {
    core::Schema schema({core::Field("a", core::DataType::String, true),
                         core::Field("b", core::DataType::String, true)});
    std::istringstream in(",\"\"\n\"\",\n1,2");
    csv::CSVBatchReader reader(in, schema, {});

    auto batch = reader.ReadNext();
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->RowsCount(), 3);
    EXPECT_TRUE(batch->ColumnAt(0).IsNull(0));
    EXPECT_FALSE(batch->ColumnAt(1).IsNull(0));
    EXPECT_EQ(batch->ColumnAt(1).GetAsString(0), "");
    EXPECT_FALSE(batch->ColumnAt(0).IsNull(1));
    EXPECT_EQ(batch->ColumnAt(0).GetAsString(1), "");
    EXPECT_TRUE(batch->ColumnAt(1).IsNull(1));
    EXPECT_EQ(batch->ColumnAt(0).GetAsString(2), "1");
    EXPECT_EQ(batch->ColumnAt(1).GetAsString(2), "2");

    std::ostringstream out;
    csv::CSVBatchWriter writer(out, {});
    writer.Write(*batch);
    writer.Flush();

    EXPECT_EQ(out.str(), ",\"\"\n\"\",\n1,2\n");
}
