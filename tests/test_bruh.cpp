#include <gtest/gtest.h>
#include <bruh/bruh.h>
#include <csv/csv.h>
#include <core/schema.h>

#include <sstream>

using namespace columnar;  // NOLINT

TEST(BruhBatchWriter, WriteBasic) {
    core::Schema schema(
        {core::Field("id", core::DataType::Int64), core::Field("name", core::DataType::String)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    bruh::BruhBatchWriter writer(ss, schema);
    core::Batch batch(schema);
    batch.ColumnAt(0).AppendFromString("1");
    batch.ColumnAt(1).AppendFromString("aaa");
    batch.ColumnAt(0).AppendFromString("2");
    batch.ColumnAt(1).AppendFromString("bbbbb");
    writer.Write(batch);
    writer.Flush();

    EXPECT_GT(ss.str().size(), 0);
}

TEST(BruhBatchReader, ReadBasic) {
    core::Schema schema(
        {core::Field("id", core::DataType::Int64), core::Field("name", core::DataType::String)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        core::Batch batch(schema);
        batch.ColumnAt(0).AppendFromString("1");
        batch.ColumnAt(1).AppendFromString("aaa");
        batch.ColumnAt(0).AppendFromString("2");
        batch.ColumnAt(1).AppendFromString("bbbbb");
        writer.Write(batch);
        writer.Flush();
    }

    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    EXPECT_EQ(reader.NumRowGroups(), 1);

    auto& read_schema = reader.GetSchema();
    EXPECT_EQ(read_schema.FieldsCount(), 2);
    EXPECT_EQ(read_schema.GetFields()[0].name, "id");
    EXPECT_EQ(read_schema.GetFields()[1].name, "name");

    auto batch = reader.ReadRowGroup(0);
    EXPECT_EQ(batch.RowsCount(), 2);
    EXPECT_EQ(batch.ColumnAt(0).GetAsString(0), "1");
    EXPECT_EQ(batch.ColumnAt(0).GetAsString(1), "2");
    EXPECT_EQ(batch.ColumnAt(1).GetAsString(0), "aaa");
    EXPECT_EQ(batch.ColumnAt(1).GetAsString(1), "bbbbb");
}

TEST(BruhBatchReader, MultipleRowGroups) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        for (int group = 0; group < 3; ++group) {
            core::Batch batch(schema);
            batch.ColumnAt(0).AppendFromString(std::to_string(group * 10));
            batch.ColumnAt(0).AppendFromString(std::to_string(group * 10 + 1));
            writer.Write(batch);
        }
        writer.Flush();
    }

    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    EXPECT_EQ(reader.NumRowGroups(), 3);

    for (std::size_t group = 0; group < 3; ++group) {
        auto batch = reader.ReadRowGroup(group);
        EXPECT_EQ(batch.RowsCount(), 2);
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(0), std::to_string(group * 10));
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(1), std::to_string(group * 10 + 1));
    }
}

TEST(BruhBatchReader, ReadNext) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        for (int i = 0; i < 3; ++i) {
            core::Batch batch(schema);
            batch.ColumnAt(0).AppendFromString(std::to_string(i));
            writer.Write(batch);
        }
        writer.Flush();
    }

    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    int count = 0;
    while (auto batch = reader.ReadNext()) {
        EXPECT_EQ(batch->RowsCount(), 1);
        EXPECT_EQ(batch->ColumnAt(0).GetAsString(0), std::to_string(count));
        ++count;
    }
    EXPECT_EQ(count, 3);
}

TEST(BruhBatchReader, NullableColumn) {
    core::Schema schema({core::Field("val", core::DataType::String, true)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        core::Batch batch(schema);
        batch.ColumnAt(0).AppendFromString("hello");
        batch.ColumnAt(0).AppendNull();
        batch.ColumnAt(0).AppendFromString("world");
        writer.Write(batch);
        writer.Flush();
    }

    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    auto batch = reader.ReadRowGroup(0);

    EXPECT_EQ(batch.RowsCount(), 3);
    EXPECT_FALSE(batch.ColumnAt(0).IsNull(0));
    EXPECT_EQ(batch.ColumnAt(0).GetAsString(0), "hello");
    EXPECT_TRUE(batch.ColumnAt(0).IsNull(1));
    EXPECT_FALSE(batch.ColumnAt(0).IsNull(2));
    EXPECT_EQ(batch.ColumnAt(0).GetAsString(2), "world");
}

TEST(BruhBatchReader, AllDataTypes) {
    core::Schema schema(
        {core::Field("d", core::DataType::Int64), core::Field("i", core::DataType::Double),
         core::Field("c", core::DataType::Bool), core::Field("k", core::DataType::String)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        core::Batch batch(schema);
        batch.ColumnAt(0).AppendFromString("42");
        batch.ColumnAt(1).AppendFromString("3.14");
        batch.ColumnAt(2).AppendFromString("true");
        batch.ColumnAt(3).AppendFromString("test");
        writer.Write(batch);
        writer.Flush();
    }

    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    auto batch = reader.ReadRowGroup(0);

    EXPECT_EQ(batch.ColumnAt(0).GetAsString(0), "42");
    EXPECT_EQ(batch.ColumnAt(2).GetAsString(0), "true");
    EXPECT_EQ(batch.ColumnAt(3).GetAsString(0), "test");
}

TEST(BruhBatchWriter, EmptyBatch) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        core::Batch batch(schema);
        writer.Write(batch);
        writer.Flush();
    }

    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    auto batch = reader.ReadRowGroup(0);
    EXPECT_EQ(batch.RowsCount(), 0);
}

TEST(BruhBatchWriter, LargeBatch) {
    core::Schema schema(
        {core::Field("id", core::DataType::Int64), core::Field("data", core::DataType::String)});
    auto size = 10000;
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        core::Batch batch(schema);
        for (std::size_t i = 0; i < size; ++i) {
            batch.ColumnAt(0).AppendFromString(std::to_string(i));
            batch.ColumnAt(1).AppendFromString("data_" + std::to_string(i));
        }
        writer.Write(batch);
        writer.Flush();
    }

    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    auto batch = reader.ReadRowGroup(0);
    EXPECT_EQ(batch.RowsCount(), size);
    EXPECT_EQ(batch.ColumnAt(0).GetAsString(0), "0");
    EXPECT_EQ(batch.ColumnAt(0).GetAsString(size - 1), std::to_string(size - 1));
    EXPECT_EQ(batch.ColumnAt(1).GetAsString(size - 1), "data_" + std::to_string(size - 1));
}

TEST(BruhBatchWriter, SpecialCharacters) {
    core::Schema schema({core::Field("s", core::DataType::String)});
    std::vector<std::string> test_strings = {
        "", "hello world", "line1\nline2", "tab\t123", "quotes\"123", "comma,123", "watafa\t\n,"};

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        core::Batch batch(schema);
        for (const auto& s : test_strings) {
            batch.ColumnAt(0).AppendFromString(s);
        }
        writer.Write(batch);
        writer.Flush();
    }

    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    auto batch = reader.ReadRowGroup(0);
    EXPECT_EQ(batch.RowsCount(), test_strings.size());
    for (std::size_t i = 0; i < test_strings.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), test_strings[i]);
    }
}

TEST(BruhBatchReader, AllNullColumn) {
    core::Schema schema({core::Field("a", core::DataType::Int64, true)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        core::Batch batch(schema);
        for (int i = 0; i < 5; ++i) {
            batch.ColumnAt(0).AppendNull();
        }
        writer.Write(batch);
        writer.Flush();
    }

    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    auto batch = reader.ReadRowGroup(0);
    EXPECT_EQ(batch.RowsCount(), 5);
    for (std::size_t i = 0; i < 5; ++i) {
        EXPECT_TRUE(batch.ColumnAt(0).IsNull(i));
    }
}

TEST(BruhFullInterface, CsvAndBruh) {
    std::string csv_data = "1,pepe,true,314\n2,shneine,false,271\n3,fa,true,141\n";
    core::Schema schema(
        {core::Field("id", core::DataType::Int64), core::Field("name", core::DataType::String),
         core::Field("daun", core::DataType::Bool), core::Field("test", core::DataType::Int64)});
    std::stringstream bruh_ss(std::ios::in | std::ios::out | std::ios::binary);

    {
        std::istringstream csv_in(csv_data);
        csv::CSVBatchReader csv_reader(csv_in, schema, {});
        bruh::BruhBatchWriter bruh_writer(bruh_ss, schema);

        while (auto batch = csv_reader.ReadNext()) {
            bruh_writer.Write(*batch);
        }
        bruh_writer.Flush();
    }

    bruh_ss.seekg(0);
    std::ostringstream csv_out;
    {
        bruh::BruhBatchReader bruh_reader(bruh_ss);
        csv::CSVBatchWriter csv_writer(csv_out, {});

        while (auto batch = bruh_reader.ReadNext()) {
            csv_writer.Write(*batch);
        }
        csv_writer.Flush();
    }

    EXPECT_EQ(csv_out.str(), csv_data);
}

TEST(BruhFullInterface, NullCsvAndBruh) {
    std::string csv_data = "1,a\n2,\n3,c\n";
    core::Schema schema({core::Field("id", core::DataType::Int64),
                         core::Field("smth", core::DataType::String, true)});
    std::stringstream bruh_ss(std::ios::in | std::ios::out | std::ios::binary);

    {
        std::istringstream csv_in(csv_data);
        csv::CSVBatchReader csv_reader(csv_in, schema, {});
        bruh::BruhBatchWriter bruh_writer(bruh_ss, schema);

        while (auto batch = csv_reader.ReadNext()) {
            bruh_writer.Write(*batch);
        }
        bruh_writer.Flush();
    }

    bruh_ss.seekg(0);
    std::ostringstream csv_out;
    {
        bruh::BruhBatchReader bruh_reader(bruh_ss);
        csv::CSVBatchWriter csv_writer(csv_out, {});

        while (auto batch = bruh_reader.ReadNext()) {
            csv_writer.Write(*batch);
        }
        csv_writer.Flush();
    }

    EXPECT_EQ(csv_out.str(), csv_data);
}
