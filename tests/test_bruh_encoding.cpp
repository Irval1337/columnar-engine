#include <core/encoding/bit_packing.h>
#include <core/encoding/delta.h>
#include <core/columns/dictionary_string_column.h>
#include <gtest/gtest.h>
#include <bruh/bruh.h>
#include <core/schema.h>
#include <util/bit_vector.h>

#include <cstdio>
#include <cstdint>
#include <random>
#include <sstream>
#include <string>
#include <vector>

using namespace columnar;  // NOLINT

namespace {
void WriteDictString(std::stringstream& ss, const core::Schema& schema,
                     const std::vector<std::string>& values, const std::vector<bool>& is_null) {
    bruh::BruhWriterOptions opts;
    opts.encoding = core::Encoding::Dictionary;
    bruh::BruhBatchWriter writer(ss, schema, opts);
    core::Batch batch(schema);
    for (size_t i = 0; i < values.size(); ++i) {
        if (!is_null.empty() && is_null[i]) {
            batch.ColumnAt(0).AppendNull();
        } else {
            batch.ColumnAt(0).AppendFromString(values[i]);
        }
    }
    writer.Write(batch);
    writer.Flush();
}

void WriteNumericVals(std::stringstream& ss, const core::Schema& schema,
                      const std::vector<int64_t>& values, core::Encoding encoding) {
    bruh::BruhWriterOptions opts;
    opts.encoding = encoding;
    bruh::BruhBatchWriter writer(ss, schema, opts);
    core::Batch batch(schema);
    for (auto v : values) {
        batch.ColumnAt(0).AppendFromString(std::to_string(v));
    }
    writer.Write(batch);
    writer.Flush();
}
}  // namespace

TEST(BruhDictionary, StringLowCardinalityRoundTrip) {
    core::Schema schema({core::Field("category", core::DataType::String)});
    std::vector<std::string> categories = {"red", "green", "blue", "yellow", "purple"};
    std::vector<std::string> values;
    for (size_t i = 0; i < 5000; ++i) {
        values.push_back(categories[i % categories.size()]);
    }

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteDictString(ss, schema, values, {});

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    ASSERT_EQ(batch.RowsCount(), values.size());
    ASSERT_NE(dynamic_cast<const core::DictionaryStringColumn*>(&batch.ColumnAt(0)), nullptr);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), values[i]);
    }
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, core::Encoding::Dictionary);
}

TEST(BruhDictionary, StringHighCardinalityUsesUint16indexes) {
    core::Schema schema({core::Field("tag", core::DataType::String)});
    std::vector<std::string> values;
    for (size_t i = 0; i < 1000; ++i) {
        values.push_back("tag_" + std::to_string(i % 500));
    }

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteDictString(ss, schema, values, {});

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    ASSERT_EQ(batch.RowsCount(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), values[i]);
    }
}

TEST(BruhDictionary, StringNullableRoundTrip) {
    core::Schema schema({core::Field("opt", core::DataType::String, true)});
    std::vector<std::string> values;
    std::vector<bool> is_null;
    for (size_t i = 0; i < 500; ++i) {
        values.push_back("v" + std::to_string(i % 4));
        is_null.push_back(i % 3 == 0);
    }

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteDictString(ss, schema, values, is_null);

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    ASSERT_EQ(batch.RowsCount(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        if (is_null[i]) {
            EXPECT_TRUE(batch.ColumnAt(0).IsNull(i));
        } else {
            EXPECT_FALSE(batch.ColumnAt(0).IsNull(i));
            EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), values[i]);
        }
    }
}

TEST(BruhDictionary, ShrinksForRepetitiveData) {
    core::Schema schema({core::Field("s", core::DataType::String)});
    std::vector<std::string> values(5000, "a_rather_long_constant_string_value");

    std::stringstream plain_ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions opts;
        opts.encoding = core::Encoding::Plain;
        bruh::BruhBatchWriter writer(plain_ss, schema, opts);
        core::Batch batch(schema);
        for (auto& v : values) {
            batch.ColumnAt(0).AppendFromString(v);
        }
        writer.Write(batch);
        writer.Flush();
    }

    std::stringstream dict_ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteDictString(dict_ss, schema, values, {});

    EXPECT_LT(dict_ss.str().size(), plain_ss.str().size());
}

TEST(BruhDictionary, ThrowsWhenCardinalityExceedsLimit) {
    core::Schema schema({core::Field("s", core::DataType::String)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    bruh::BruhWriterOptions opts;
    opts.encoding = core::Encoding::Dictionary;
    bruh::BruhBatchWriter writer(ss, schema, opts);
    core::Batch batch(schema);
    for (size_t i = 0; i < 65540; ++i) {
        batch.ColumnAt(0).AppendFromString("v" + std::to_string(i));
    }
    EXPECT_THROW(writer.Write(batch), std::runtime_error);
}

TEST(BruhRLE, Int64RunsRoundTrip) {
    core::Schema schema({core::Field("x", core::DataType::Int64, true)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    std::vector<int64_t> values;
    std::vector<bool> is_null;
    for (size_t r = 0; r < 100; ++r) {
        for (size_t k = 0; k < 50; ++k) {
            values.push_back(static_cast<int64_t>(r));
            is_null.push_back((r + k) % 17 == 0);
        }
    }

    {
        bruh::BruhWriterOptions opts;
        opts.encoding = core::Encoding::RLE;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (size_t i = 0; i < values.size(); ++i) {
            if (is_null[i]) {
                batch.ColumnAt(0).AppendNull();
            } else {
                batch.ColumnAt(0).AppendFromString(std::to_string(values[i]));
            }
        }
        writer.Write(batch);
        writer.Flush();
    }

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    ASSERT_EQ(batch.RowsCount(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        if (is_null[i]) {
            EXPECT_TRUE(batch.ColumnAt(0).IsNull(i));
        } else {
            EXPECT_FALSE(batch.ColumnAt(0).IsNull(i));
            EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), std::to_string(values[i]));
        }
    }
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, core::Encoding::RLE);
}

TEST(BruhRLE, BoolRoundTrip) {
    core::Schema schema({core::Field("flag", core::DataType::Bool)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    std::vector<bool> values;
    for (size_t r = 0; r < 20; ++r) {
        bool v = r % 2 == 0;
        for (size_t k = 0; k < 100; ++k) {
            values.push_back(v);
        }
    }

    {
        bruh::BruhWriterOptions opts;
        opts.encoding = core::Encoding::RLE;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (auto v : values) {
            batch.ColumnAt(0).AppendFromString(v ? "true" : "false");
        }
        writer.Write(batch);
        writer.Flush();
    }

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    ASSERT_EQ(batch.RowsCount(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), values[i] ? "true" : "false");
    }
}

TEST(BruhRLE, CharRoundTrip) {
    core::Schema schema({core::Field("c", core::DataType::Char)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    std::string values;
    for (size_t r = 0; r < 26; ++r) {
        values.append(40, static_cast<char>('a' + r));
    }

    {
        bruh::BruhWriterOptions opts;
        opts.encoding = core::Encoding::RLE;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (char c : values) {
            batch.ColumnAt(0).AppendFromString(std::string(1, c));
        }
        writer.Write(batch);
        writer.Flush();
    }

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    ASSERT_EQ(batch.RowsCount(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), std::string(1, values[i]));
    }
}

TEST(BruhRLE, ShrinksForRepetitiveIntegers) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});

    std::stringstream plain_ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions opts;
        opts.encoding = core::Encoding::Plain;
        bruh::BruhBatchWriter writer(plain_ss, schema, opts);
        core::Batch batch(schema);
        for (size_t i = 0; i < 5000; ++i) {
            batch.ColumnAt(0).AppendFromString(std::to_string(static_cast<int64_t>(42)));
        }
        writer.Write(batch);
        writer.Flush();
    }

    std::stringstream rle_ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions opts;
        opts.encoding = core::Encoding::RLE;
        bruh::BruhBatchWriter writer(rle_ss, schema, opts);
        core::Batch batch(schema);
        for (size_t i = 0; i < 5000; ++i) {
            batch.ColumnAt(0).AppendFromString(std::to_string(static_cast<int64_t>(42)));
        }
        writer.Write(batch);
        writer.Flush();
    }

    EXPECT_LT(rle_ss.str().size(), plain_ss.str().size());
}

TEST(BruhFOR, Int32NarrowRangeRoundTrip) {
    core::Schema schema({core::Field("x", core::DataType::Int32)});
    std::vector<int64_t> values;
    for (size_t i = 0; i < 5000; ++i) {
        values.push_back(1000000 + static_cast<int64_t>(i % 100));
    }

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteNumericVals(ss, schema, values, core::Encoding::FrameOfReference);

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    ASSERT_EQ(batch.RowsCount(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), std::to_string(values[i]));
    }
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding,
              core::Encoding::FrameOfReference);
}

TEST(BruhFOR, Int64ConstantOptimum) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::vector<int64_t> values(5000, -12345);

    std::stringstream plain_ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteNumericVals(plain_ss, schema, values, core::Encoding::Plain);

    std::stringstream for_ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteNumericVals(for_ss, schema, values, core::Encoding::FrameOfReference);

    EXPECT_LT(for_ss.str().size(), plain_ss.str().size());

    auto buf = for_ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), std::to_string(values[i]));
    }
}

TEST(BruhFOR, Int64NegativeRangeRoundTrip) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::vector<int64_t> values;
    for (size_t i = 0; i < 1000; ++i) {
        values.push_back(-500 + static_cast<int64_t>(i));
    }

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteNumericVals(ss, schema, values, core::Encoding::FrameOfReference);

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), std::to_string(values[i]));
    }
}

TEST(BruhFOR, DateRoundTrip) {
    core::Schema schema({core::Field("d", core::DataType::Date)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    bruh::BruhWriterOptions opts;
    opts.encoding = core::Encoding::FrameOfReference;
    bruh::BruhBatchWriter writer(ss, schema, opts);
    core::Batch batch(schema);
    for (size_t i = 0; i < 500; ++i) {
        batch.ColumnAt(0).AppendFromString("2026-01-01");
    }
    writer.Write(batch);
    writer.Flush();

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch2 = reader.ReadRowGroup(0);
    ASSERT_EQ(batch2.RowsCount(), 500u);
    for (size_t i = 0; i < 500; ++i) {
        EXPECT_EQ(batch2.ColumnAt(0).GetAsString(i), "2026-01-01");
    }
}

TEST(BruhBitPacking, Int32UnsignedRoundTrip) {
    core::Schema schema({core::Field("x", core::DataType::Int32)});
    std::vector<int64_t> values;
    for (size_t i = 0; i < 2000; ++i) {
        values.push_back(static_cast<int64_t>(i % 1024));
    }

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteNumericVals(ss, schema, values, core::Encoding::BitPacking);

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), std::to_string(values[i]));
    }
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, core::Encoding::BitPacking);
}

TEST(BruhBitPacking, BoolRoundTrip) {
    core::Schema schema({core::Field("flag", core::DataType::Bool)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    std::vector<bool> values;
    std::mt19937 rng(42);
    for (size_t i = 0; i < 5000; ++i) {
        values.push_back((rng() & 1) != 0);
    }

    {
        bruh::BruhWriterOptions opts;
        opts.encoding = core::Encoding::BitPacking;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (auto v : values) {
            batch.ColumnAt(0).AppendFromString(v ? "true" : "false");
        }
        writer.Write(batch);
        writer.Flush();
    }

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    ASSERT_EQ(batch.RowsCount(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), values[i] ? "true" : "false");
    }
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, core::Encoding::BitPacking);
}

TEST(BruhBitPacking, RejectsNegativeValues) {
    core::Schema schema({core::Field("x", core::DataType::Int32)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    bruh::BruhWriterOptions opts;
    opts.encoding = core::Encoding::BitPacking;
    bruh::BruhBatchWriter writer(ss, schema, opts);
    core::Batch batch(schema);
    batch.ColumnAt(0).AppendFromString("1");
    batch.ColumnAt(0).AppendFromString("-1");
    EXPECT_THROW(writer.Write(batch), std::runtime_error);
}

TEST(BruhDelta, Int64SortedRoundTrip) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::vector<int64_t> values;
    for (size_t i = 0; i < 5000; ++i) {
        values.push_back(1'000'000'000LL + static_cast<int64_t>(i * 3));
    }

    std::stringstream plain_ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteNumericVals(plain_ss, schema, values, core::Encoding::Plain);

    std::stringstream delta_ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteNumericVals(delta_ss, schema, values, core::Encoding::Delta);

    EXPECT_LT(delta_ss.str().size(), plain_ss.str().size());

    auto buf = delta_ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    ASSERT_EQ(batch.RowsCount(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), std::to_string(values[i]));
    }
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, core::Encoding::Delta);
}

TEST(BruhDelta, Int32NonMonotonicRoundTrip) {
    core::Schema schema({core::Field("x", core::DataType::Int32)});
    std::vector<int64_t> values;
    std::mt19937 rng(123);
    int32_t cur = 0;
    for (size_t i = 0; i < 2000; ++i) {
        cur += static_cast<int32_t>(rng() % 200) - 100;
        values.push_back(cur);
    }

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteNumericVals(ss, schema, values, core::Encoding::Delta);

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch = reader.ReadRowGroup(0);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), std::to_string(values[i]));
    }
}

TEST(BruhDelta, TimestampRoundTrip) {
    core::Schema schema({core::Field("ts", core::DataType::Timestamp)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    bruh::BruhWriterOptions opts;
    opts.encoding = core::Encoding::Delta;
    bruh::BruhBatchWriter writer(ss, schema, opts);
    core::Batch batch(schema);
    std::vector<std::string> stamps;
    for (size_t i = 0; i < 200; ++i) {
        char buf[40];
        int minute = static_cast<int>(i % 60);
        int hour = static_cast<int>(i / 60);
        std::snprintf(buf, sizeof(buf), "2026-01-01 %02d:%02d:00", hour, minute);
        stamps.emplace_back(buf);
        batch.ColumnAt(0).AppendFromString(stamps.back());
    }
    writer.Write(batch);
    writer.Flush();

    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    auto batch2 = reader.ReadRowGroup(0);
    ASSERT_EQ(batch2.RowsCount(), stamps.size());
    for (size_t i = 0; i < stamps.size(); ++i) {
        EXPECT_EQ(batch2.ColumnAt(0).GetAsString(i), stamps[i]);
    }
}

TEST(BruhDelta, EmptyPayloadHasNoExtraBytes) {
    std::vector<uint8_t> buf;
    util::BufWriter w(buf);
    std::vector<int64_t> values;
    core::encoding::EncodeDelta<int64_t>(w, values.data(), values.size());

    EXPECT_EQ(buf.size(), sizeof(int64_t));
    util::BufReader r(buf.data(), buf.size());
    auto out = core::encoding::DecodeDelta<int64_t>(r, 0);
    EXPECT_TRUE(out.empty());
    EXPECT_EQ(r.Remaining(), 0u);
}

TEST(BruhAutoSelect, LowCardinalityStringsPicksDictionary) {
    core::Schema schema({core::Field("s", core::DataType::String)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions opts;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        std::vector<std::string> cats = {"x", "yy", "zzz"};
        for (size_t i = 0; i < 1000; ++i) {
            batch.ColumnAt(0).AppendFromString(cats[i % cats.size()]);
        }
        writer.Write(batch);
        writer.Flush();
    }
    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, core::Encoding::Dictionary);
}

TEST(BruhAutoSelect, HighCardinalityStringsPicksPlain) {
    core::Schema schema({core::Field("s", core::DataType::String)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions opts;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (size_t i = 0; i < 300; ++i) {
            batch.ColumnAt(0).AppendFromString("unique_string_" + std::to_string(i));
        }
        writer.Write(batch);
        writer.Flush();
    }
    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, core::Encoding::Plain);
}

TEST(BruhAutoSelect, SortedInt64PicksDelta) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions opts;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (size_t i = 0; i < 2000; ++i) {
            batch.ColumnAt(0).AppendFromString(
                std::to_string(1'000'000'000LL + static_cast<int64_t>(i)));
        }
        writer.Write(batch);
        writer.Flush();
    }
    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, core::Encoding::Delta);
}

TEST(BruhAutoSelect, NarrowRangeInt32PicksFOR) {
    core::Schema schema({core::Field("x", core::DataType::Int32)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions opts;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        std::mt19937 rng(7);
        for (size_t i = 0; i < 2000; ++i) {
            batch.ColumnAt(0).AppendFromString(
                std::to_string(1'000'000 + static_cast<int32_t>(rng() % 100)));
        }
        writer.Write(batch);
        writer.Flush();
    }
    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding,
              core::Encoding::FrameOfReference);
}

TEST(BruhAutoSelect, ConstantInt64PicksFOR) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions opts;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (size_t i = 0; i < 1000; ++i) {
            batch.ColumnAt(0).AppendFromString("42");
        }
        writer.Write(batch);
        writer.Flush();
    }
    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding,
              core::Encoding::FrameOfReference);
}

TEST(BruhAutoSelect, RepetitiveBoolPicksRLE) {
    core::Schema schema({core::Field("b", core::DataType::Bool)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions opts;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (size_t r = 0; r < 20; ++r) {
            bool v = r % 2 == 0;
            for (size_t k = 0; k < 500; ++k) {
                batch.ColumnAt(0).AppendFromString(v ? "true" : "false");
            }
        }
        writer.Write(batch);
        writer.Flush();
    }
    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, core::Encoding::RLE);
}

TEST(BruhAutoSelect, RandomBoolPicksBitPacking) {
    core::Schema schema({core::Field("b", core::DataType::Bool)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions opts;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        std::mt19937 rng(42);
        for (size_t i = 0; i < 1000; ++i) {
            batch.ColumnAt(0).AppendFromString(rng() % 2 ? "true" : "false");
        }
        writer.Write(batch);
        writer.Flush();
    }
    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, core::Encoding::BitPacking);
}

TEST(BruhAutoSelect, DoubleAlwaysPicksPlain) {
    core::Schema schema({core::Field("d", core::DataType::Double)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions opts;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (size_t i = 0; i < 500; ++i) {
            batch.ColumnAt(0).AppendFromString(std::to_string(static_cast<double>(i) * 0.5));
        }
        writer.Write(batch);
        writer.Flush();
    }
    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, core::Encoding::Plain);
}

TEST(BruhAutoSelect, MonotonicTimestampPicksDelta) {
    core::Schema schema({core::Field("ts", core::DataType::Timestamp)});
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhWriterOptions opts;
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (size_t i = 0; i < 500; ++i) {
            char buf[40];
            int day = static_cast<int>(i / 24) + 1;
            int hour = static_cast<int>(i % 24);
            std::snprintf(buf, sizeof(buf), "2026-01-%02d %02d:00:00", day, hour);
            batch.ColumnAt(0).AppendFromString(buf);
        }
        writer.Write(batch);
        writer.Flush();
    }
    auto buf = ss.str();
    bruh::BruhBatchReader reader(
        util::ByteView{reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, core::Encoding::Delta);
}

namespace {
using columnar::core::encoding::BitPackedSize;
using columnar::core::encoding::BitPackWithOffset;
using columnar::core::encoding::BitUnpackWithOffset;
using columnar::core::encoding::BitWidth;
using columnar::core::encoding::PackBitVector;
using columnar::core::encoding::UnpackBitVector;

void BitPackRoundTrip(const std::vector<uint64_t>& values, uint8_t bit_width) {
    std::vector<uint8_t> packed(BitPackedSize(values.size(), bit_width));
    BitPackWithOffset(values.data(), values.size(), uint64_t{0}, bit_width, packed.data());
    std::vector<uint64_t> out(values.size());
    BitUnpackWithOffset(packed.data(), packed.size(), values.size(), bit_width, uint64_t{0},
                        out.data());
    ASSERT_EQ(out, values);
}
}  // namespace

TEST(BitPacking, ZeroWidth) {
    BitPackRoundTrip(std::vector<uint64_t>(100, 0), 0);
}

TEST(BitPacking, SingleBit) {
    std::vector<uint64_t> values;
    for (size_t i = 0; i < 200; ++i) {
        values.push_back(i & 1);
    }
    BitPackRoundTrip(values, 1);
}

TEST(BitPacking, SevenBits) {
    std::vector<uint64_t> values;
    for (size_t i = 0; i < 1000; ++i) {
        values.push_back(i % 128);
    }
    BitPackRoundTrip(values, 7);
}

TEST(BitPacking, ThirtyThreeBits) {
    std::mt19937_64 rng(0xC0FFEE);
    uint64_t mask = (uint64_t{1} << 33) - 1;
    std::vector<uint64_t> values;
    for (size_t i = 0; i < 500; ++i) {
        values.push_back(rng() & mask);
    }
    BitPackRoundTrip(values, 33);
}

TEST(BitPacking, FiftySixBits) {
    std::mt19937_64 rng(42);
    uint64_t mask = (uint64_t{1} << 56) - 1;
    std::vector<uint64_t> values;
    for (size_t i = 0; i < 300; ++i) {
        values.push_back(rng() & mask);
    }
    BitPackRoundTrip(values, 56);
}

TEST(BitPacking, RandomWidths) {
    std::mt19937_64 rng(0xDEAD);
    for (uint8_t w = 1; w <= 56; ++w) {
        uint64_t mask = (uint64_t{1} << w) - 1;
        size_t n = 1 + rng() % 200;
        std::vector<uint64_t> values(n);
        for (auto& v : values) {
            v = rng() & mask;
        }
        BitPackRoundTrip(values, w);
    }
}

TEST(BitPacking, EmptyInput) {
    std::vector<uint64_t> out;
    BitUnpackWithOffset<uint64_t>(nullptr, 0, 0, 7, 0, out.data());
    SUCCEED();
}

TEST(BitPacking, UnderrunThrows) {
    std::vector<uint8_t> packed(2, 0);
    std::vector<uint64_t> out(100);
    EXPECT_THROW(BitUnpackWithOffset(packed.data(), packed.size(), 100, 8, uint64_t{0}, out.data()),
                 std::runtime_error);
}

TEST(BitPacking, BitWidthHelper) {
    EXPECT_EQ(BitWidth(uint64_t{0}), 0);
    EXPECT_EQ(BitWidth(uint64_t{1}), 1);
    EXPECT_EQ(BitWidth(uint64_t{3}), 2);
    EXPECT_EQ(BitWidth(uint64_t{255}), 8);
    EXPECT_EQ(BitWidth(uint64_t{256}), 9);
    EXPECT_EQ(BitWidth((uint64_t{1} << 40) - 1), 40);
}

TEST(BitPacking, BitVectorRoundTrip) {
    util::BitVector bits;
    for (size_t i = 0; i < 1000; ++i) {
        bits.PushBack((i % 3) == 1);
    }
    std::vector<uint8_t> packed(BitPackedSize(bits.Size(), 1));
    PackBitVector(bits, packed.data());
    auto out = UnpackBitVector(packed.data(), packed.size(), bits.Size());
    ASSERT_EQ(out.Size(), bits.Size());
    for (size_t i = 0; i < bits.Size(); ++i) {
        EXPECT_EQ(out.Get(i), bits.Get(i));
    }
}
