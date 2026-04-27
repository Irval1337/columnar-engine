#include <bruh/bruh.h>
#include <core/schema.h>
#include <util/compression.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <sstream>
#include <string>
#include <vector>

using namespace columnar;  // NOLINT

namespace {
struct CompressionRoundTripParam {
    core::Encoding encoding;
    util::Compression compression;
};

class BruhCompressionRoundTrip : public ::testing::TestWithParam<CompressionRoundTripParam> {};

void WriteIntColumn(std::stringstream& ss, const core::Schema& schema,
                    const std::vector<int64_t>& values, core::Encoding encoding,
                    util::Compression compression) {
    bruh::BruhWriterOptions opts;
    opts.encoding = encoding;
    opts.compression = compression;
    bruh::BruhBatchWriter writer(ss, schema, opts);
    core::Batch batch(schema);
    for (auto v : values) {
        batch.ColumnAt(0).AppendFromString(std::to_string(v));
    }
    writer.Write(batch);
    writer.Flush();
}
}  // namespace

TEST_P(BruhCompressionRoundTrip, Int64) {
    auto param = GetParam();
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::vector<int64_t> values;
    for (int64_t i = 0; i < 2000; ++i) {
        values.push_back(1000 + (i % 50));
    }

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    WriteIntColumn(ss, schema, values, param.encoding, param.compression);

    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    auto batch = reader.ReadRowGroup(0);
    ASSERT_EQ(batch.RowsCount(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), std::to_string(values[i]));
    }
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].encoding, param.encoding);
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].compression, param.compression);
}

INSTANTIATE_TEST_SUITE_P(
    EncodingByCompression, BruhCompressionRoundTrip,
    ::testing::Values(
        CompressionRoundTripParam{core::Encoding::Plain, util::Compression::None},
        CompressionRoundTripParam{core::Encoding::Plain, util::Compression::Lz4},
        CompressionRoundTripParam{core::Encoding::Plain, util::Compression::Zstd},
        CompressionRoundTripParam{core::Encoding::FrameOfReference, util::Compression::None},
        CompressionRoundTripParam{core::Encoding::FrameOfReference, util::Compression::Lz4},
        CompressionRoundTripParam{core::Encoding::FrameOfReference, util::Compression::Zstd},
        CompressionRoundTripParam{core::Encoding::Delta, util::Compression::None},
        CompressionRoundTripParam{core::Encoding::Delta, util::Compression::Lz4},
        CompressionRoundTripParam{core::Encoding::Delta, util::Compression::Zstd},
        CompressionRoundTripParam{core::Encoding::RLE, util::Compression::Lz4},
        CompressionRoundTripParam{core::Encoding::RLE, util::Compression::Zstd}));

TEST(BruhCompression, StringDictionaryWithLz4RoundTrip) {
    core::Schema schema({core::Field("s", core::DataType::String)});
    std::vector<std::string> categories = {"alpha", "beta", "gamma", "delta", "epsilon"};
    std::vector<std::string> values;
    for (size_t i = 0; i < 3000; ++i) {
        values.push_back(categories[i % categories.size()]);
    }

    bruh::BruhWriterOptions opts;
    opts.encoding = core::Encoding::Dictionary;
    opts.compression = util::Compression::Lz4;

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (auto& v : values) {
            batch.ColumnAt(0).AppendFromString(v);
        }
        writer.Write(batch);
        writer.Flush();
    }

    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    auto batch = reader.ReadRowGroup(0);
    ASSERT_EQ(batch.RowsCount(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(batch.ColumnAt(0).GetAsString(i), values[i]);
    }
    auto& chunk = reader.GetMetaData().row_groups[0].columns[0];
    EXPECT_LT(chunk.compressed_size, chunk.uncompressed_size);
}

TEST(BruhCompression, RepetitiveDataIsActuallySmaller) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::vector<int64_t> values(10000, 42);

    auto encoded_size = [&](util::Compression compression) {
        bruh::BruhWriterOptions opts;
        opts.encoding = core::Encoding::Plain;
        opts.compression = compression;
        std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (auto v : values) {
            batch.ColumnAt(0).AppendFromString(std::to_string(v));
        }
        writer.Write(batch);
        writer.Flush();
        ss.seekg(0);
        bruh::BruhBatchReader reader(ss);
        return reader.GetMetaData().row_groups[0].columns[0].compressed_size;
    };

    auto plain = encoded_size(util::Compression::None);
    auto lz4 = encoded_size(util::Compression::Lz4);
    auto zstd = encoded_size(util::Compression::Zstd);
    EXPECT_LT(lz4, plain);
    EXPECT_LT(zstd, plain);
    EXPECT_LT(zstd, plain / 4);
}

TEST(BruhCompression, AutoCompressionUsesOptionWhenUseful) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    std::vector<int64_t> values;
    for (int64_t i = 0; i < 1000; ++i) {
        values.push_back(42);
    }

    bruh::BruhWriterOptions opts;
    opts.encoding = core::Encoding::Plain;
    opts.compression = util::Compression::Zstd;

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (auto v : values) {
            batch.ColumnAt(0).AppendFromString(std::to_string(v));
        }
        writer.Write(batch);
        writer.Flush();
    }
    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    EXPECT_EQ(reader.GetMetaData().row_groups[0].columns[0].compression, util::Compression::Zstd);
}

TEST(BruhCompression, AutoCompressionSkipsBiggerResult) {
    core::Schema schema({core::Field("x", core::DataType::Int64)});
    bruh::BruhWriterOptions opts;
    opts.encoding = core::Encoding::Plain;
    opts.compression = util::Compression::Lz4;

    std::mt19937_64 rng(42);
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (size_t i = 0; i < 5000; ++i) {
            batch.ColumnAt(0).AppendFromString(std::to_string(rng() & ((uint64_t{1} << 60) - 1)));
        }
        writer.Write(batch);
        writer.Flush();
    }

    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    auto& chunk = reader.GetMetaData().row_groups[0].columns[0];
    EXPECT_EQ(chunk.compression, util::Compression::None);
    EXPECT_EQ(chunk.compressed_size, chunk.uncompressed_size);
}

TEST(BruhCompression, PerColumnOverridesGlobal) {
    core::Schema schema(
        {core::Field("a", core::DataType::Int64), core::Field("b", core::DataType::Int64)});
    bruh::BruhWriterOptions opts;
    opts.compression = util::Compression::Zstd;
    opts.column_compression[0] = util::Compression::Lz4;
    opts.column_compression[1] = util::Compression::None;

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema, opts);
        core::Batch batch(schema);
        for (int64_t i = 0; i < 100; ++i) {
            batch.ColumnAt(0).AppendFromString(std::to_string(i));
            batch.ColumnAt(1).AppendFromString(std::to_string(i * 2));
        }
        writer.Write(batch);
        writer.Flush();
    }
    ss.seekg(0);
    bruh::BruhBatchReader reader(ss);
    auto& cols = reader.GetMetaData().row_groups[0].columns;
    EXPECT_EQ(cols[0].compression, util::Compression::Lz4);
    EXPECT_EQ(cols[1].compression, util::Compression::None);
}
