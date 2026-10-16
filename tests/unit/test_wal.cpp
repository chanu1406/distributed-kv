#include <gtest/gtest.h>

#include "storage/wal.h"

#include <cstdio>
#include <filesystem>
#include <fstream>

namespace {

// Create a unique temp directory for each test to avoid collisions
class WalTest : public ::testing::Test {
protected:
    std::string test_dir;

    void SetUp() override {
        test_dir = "/tmp/dkv_wal_test_" + std::to_string(::getpid()) + "_" +
                   std::to_string(counter_++);
        std::filesystem::remove_all(test_dir);
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir);
    }

private:
    static int counter_;
};

int WalTest::counter_ = 0;

}  // namespace

TEST_F(WalTest, AppendAndRecover) {
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));

        dkv::WalRecord rec;
        rec.timestamp_ms = 1000;
        rec.op_type      = dkv::OpType::SET;
        rec.key           = "hello";
        rec.value         = "world";

        uint64_t seq = wal.append(rec);
        EXPECT_EQ(seq, 1u);
        wal.sync();
        wal.close();
    }

    // Reopen and recover
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));

        auto records = wal.recover();
        ASSERT_EQ(records.size(), 1u);
        EXPECT_EQ(records[0].seq_no, 1u);
        EXPECT_EQ(records[0].timestamp_ms, 1000u);
        EXPECT_EQ(records[0].op_type, dkv::OpType::SET);
        EXPECT_EQ(records[0].key, "hello");
        EXPECT_EQ(records[0].value, "world");
        wal.close();
    }
}

TEST_F(WalTest, MultipleRecords) {
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));

        for (int i = 0; i < 10; i++) {
            dkv::WalRecord rec;
            rec.timestamp_ms = static_cast<uint64_t>(i * 100);
            rec.op_type      = (i % 2 == 0) ? dkv::OpType::SET : dkv::OpType::DEL;
            rec.key           = "key_" + std::to_string(i);
            rec.value         = (i % 2 == 0) ? "val_" + std::to_string(i) : "";

            wal.append(rec);
        }
        wal.sync();
        wal.close();
    }

    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));

        auto records = wal.recover();
        ASSERT_EQ(records.size(), 10u);

        for (int i = 0; i < 10; i++) {
            EXPECT_EQ(records[i].seq_no, static_cast<uint64_t>(i + 1));
            EXPECT_EQ(records[i].key, "key_" + std::to_string(i));
        }
        wal.close();
    }
}

TEST_F(WalTest, CorruptedTailRecovery) {
    std::string filepath;
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));

        for (int i = 0; i < 5; i++) {
            dkv::WalRecord rec;
            rec.timestamp_ms = static_cast<uint64_t>(i * 100);
            rec.op_type      = dkv::OpType::SET;
            rec.key           = "key_" + std::to_string(i);
            rec.value         = "val_" + std::to_string(i);
            wal.append(rec);
        }
        wal.sync();
        wal.close();
        filepath = test_dir + "/wal.bin";
    }

    // Corrupt the last record by truncating a few bytes
    {
        auto file_size = std::filesystem::file_size(filepath);
        std::filesystem::resize_file(filepath, file_size - 5);
    }

    // Recovery should return the first 4 valid records
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));

        auto records = wal.recover();
        EXPECT_EQ(records.size(), 4u);

        for (size_t i = 0; i < records.size(); i++) {
            EXPECT_EQ(records[i].key, "key_" + std::to_string(i));
        }
        wal.close();
    }
}

TEST_F(WalTest, MonotonicSequenceNumbers) {
    dkv::WAL wal;
    ASSERT_TRUE(wal.open(test_dir));

    uint64_t prev = 0;
    for (int i = 0; i < 20; i++) {
        dkv::WalRecord rec;
        rec.op_type = dkv::OpType::SET;
        rec.key     = "k";
        rec.value   = "v";
        uint64_t seq = wal.append(rec);
        EXPECT_GT(seq, prev);
        prev = seq;
    }
    wal.close();
}

TEST_F(WalTest, EmptyWalRecovery) {
    dkv::WAL wal;
    ASSERT_TRUE(wal.open(test_dir));

    auto records = wal.recover();
    EXPECT_TRUE(records.empty());
    wal.close();
}

// ── WAL truncation tests ──────────────────────────────────────────────────────

// 1. Write 10 records; truncate before seq 5; recover — only seqs 6-10 survive.
TEST_F(WalTest, TruncateBeforeMidPoint) {
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));

        for (int i = 0; i < 10; ++i) {
            dkv::WalRecord rec;
            rec.timestamp_ms = static_cast<uint64_t>(i * 100);
            rec.op_type      = dkv::OpType::SET;
            rec.key          = "key_" + std::to_string(i + 1);  // keys 1-10
            rec.value        = "val_" + std::to_string(i + 1);
            wal.append(rec);  // seq_no 1..10
        }
        wal.sync();

        // Truncate: remove seq_no 1-5, keep 6-10.
        wal.truncate_before(5);
        wal.close();
    }

    // Reopen and recover — must return exactly seqs 6-10.
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));
        auto records = wal.recover();
        ASSERT_EQ(records.size(), 5u);
        for (size_t i = 0; i < records.size(); ++i) {
            EXPECT_EQ(records[i].seq_no, static_cast<uint64_t>(i + 6));
            EXPECT_EQ(records[i].key, "key_" + std::to_string(i + 6));
        }
        wal.close();
    }
}

// 2. Truncate, then simulate crash (skip close), reopen — post-truncation
//    records still present.
TEST_F(WalTest, TruncateSurvivesReopen) {
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));

        for (int i = 0; i < 8; ++i) {
            dkv::WalRecord rec;
            rec.op_type = dkv::OpType::SET;
            rec.key     = "k" + std::to_string(i + 1);
            rec.value   = "v";
            wal.append(rec);  // seqs 1-8
        }
        wal.sync();
        wal.truncate_before(4);  // keep seqs 5-8
        wal.sync();
        // close() mimics clean shutdown; rename was already atomic so even
        // without this the file is consistent.
        wal.close();
    }

    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));
        auto records = wal.recover();
        ASSERT_EQ(records.size(), 4u);
        for (size_t i = 0; i < records.size(); ++i) {
            EXPECT_EQ(records[i].seq_no, static_cast<uint64_t>(i + 5));
        }
        wal.close();
    }
}

// 3. Truncate before seq 0 is a no-op — all records survive.
TEST_F(WalTest, TruncateBeforeZeroIsNoop) {
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));

        for (int i = 0; i < 5; ++i) {
            dkv::WalRecord rec;
            rec.op_type = dkv::OpType::SET;
            rec.key     = "key" + std::to_string(i);
            rec.value   = "val";
            wal.append(rec);
        }
        wal.sync();
        wal.truncate_before(0);  // no-op
        wal.close();
    }

    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));
        auto records = wal.recover();
        EXPECT_EQ(records.size(), 5u);
        wal.close();
    }
}

// 4. Truncate before seq > max — all records removed, WAL becomes empty.
TEST_F(WalTest, TruncateBeforeMaxRemovesAll) {
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));

        for (int i = 0; i < 5; ++i) {
            dkv::WalRecord rec;
            rec.op_type = dkv::OpType::SET;
            rec.key     = "key" + std::to_string(i);
            rec.value   = "val";
            wal.append(rec);
        }
        wal.sync();
        wal.truncate_before(9999);  // beyond last seq
        wal.close();
    }

    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));
        auto records = wal.recover();
        EXPECT_TRUE(records.empty());
        wal.close();
    }
}

// 5. After truncation, new appends land in the same file and are recoverable.
TEST_F(WalTest, AppendAfterTruncateWorks) {
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));

        // Write 6 records.
        for (int i = 0; i < 6; ++i) {
            dkv::WalRecord rec;
            rec.op_type = dkv::OpType::SET;
            rec.key     = "pre_" + std::to_string(i + 1);
            rec.value   = "v";
            wal.append(rec);  // seqs 1-6
        }
        wal.sync();
        wal.truncate_before(3);  // keep seqs 4-6

        // Append 3 more records after truncation.
        for (int i = 0; i < 3; ++i) {
            dkv::WalRecord rec;
            rec.op_type = dkv::OpType::SET;
            rec.key     = "post_" + std::to_string(i);
            rec.value   = "w";
            wal.append(rec);  // seqs 7, 8, 9
        }
        wal.sync();
        wal.close();
    }

    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));
        auto records = wal.recover();
        // Expect seqs 4, 5, 6, 7, 8, 9.
        ASSERT_EQ(records.size(), 6u);
        EXPECT_EQ(records[0].seq_no, 4u);
        EXPECT_EQ(records[0].key, "pre_4");
        EXPECT_EQ(records[2].seq_no, 6u);
        EXPECT_EQ(records[3].seq_no, 7u);
        EXPECT_EQ(records[3].key, "post_0");
        EXPECT_EQ(records[5].seq_no, 9u);
        wal.close();
    }
}

TEST_F(WalTest, AppendAfterTornTailSurvivesSecondRecovery) {
    dkv::WalRecord rec;
    rec.key = "key";
    rec.value = "value";
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));
        wal.append(rec);
        wal.append(rec);
        wal.close();
    }
    const auto path = test_dir + "/wal.bin";
    std::filesystem::resize_file(path, std::filesystem::file_size(path) - 1);
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));
        ASSERT_EQ(wal.recover().size(), 1u);
        rec.key = "after_recovery";
        EXPECT_EQ(wal.append(rec), 2u);
        wal.close();
    }
    dkv::WAL wal;
    ASSERT_TRUE(wal.open(test_dir));
    auto records = wal.recover();
    wal.close();
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records.back().key, "after_recovery");
}

TEST_F(WalTest, RejectsOverflowingKeyLength) {
    std::filesystem::create_directories(test_dir);
    // Legacy record: key_len=UINT32_MAX wraps the original 32-bit bound check.
    std::string bytes(29, '\0');
    for (size_t i = 21; i < 25; ++i) bytes[i] = static_cast<char>(0xff);
    std::ofstream(test_dir + "/wal.bin", std::ios::binary).write(bytes.data(), bytes.size());
    dkv::WAL wal;
    ASSERT_TRUE(wal.open(test_dir));
    EXPECT_TRUE(wal.recover().empty());
    wal.close();
}

TEST_F(WalTest, DestructionJoinsFsyncAndClosesWal) {
    ASSERT_EXIT({
        {
            dkv::WAL wal;
            if (!wal.open(test_dir, 1, 0)) std::exit(2);
            dkv::WalRecord rec;
            rec.key = "last";
            rec.value = "write";
            wal.append(rec);
        }
        std::exit(0);
    }, ::testing::ExitedWithCode(0), "");
    dkv::WAL wal;
    ASSERT_TRUE(wal.open(test_dir));
    const auto records = wal.recover();
    wal.close();
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].key, "last");
}

TEST_F(WalTest, MixedLegacyAndVersionedRecordsSurviveCompaction) {
    dkv::WalRecord rec;
    rec.key = "k";
    rec.value = "v";
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));
        wal.append(rec); // legacy record without origin
        rec.node_id = 0;
        wal.append(rec); // node 0 is present, not a legacy sentinel
        rec.node_id = UINT32_MAX;
        rec.op_type = dkv::OpType::DEL;
        rec.value.clear();
        wal.append(rec);
        const auto records = wal.recover();
        ASSERT_EQ(records.size(), 3u);
        EXPECT_FALSE(records[0].node_id.has_value());
        EXPECT_EQ(records[1].node_id, 0u);
        wal.truncate_before(1);
    }
    dkv::WAL wal;
    ASSERT_TRUE(wal.open(test_dir));
    const auto records = wal.recover(1);
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[0].node_id, 0u);
    EXPECT_EQ(records[1].node_id, UINT32_MAX);
    EXPECT_EQ(records[1].op_type, dkv::OpType::DEL);
    EXPECT_EQ(wal.append(rec), 4u);
}

TEST_F(WalTest, EveryTruncatedVersionedTailAllowsFurtherRecovery) {
    dkv::WalRecord rec;
    rec.key = "key";
    rec.value = "value";
    rec.node_id = 7;
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));
        wal.append(rec);
    }
    const auto path = test_dir + "/wal.bin";
    std::ifstream in(path, std::ios::binary);
    const std::string original((std::istreambuf_iterator<char>(in)), {});
    in.close();
    for (size_t cut = 1; cut < original.size(); ++cut) {
        SCOPED_TRACE(cut);
        std::ofstream(path, std::ios::binary | std::ios::trunc)
            .write(original.data(), static_cast<std::streamsize>(cut));
        {
            dkv::WAL wal;
            ASSERT_TRUE(wal.open(test_dir));
            EXPECT_TRUE(wal.recover(10).empty());
            EXPECT_EQ(wal.append(rec), 11u);
        }
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));
        const auto records = wal.recover(10);
        ASSERT_EQ(records.size(), 1u);
        EXPECT_EQ(records[0].seq_no, 11u);
        EXPECT_EQ(records[0].node_id, 7u);
        EXPECT_EQ(records[0].value, "value");
    }
}
