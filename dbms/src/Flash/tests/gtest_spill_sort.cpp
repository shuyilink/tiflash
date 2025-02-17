// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{

class SpillSortTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
    }
};

/// todo add more tests
TEST_F(SpillSortTestRunner, SimpleCase)
try
{
    DB::MockColumnInfoVec column_infos{{"a", TiDB::TP::TypeLongLong}, {"b", TiDB::TP::TypeLongLong}, {"c", TiDB::TP::TypeLongLong}, {"d", TiDB::TP::TypeLongLong}, {"e", TiDB::TP::TypeLongLong}};
    ColumnsWithTypeAndName column_data;
    size_t table_rows = 102400;
    UInt64 max_block_size = 500;
    size_t original_max_streams = 20;
    size_t total_data_size = 0;
    size_t limit_size = table_rows / 10 * 9;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
    {
        ColumnGeneratorOpts opts{table_rows, getDataTypeByColumnInfoForComputingLayer(column_info)->getName(), RANDOM, column_info.name};
        column_data.push_back(ColumnGenerator::instance().generate(opts));
        total_data_size += column_data.back().column->byteSize();
    }
    context.addMockTable("spill_sort_test", "simple_table", column_infos, column_data, 8);

    MockOrderByItemVec order_by_items{std::make_pair("a", true), std::make_pair("b", true), std::make_pair("c", true), std::make_pair("d", true), std::make_pair("e", true)};

    auto request = context
                       .scan("spill_sort_test", "simple_table")
                       .topN(order_by_items, limit_size)
                       .build(context);
    context.context.setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    /// disable spill
    context.context.setSetting("max_bytes_before_external_sort", Field(static_cast<UInt64>(0)));
    auto ref_columns = executeStreams(request, original_max_streams);
    /// enable spill
    context.context.setSetting("max_bytes_before_external_sort", Field(static_cast<UInt64>(total_data_size / 10)));
    // don't use `executeAndAssertColumnsEqual` since it takes too long to run
    /// todo use ASSERT_COLUMNS_EQ_R once TiFlash support final TopN
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
    /// enable spill and use small max_spilled_size_per_spill
    context.context.setSetting("max_spilled_size_per_spill", Field(static_cast<UInt64>(total_data_size / 100)));
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
}
CATCH
} // namespace tests
} // namespace DB
