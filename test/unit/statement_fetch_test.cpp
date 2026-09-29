#include "maxcompute_odbc/odbc_api/handles.h"
#include <gtest/gtest.h>

using namespace maxcompute_odbc;

TEST(StatementFetch, NullColumnInvalidatesPreviousReadPosition) {
  for (const SQLLEN first_buffer_size : {SQLLEN(4), SQLLEN(16)}) {
    StmtHandle stmt(nullptr);
    std::vector<Column> columns(2);
    auto schema = std::make_shared<ResultSetSchema>(std::move(columns));
    Record row;
    row.values.emplace_back(std::string("abcdef"));
    row.values.emplace_back(std::monostate{});
    std::vector<Record> rows;
    rows.push_back(std::move(row));
    stmt.setResultStream(std::make_unique<StaticResultStream>(schema, std::move(rows)));
    ASSERT_EQ(SQL_SUCCESS, stmt.fetch());
    char buffer[16] = {};
    SQLLEN indicator = 0;
    ASSERT_TRUE(SQL_SUCCEEDED(stmt.getData(1, SQL_C_CHAR, buffer,
                                          first_buffer_size, &indicator)));
    ASSERT_EQ(SQL_SUCCESS, stmt.getData(2, SQL_C_CHAR, buffer, sizeof(buffer), &indicator));
    ASSERT_EQ(SQL_NULL_DATA, indicator);
    ASSERT_EQ(SQL_SUCCESS, stmt.getData(1, SQL_C_CHAR, buffer, sizeof(buffer), &indicator));
    EXPECT_EQ(6, indicator);
    EXPECT_STREQ("abcdef", buffer);
  }
}
