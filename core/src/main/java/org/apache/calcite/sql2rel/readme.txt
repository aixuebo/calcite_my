一、入口 Prepare -- PreparedResult prepareSql

SqlToRelConverter -- protected void convertFrom
    SqlToRelConverter --- void convertSelectImpl
        SqlToRelConverter --- RelNode convertSelect(SqlSelect select)
            SqlToRelConverter -- RelNode convertQueryRecursive
                SqlToRelConverter -- RelNode convertQuery
                    Prepare -- PreparedResult prepareSql --- 核心入口
                    CalcitePrepareImpl -- ParseResult parse_(Context context, String sql, boolean convert)