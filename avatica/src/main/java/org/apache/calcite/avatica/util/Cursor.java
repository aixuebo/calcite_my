/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.avatica.util;

import org.apache.calcite.avatica.ColumnMetaData;

import java.io.Closeable;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * Interface to an iteration that is similar to, and can easily support,
 * a JDBC {@link java.sql.ResultSet}, but is simpler to implement.
 * 游标对象,相当于迭代器对象,一行一行查看数据
 *
 背景知识:
 1.经过解析,已经知道数据list集合，以及每一行数据对应的列信息(列序号、列名、列类型)
 2.循环每一行数据,每一列数据(Getter方法返回列的值)。
 3.因为知道每一列的类型,因此可以转换成对应的列的值转换成对应的类型方法，比如long getLong()返回列的long值

 因此 参考 ObjectEnumeratorCursor  以及 AvaticaResultSet
 while(next()){ //是否有下一行数据
    Object record = current() //获取下一行数据


 }


 */
public interface Cursor extends Closeable {
  /**
   * Creates a list of accessors, one per column.
   *
   *
   * @param types List of column types, per {@link java.sql.Types}.
   * @param localCalendar Calendar in local timezone
   * @param factory Factory that creates sub-ResultSets when needed
   * @return List of column accessors
   */
  List<Accessor> createAccessors(List<ColumnMetaData> types,
      Calendar localCalendar, //日期对象如何转换
      ArrayImpl.Factory factory);//数组的时候处理,似乎使用的价值没有那么大,暂时可忽略

  /**
   * Moves to the next row.
   *
   * @return Whether moved
   *
   * @throws SQLException on database error
   * 是否有下一行数据
   */
  boolean next() throws SQLException;

  /**
   * Closes this cursor and releases resources.
   */
  void close();

  /**
   * Returns whether the last value returned was null.
   *
   * @throws SQLException on database error
   */
  boolean wasNull() throws SQLException;

  /**
   * Accessor of a column value.
   * 代表一行数据中的所有列
   */
  public interface Accessor {
    boolean wasNull() throws SQLException;

    String getString() throws SQLException;

    boolean getBoolean() throws SQLException;

    byte getByte() throws SQLException;

    short getShort() throws SQLException;

    int getInt() throws SQLException;

    long getLong() throws SQLException;

    float getFloat() throws SQLException;

    double getDouble() throws SQLException;

    BigDecimal getBigDecimal() throws SQLException;

    BigDecimal getBigDecimal(int scale) throws SQLException;

    byte[] getBytes() throws SQLException;

    InputStream getAsciiStream() throws SQLException;

    InputStream getUnicodeStream() throws SQLException;

    InputStream getBinaryStream() throws SQLException;

    Object getObject() throws SQLException;

    Reader getCharacterStream() throws SQLException;

    Object getObject(Map<String, Class<?>> map) throws SQLException;

    Ref getRef() throws SQLException;

    Blob getBlob() throws SQLException;

    Clob getClob() throws SQLException;

    Array getArray() throws SQLException;

    Date getDate(Calendar calendar) throws SQLException;

    Time getTime(Calendar calendar) throws SQLException;

    Timestamp getTimestamp(Calendar calendar) throws SQLException;

    URL getURL() throws SQLException;

    NClob getNClob() throws SQLException;

    SQLXML getSQLXML() throws SQLException;

    String getNString() throws SQLException;

    Reader getNCharacterStream() throws SQLException;

    <T> T getObject(Class<T> type) throws SQLException;
  }
}

// End Cursor.java
