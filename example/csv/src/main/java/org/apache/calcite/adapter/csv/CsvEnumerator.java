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
package org.apache.calcite.adapter.csv;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;

import org.apache.commons.lang3.time.FastDateFormat;

import au.com.bytecode.opencsv.CSVReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;


/** Enumerator that reads from a CSV file.
 *
 * @param <E> Row type
 */
class CsvEnumerator<E> implements Enumerator<E> {
  private final CSVReader reader;
  private final String[] filterValues;//过滤条件,每一个列必须的值必须满足该条件,才能被保留,即属于where条件
  private final RowConverter<E> rowConverter;
  private E current;

  private static final FastDateFormat TIME_FORMAT_DATE;
  private static final FastDateFormat TIME_FORMAT_TIME;
  private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

  static {
    TimeZone gmt = TimeZone.getTimeZone("GMT");
    TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
    TIME_FORMAT_TIME = FastDateFormat.getInstance("hh:mm:ss", gmt);
    TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance(
        "yyyy-MM-dd hh:mm:ss", gmt);
  }

  public CsvEnumerator(File file, List<CsvFieldType> fieldTypes) {
    this(file, fieldTypes, identityList(fieldTypes.size()));
  }

  public CsvEnumerator(File file, List<CsvFieldType> fieldTypes, int[] fields) {
    //noinspection unchecked
    this(file, null, (RowConverter<E>) converter(fieldTypes, fields));
  }

  public CsvEnumerator(File file, String[] filterValues,
      RowConverter<E> rowConverter) {
    this.rowConverter = rowConverter;
    this.filterValues = filterValues;
    try {
      this.reader = openCsv(file);
      this.reader.readNext(); // skip header row 因为第一行是schema,所以要跳过
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static RowConverter<?> converter(List<CsvFieldType> fieldTypes,
      int[] fields) {
    if (fields.length == 1) {
      final int field = fields[0];
      return new SingleColumnRowConverter(fieldTypes.get(field), field);
    } else {
      return new ArrayRowConverter(fieldTypes, fields);
    }
  }

  /** Deduces the names and types of a table's columns by reading the first line
   * of a CSV file.
   * 第一行描述列名  格式:colName:colType,colName:colType,colName:colType
   **/
  static RelDataType deduceRowType(JavaTypeFactory typeFactory, File file,
      List<CsvFieldType> fieldTypes) {
    final List<RelDataType> types = new ArrayList<RelDataType>();
    final List<String> names = new ArrayList<String>();
    CSVReader reader = null;
    try {
      reader = openCsv(file);
      final String[] strings = reader.readNext();
      for (String string : strings) {
        final String name;
        final CsvFieldType fieldType;
        final int colon = string.indexOf(':');
        if (colon >= 0) {
          name = string.substring(0, colon);
          String typeString = string.substring(colon + 1);
          fieldType = CsvFieldType.of(typeString);
          if (fieldType == null) {
            System.out.println("WARNING: Found unknown type: "
              + typeString + " in file: " + file.getAbsolutePath()
              + " for column: " + name
              + ". Will assume the type of column is string");
          }
        } else {
          name = string;
          fieldType = null;
        }
        final RelDataType type;
        if (fieldType == null) {
          type = typeFactory.createJavaType(String.class);
        } else {
          type = fieldType.toType(typeFactory);
        }
        names.add(name);
        types.add(type);
        if (fieldTypes != null) {
          fieldTypes.add(fieldType);
        }
      }
    } catch (IOException e) {
      // ignore
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
    if (names.isEmpty()) {
      names.add("line");
      types.add(typeFactory.createJavaType(String.class));
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  private static CSVReader openCsv(File file) throws IOException {
    final Reader fileReader;
    if (file.getName().endsWith(".gz")) {
      final GZIPInputStream inputStream =
          new GZIPInputStream(new FileInputStream(file));
      fileReader = new InputStreamReader(inputStream);
    } else {
      fileReader = new FileReader(file);
    }
    return new CSVReader(fileReader);
  }

  public E current() {
    return current;
  }

  public boolean moveNext() {
    try {
    outer:
      for (;;) {
        final String[] strings = reader.readNext();//读取一行数据
        if (strings == null) {
          current = null;
          reader.close();
          return false;
        }
        if (filterValues != null) {//说明有where条件
          for (int i = 0; i < strings.length; i++) {
            String filterValue = filterValues[i];//说明该列有where条件
            if (filterValue != null) {
              if (!filterValue.equals(strings[i])) {//必须要求相等
                continue outer;
              }
            }
          }
        }
        current = rowConverter.convertRow(strings);
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing CSV reader", e);
    }
  }

  /** Returns an array of integers {0, ..., n - 1}. */
  static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  /** Row converter.
   *  解析的是字符串,但最终结果每一行的列是有类型的,要转换成所属类型
   **/
  abstract static class RowConverter<E> {
    abstract E convertRow(String[] rows);//传入字符串形式的row信息

    //把string字符串转换成CsvFieldType对应的类型值,返回object
    protected Object convert(CsvFieldType fieldType, String string) {
      if (fieldType == null) {
        return string;
      }
      switch (fieldType) {
      case BOOLEAN:
        if (string.length() == 0) {
          return null;
        }
        return Boolean.parseBoolean(string);
      case BYTE:
        if (string.length() == 0) {
          return null;
        }
        return Byte.parseByte(string);
      case SHORT:
        if (string.length() == 0) {
          return null;
        }
        return Short.parseShort(string);
      case INT:
        if (string.length() == 0) {
          return null;
        }
        return Integer.parseInt(string);
      case LONG:
        if (string.length() == 0) {
          return null;
        }
        return Long.parseLong(string);
      case FLOAT:
        if (string.length() == 0) {
          return null;
        }
        return Float.parseFloat(string);
      case DOUBLE:
        if (string.length() == 0) {
          return null;
        }
        return Double.parseDouble(string);
      case DATE:
        if (string.length() == 0) {
          return null;
        }
        try {
          Date date = TIME_FORMAT_DATE.parse(string);
          return new java.sql.Date(date.getTime());
        } catch (ParseException e) {
          return null;
        }
      case TIME:
        if (string.length() == 0) {
          return null;
        }
        try {
          Date date = TIME_FORMAT_TIME.parse(string);
          return new java.sql.Time(date.getTime());
        } catch (ParseException e) {
          return null;
        }
      case TIMESTAMP:
        if (string.length() == 0) {
          return null;
        }
        try {
          Date date = TIME_FORMAT_TIMESTAMP.parse(string);
          return new java.sql.Timestamp(date.getTime());
        } catch (ParseException e) {
          return null;
        }
      case STRING:
      default:
        return string;
      }
    }
  }

  /** Array row converter. */
  static class ArrayRowConverter extends RowConverter<Object[]> {
    private final CsvFieldType[] fieldTypes;
    private final int[] fields;

    ArrayRowConverter(List<CsvFieldType> fieldTypes, int[] fields) {
      this.fieldTypes = fieldTypes.toArray(new CsvFieldType[fieldTypes.size()]);
      this.fields = fields;
    }

    public Object[] convertRow(String[] strings) {
      final Object[] objects = new Object[fields.length];
      for (int i = 0; i < fields.length; i++) {
        int field = fields[i];
        objects[i] = convert(fieldTypes[field], strings[field]);
      }
      return objects;
    }
  }

  /** Single column row converter. */
  private static class SingleColumnRowConverter extends RowConverter {
    private final CsvFieldType fieldType;
    private final int fieldIndex;

    private SingleColumnRowConverter(CsvFieldType fieldType, int fieldIndex) {
      this.fieldType = fieldType;
      this.fieldIndex = fieldIndex;
    }

    public Object convertRow(String[] strings) {
      return convert(fieldType, strings[fieldIndex]);
    }
  }
}

// End CsvEnumerator.java
