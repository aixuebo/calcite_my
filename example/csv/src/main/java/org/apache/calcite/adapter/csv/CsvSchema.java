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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Map;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class CsvSchema extends AbstractSchema {
  final File directoryFile;
  private final CsvTable.Flavor flavor;

  /**
   * Creates a CSV schema.
   *
   * @param directoryFile Directory that holds {@code .csv} files
   * @param flavor     Whether to instantiate flavor tables that undergo
   *                   query optimization
   */
  public CsvSchema(File directoryFile, CsvTable.Flavor flavor) {
    super();
    this.directoryFile = directoryFile;
    this.flavor = flavor;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or the original string.
   *  去除后缀
   **/
  private static String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    return trimmed != null ? trimmed : s;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or null.
   *  去除后缀
   **/
  private static String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  /**
   * 假设文件是aaa.json，则解析后表名是aaa，解析的内容是table对象
   */
  @Override protected Map<String, Table> getTableMap() {
    // Look for files in the directory ending in ".csv", ".csv.gz", ".json",
    // ".json.gz".
    //查找以下文件".csv", ".csv.gz", ".json",".json.gz".
    File[] files = directoryFile.listFiles(
        new FilenameFilter() {
          public boolean accept(File dir, String name) {
            final String nameSansGz = trim(name, ".gz");
            return nameSansGz.endsWith(".csv")
                || nameSansGz.endsWith(".json");
          }
        });
    if (files == null) {
      System.out.println("directory " + directoryFile + " not found");
      files = new File[0];
    }
    // Build a map from table name to table; each file becomes a table.
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (File file : files) {
      String tableName = trim(file.getName(), ".gz");
      final String tableNameSansJson = trimOrNull(tableName, ".json");
      if (tableNameSansJson != null) {//说明是json文件
        JsonTable table = new JsonTable(file);
        builder.put(tableNameSansJson, table);
        continue;
      }
      //说明是csv文件
      tableName = trim(tableName, ".csv");

      final Table table = createTable(file);
      builder.put(tableName, table);
    }
    return builder.build();
  }

  /** Creates different sub-type of table based on the "flavor" attribute. */
  private Table createTable(File file) {
    switch (flavor) {
    case TRANSLATABLE:
      return new CsvTranslatableTable(file, null);
    case SCANNABLE:
      return new CsvScannableTable(file, null);
    case FILTERABLE:
      return new CsvFilterableTable(file, null);
    default:
      throw new AssertionError("Unknown flavor " + flavor);
    }
  }
}

// End CsvSchema.java
