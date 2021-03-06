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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Avatica Remote JDBC driver.
 */
public class Driver extends UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:avatica:remote:";

  static {
    new Driver().register();
  }

  public Driver() {
    super();
  }

  @Override protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        Driver.class,
        "org-apache-calcite-jdbc.properties",
        "Avatica Remote JDBC Driver",
        "unknown version",
        "Avatica",
        "unknown version");
  }

  //连接的属性集合
  @Override protected Collection<ConnectionProperty> getConnectionProperties() {
    final List<ConnectionProperty> list = new ArrayList<ConnectionProperty>();
    Collections.addAll(list, BuiltInConnectionProperty.values());
    Collections.addAll(list, AvaticaRemoteConnectionProperty.values());
    return list;
  }

  @Override public Meta createMeta(AvaticaConnection connection) {
    final ConnectionConfig config = connection.config();
    final Service.Factory metaFactory = config.factory();
    final Service service;
    if (metaFactory != null) {//本地创建服务对象
      service = metaFactory.create(connection);
    } else if (config.url() != null) {//通过URL远程获取服务对象
      final URL url;
      try {
        url = new URL(config.url());
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
      service = new RemoteService(url);
    } else {//通过json方式mock服务对象
      service = new MockJsonService(Collections.<String, String>emptyMap());
    }
    return new RemoteMeta(connection, service);
  }
}

// End Driver.java
