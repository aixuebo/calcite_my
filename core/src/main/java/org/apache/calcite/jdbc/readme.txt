一、重点学习入口
CalcitePrepareImpl -- prepareSql -- prepare_


二、Driver
向服务器告知一个驱动,即可以解析一种url前缀,去查询数据库信息
1.基础能力
a.String getConnectStringPrefix();获取driver需要适配的url前缀
return "jdbc:calcite:"
b.boolean acceptsURL(String url) 校验是否url是合法的以xxx前缀开头的
    return url.startsWith(getConnectStringPrefix());
c.DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info) 返回driver需要的PropertyInfo信息
d.DriverVersion getDriverVersion() Driver的元数据信息,包括版本号等信息

2.核心能力
void register() 注册,系统就知道如何解析了
DriverManager.registerDriver(this);

3.高级能力
a.AvaticaFactory createFactory() 创建工厂,工厂可以创建Connection、Statement
b.Handler handler 处理监听拦截器
    void onConnectionInit(AvaticaConnection connection)
    void onConnectionClose(AvaticaConnection connection)
    void onStatementExecute(AvaticaStatement statement,ResultSink resultSink)
    void onStatementClose(AvaticaStatement statement)
c.Meta createMeta(AvaticaConnection connection) 获取连接的元数据信息
    return new CalciteMetaImpl((CalciteConnectionImpl) connection)

4.特殊实现
a.CalcitePrepare prepareFactory = new CalcitePrepareImpl()

b.protected Handler createHandler() {
    return new HandlerImpl() {
      @Override public void onConnectionInit(AvaticaConnection connection_) {
        final String model = connection.config().model();
        if (model != null) {
          try {
            new ModelHandler(connection, model);//找到对应的model对象
          } catch (IOException e) {
            throw new SQLException(e);
          }
        }
        connection.init();//调用连接的init方法
      }
    };
  }

c.Connection connect(String url, java.util.Properties info) throws SQLException;
确保acceptsURL(url) = true校验通过。
Properties info2 = ConnectStringParser.parse(urlSuffix, info) 通过url获取配置信息
AvaticaConnection connection = factory.newConnection(this, factory, url, info2); 创建连接,返回说明连接成功
handler.onConnectionInit(connection); 处理事件,会调用connection的init方法。
return connection;

d.利用schema信息,说明已经有连接成功了,则直接调用工厂方法创建连接对象
CalciteConnection connect(CalciteRootSchema rootSchema,JavaTypeFactory typeFactory) {
      return (CalciteConnection) ((CalciteFactory) factory).newConnection(this.driver, factory,
      "jdbc:calcite:", new Properties(),rootSchema, typeFactory);
}

三、CalciteSchema和CalciteRootSchema
定义了schema信息。属于基础设置
Schema包含父Schema、SchemaName、对应的表集合、对应的function集合信息。


四、AvaticaFactory 工厂对象
虽然是工厂对象,但driver生产connect，connect生产Statement也是有依赖规则的。

a.创建连接
AvaticaConnection newConnection(
      UnregisteredDriver driver,
      AvaticaFactory factory,
      String url,
      Properties info);
  return new CalciteConnectionImpl(driver, factory, url, info, rootSchema, typeFactory);

b.AvaticaStatement newStatement(AvaticaConnection connection,
      @Nullable Meta.StatementHandle h, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) ;
return new CalciteStatement(connection,h,resultSetType, resultSetConcurrency,resultSetHoldability);

c.AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection,
      @Nullable Meta.StatementHandle h, Meta.Signature signature,
      int resultSetType, int resultSetConcurrency, int resultSetHoldability);

return CalcitePreparedStatement(connection, h, signature, resultSetType, resultSetConcurrency,resultSetHoldability);


d.返回ResultSet结果集的元数据信息
  ResultSetMetaData newResultSetMetaData(AvaticaStatement statement,Meta.Signature signature)
  new AvaticaResultSetMetaData(statement, null, signature);


e.返回数据库级别的元数据信息
AvaticaDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection);
 return new AvaticaDatabaseMetaData(CalciteConnectionImpl）

f.返回结果集
AvaticaResultSet newResultSet(AvaticaStatement statement,
        Meta.Signature signature, TimeZone timeZone, Iterable<Object> iterable);
ResultSetMetaData metaData = newResultSetMetaData(statement, signature);
CalcitePrepare.CalciteSignature calciteSignature = (CalcitePrepare.CalciteSignature) signature;
return new CalciteResultSet(statement, calciteSignature, metaData, timeZone,iterable);

五、Connection 可以获取自身连接信息、创建Statement/PreparedStatement、获取数据库元数据
java.util.sql接口
   String setSchema/getSchema()
   String setCatalog/getCatalog()
   DatabaseMetaData getMetaData()
   Statement createStatement()
   Statement createStatement(int resultSetType, int resultSetConcurrency,int resultSetHoldability)
   Statement createStatement(int resultSetType, int resultSetConcurrency)
   PreparedStatement prepareStatement(String sql)
   PreparedStatement prepareStatement(String sql, String columnNames[])
   PreparedStatement prepareStatement(String sql, int resultSetType,int resultSetConcurrency)
   PreparedStatement prepareStatement(String sql, int resultSetType,int resultSetConcurrency, int resultSetHoldability)

   void setAutoCommit(boolean autoCommit)
   void commit()
   void rollback()
   void close()


特别说明:
prepareStatement(String sql, int resultSetType,int resultSetConcurrency, int resultSetHoldability)

1.参数resultSetType:
ResultSet的数据集合,在客户端是一个游标,使用next的方式一条一条信息读取。而游标本身是可以向上读取的,也可以向下读取.因此有不同的取值参数。

a.ResultSet.TYPE_FORWARD_ONLY:
默认的cursor 类型，仅仅支持结果集forward ，不支持backforward ，random ，last ，first 等操作。 此时性能最佳,因为不需要缓存数据,用完销毁,因此查询1000万条数据的时候，也不会出现OOM问题。
b.ResultSet.TYPE_SCROLL_INSENSITIVE:
支持结果集backforward ，random ，last ，first 等操作，对其它session 对数据库中数据做出的更改是不敏感的。
实现方法：从数据库取出数据后，会把全部数据缓存到cache 中，对结果集的后续操作，是操作的cache 中的数据，数据库中记录发生变化后，不影响cache 中的数据，所以ResultSet 对结果集中的数据是INSENSITIVE(数据集变更不敏感) 的。
c.ResultSet.TYPE_SCROLL_SENSITIVE
支持结果集backforward ，random ，last ，first 等操作，对其它session 对数据库中数据做出的更改是敏感的，即其他session 修改了数据库中的数据，会反应到本结果集中。
实现方法：从数据库取出数据后，不是把全部数据缓存到cache 中，而是把每条数据的rowid 缓存到cache 中，对结果集后续操作时，是根据rowid 再去数据库中取数据。所以数据库中记录发生变化后，通过ResultSet 取出的记录是最新的。
但insert 和delete 操作不会影响到ResultSet ，因为insert 数据的rowid 不在ResultSet 取出的rowid 中，所以insert 的数据对ResultSet 是不可见的，
而delete 数据的rowid 依旧在ResultSet 中，所以ResultSet 仍可以取出被删除的记录（ 因为一般数据库的删除是标记删除，不是真正在数据库文件中删除 ）。

2.参数resultSetConcurrency:
ResultSet.CONCUR_READ_ONLY 在ResultSet中的数据记录是只读的，不可以修改
ResultSet.CONCUR_UPDATABLE 在ResultSet中的数据记录可以任意修改，然后更新到数据库，可以插入，删除，修改。

3.参数resultSetHoldability:
ResultSet.HOLD_CURSORS_OVER_COMMIT: 在事务commit 或rollback 后，ResultSet 仍然可用。
ResultSet.CLOSE_CURSORS_AT_COMMIT: 在事务commit 或rollback 后，ResultSet 被关闭。



扩展信息
   Properties getProperties(); 连接属性信息
   SchemaPlus getRootSchema(); 连接器对应的schema对象
   JavaTypeFactory getTypeFactory(); 连接器对应的字段类型与sql类型映射关系
   void getSchema/setSchema(String schema) 设置连接器连接的是哪一个数据库schema
   CalciteConnectionConfig config();连接属性信息




