一、各种表的价值与关系
1.CsvSchema
定义schema的文件
Map<String, Table> getTableMap() 获取每一个表名与表对象
2.CsvTable 只描述了 table的字段类型。以及 对应为文件file数据源是什么。
3.CsvFieldType 描述了具体的某一个字段的类型
4.class CsvEnumerator<E> implements Enumerator<E> 真正读取一个csv文件,将一行数据进行格式转换,以及where条件处理
5.具体读取table的内容 --- 底层用4逻辑在处理
new CsvScannableTable(file, null);
new CsvFilterableTable(file, null);
new CsvTranslatableTable(file, null);

二、高级用法
背景是 如果一个表有100个字段,有100万行数据，很明显他不能直接scan扫描。
因此需要动态的创建成表达式,参与规则优化。

1.CsvTranslatableTable 就是最终代表一个table的表。
2.CsvProjectTableScanRule是一个规则，当规则满足时，可以转换成CsvTableScan对象。
3.CsvTableScan对象承接最终的调用接口，调用CsvTranslatableTable创建数据
