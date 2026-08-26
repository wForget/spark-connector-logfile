# spark-connector-logfile

一个基于 Spark DataSource V2 Catalog API 的只读日志连接器。它把指定目录中的日志文件暴露为 Spark SQL 表，并自动补充 `dt`、`hour` 和 `app_id` 分区列，适合直接使用 SQL 分析 Spark Event Log 或其他按应用组织的日志。

## 功能特性

- 支持 `json`、`csv`、`text` 和 Hadoop `TFile` 格式
- 支持根目录中的平铺日志文件，以及递归扫描子目录
- 识别 Spark 滚动事件日志目录 `eventlog_v2_<app_id>`
- 根据每个日志文件的修改时间生成 `dt` 和 `hour` 分区列
- 对 `dt`、`hour`、`app_id` 下推过滤条件，在读取前裁剪文件
- JSON/CSV 可选 schema 推断；默认只读取 `value` 字段
- 透传 Spark 文件格式读取参数及 `hadoop.*` 配置
- 兼容 Spark 3.5 / Scala 2.12 和 Spark 4.2 / Scala 2.13

## 兼容性

| Maven Profile | Spark | Scala | 最低 Java 版本 | 默认 |
| --- | --- | --- | --- | --- |
| `spark-3.5` | 3.5.7 | 2.12.18 | 8 | 否 |
| `spark-4.2` | 4.2.0 | 2.13.18 | 17 | 是 |

构建和运行 `spark-3.5` Profile 需要 JDK 8 或更高版本，`spark-4.2` Profile 需要 JDK 17 或更高版本；同时应使用与目标 Spark 发行版一致的 Scala 版本。Spark 和 Scala 依赖以 `provided` 方式打包，不会包含在连接器 JAR 中。

## 构建

构建默认的 Spark 4.2 版本：

```bash
mvn clean package
```

构建指定版本：

```bash
mvn -Pspark-3.5 clean package
mvn -Pspark-4.2 clean package
```

产物路径分别为：

```text
target/spark-3.5/spark-connector-logfile-1.0-SNAPSHOT-spark-3.5_2.12.jar
target/spark-4.2/spark-connector-logfile-1.0-SNAPSHOT-spark-4.2_2.13.jar
```

## 快速开始

以 Spark 4.2 为例，启动 `spark-sql` 并注册 Catalog：

```bash
spark-sql \
  --jars target/spark-4.2/spark-connector-logfile-1.0-SNAPSHOT-spark-4.2_2.13.jar \
  --conf spark.sql.catalog.logs=cn.wangz.spark.connector.logfile.LogFileCatalog \
  --conf spark.sql.catalog.logs.logDir=file:///data/spark-events \
  --conf spark.sql.catalog.logs.fileFormat=json
```

随后可以像查询普通表一样读取日志：

```sql
SELECT value, dt, hour, app_id
FROM logs.default.spark_log_file
WHERE dt = '2026-08-26'
  AND hour >= '09'
  AND app_id = 'application_123';
```

也可以在代码中配置：

```scala
val spark = SparkSession.builder()
  .config(
    "spark.sql.catalog.logs",
    "cn.wangz.spark.connector.logfile.LogFileCatalog")
  .config("spark.sql.catalog.logs.logDir", "hdfs:///spark-history")
  .config("spark.sql.catalog.logs.fileFormat", "json")
  .config("spark.sql.catalog.logs.inferSchema", "true")
  .getOrCreate()

val events = spark.table("logs.default.spark_log_file")
events.filter("app_id = 'application_123'").show(false)
```

Catalog 默认只暴露 `default` 命名空间中的一张表：

```text
logs.default.spark_log_file
```

可通过 `tableName` 参数修改表名。

## 配置项

所有 Catalog 参数都使用以下前缀：

```text
spark.sql.catalog.<catalog_name>.<option>
```

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `logDir` | 无 | 日志根目录，必填；支持 Hadoop `Path` 可识别的 URI |
| `fileFormat` | `json` | 日志格式：`json`、`csv`、`text` 或 `tfile`，大小写不敏感 |
| `inferSchema` | `false` | 是否为 JSON/CSV 推断 schema；其他格式忽略该参数 |
| `tableName` | `spark_log_file` | Catalog 暴露的表名 |
| `partitionTimeZone` | `spark.sql.session.timeZone` | 将文件修改时间转换为 `dt`、`hour` 时使用的时区 |
| `hadoop.<key>` | 无 | 写入读取任务 Hadoop Configuration 的配置，例如 `hadoop.fs.s3a.endpoint` |

其他参数会继续传给对应的 Spark 文件读取器。例如：

```text
spark.sql.catalog.logs.header=true
spark.sql.catalog.logs.multiLine=false
spark.sql.catalog.logs.ignoreCorruptFiles=true
spark.sql.catalog.logs.wholetext=true
```

通过 DataFrameReader 指定的非结构性选项会覆盖 Catalog 同名选项，参数名大小写不敏感：

```scala
spark.read
  .option("wholetext", "true")
  .option("partitionTimeZone", "Asia/Shanghai")
  .table("logs.default.spark_log_file")
```

`logDir`、`fileFormat` 和 `inferSchema` 决定表的位置或 schema，不能在单次读取时改成不同的值。

## 表结构

默认结构如下：

| 列名 | 类型 | 可空 | 来源 |
| --- | --- | --- | --- |
| `value` | `string` | 是 | 日志内容或源数据中的 `value` 字段 |
| `dt` | `string` | 否 | 文件修改时间对应的日期，格式为 `yyyy-MM-dd` |
| `hour` | `string` | 否 | 文件修改时间对应的小时，格式为 `HH` |
| `app_id` | `string` | 否 | 根据日志根目录下的文件或目录名称提取 |

启用 `inferSchema=true` 后，JSON/CSV 的数据列由 Spark 推断，并在末尾追加三个分区列。源数据不能包含与 `dt`、`hour`、`app_id` 同名（大小写不敏感）的字段，否则连接器会拒绝加载表。

## 日志目录规则

### 平铺文件

根目录下每个文件被视为一个应用日志，文件名即 `app_id`：

```text
/data/logs/
├── application_001
└── application_002.gz
```

对应的 `app_id` 为 `application_001` 和 `application_002`。提取 ID 时会去掉已知压缩后缀：`.lz4`、`.snappy`、`.zstd`、`.lzf`、`.gz`、`.bz2`。

### 普通目录

根目录下的普通目录名作为 `app_id`，其中的文件会被递归发现：

```text
/data/logs/application_001/year/month/events.json
```

该文件的 `app_id` 为 `application_001`。

### Spark 滚动事件日志目录

对于 `eventlog_v2_<app_id>` 目录，连接器会递归读取名称以 `events_` 开头的文件：

```text
/data/logs/eventlog_v2_application_001/
├── events_1
├── events_2
└── appstatus_application_001
```

这里只有 `events_1`、`events_2` 会被读取，`app_id` 为 `application_001`。

以下路径会被忽略：

- 名称以 `.` 或 `_` 开头的文件和目录
- 名称以 `.inprogress` 结尾的文件和目录
- 目录符号链接

不存在或为空的 `logDir` 返回空结果，不会报错。

## 分区过滤

连接器可对 `dt`、`hour` 和 `app_id` 下推以下过滤条件：

- `=`、`IN`
- `>`、`>=`、`<`、`<=`
- 字符串前缀匹配（Spark `startsWith`）

分区值均为固定格式字符串，因此日期和小时的字典序与时间顺序一致：

```sql
SELECT app_id, count(*)
FROM logs.default.spark_log_file
WHERE dt BETWEEN '2026-08-01' AND '2026-08-31'
  AND hour IN ('08', '09', '10')
GROUP BY app_id;
```

## 测试

分别运行两套兼容性测试：

```bash
mvn -Pspark-3.5 clean test
mvn -Pspark-4.2 clean test
```

运行 Spark 4.2 测试前，请确认 `mvn -version` 显示 Maven 正在使用 JDK 17 或更高版本。

测试覆盖 Catalog 行为、四种文件格式、schema 推断、递归文件发现、分区时间与过滤、损坏/缺失文件处理，以及 Spark/Scala/Java 兼容性基线。

## 限制

- 仅支持批量读取，不支持写入和 Structured Streaming。
- Catalog 只支持根命名空间和 `default` 命名空间。
- 当前按文件创建输入分区，不会拆分单个大文件。
- `dt` 和 `hour` 来自文件修改时间，而不是日志内容中的事件时间。
