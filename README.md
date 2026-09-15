# Spark SQL 读取日志文件

## 背景

`spark-connector-logfile` 是一个基于 Spark DataSource V2 Catalog API 的只读日志连接器。它将日志目录映射为 Spark SQL 表，并根据文件信息添加 `dt`、`hour`、`app_id` 分区列，便于在查询前按日期、小时和应用裁剪文件。

连接器支持 JSON、CSV、Text 和 Hadoop TFile，可用于读取 Spark Event Log 或其他按应用组织的日志文件。

## 打包与配置

### 打包

项目默认构建 Spark 4.2 / Scala 2.13 版本：

```bash
mvn clean package -DskipTests
```

也可以通过 Maven Profile 构建指定版本：

```bash
# Spark 3.5.7 / Scala 2.12 / Java 8+
mvn -Pspark-3.5 clean package -DskipTests

# Spark 4.2.0 / Scala 2.13 / Java 17+
mvn -Pspark-4.2 clean package -DskipTests
```

生成的 JAR 位于 `target/<profile>/`。将其复制到 `$SPARK_HOME/jars`，或在 Spark SQL 中加载：

```sql
ADD JAR hdfs:///path/spark-connector-logfile-1.0-SNAPSHOT-spark-4.2_2.13.jar;
```

Spark 与 Scala 依赖不会打入连接器 JAR，运行时版本应与所选 Profile 一致。

### 配置 Catalog

```properties
spark.sql.catalog.logfile=cn.wangz.spark.connector.logfile.LogFileCatalog
spark.sql.catalog.logfile.logDir=hdfs:///spark-history
spark.sql.catalog.logfile.fileFormat=json
```

所有参数的前缀均为 `spark.sql.catalog.<catalog_name>.`：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `logDir` | 无 | 日志根目录，必填 |
| `fileFormat` | `json` | `json`、`csv`、`text` 或 `tfile` |
| `tableName` | `spark_log_file` | Catalog 暴露的表名 |
| `schema` | 无 | JSON/CSV 的 Spark DDL schema |
| `inferSchema` | `false` | 是否为 JSON/CSV 推断 schema |
| `partitionTimeZone` | `spark.sql.session.timeZone` | 生成 `dt`、`hour` 时使用的时区 |
| `hadoop.<key>` | 无 | 传入 Hadoop Configuration 的配置 |

其他参数会传给对应的 Spark 文件读取器，例如 CSV 的 `header`、JSON 的 `multiLine`、Text 的 `wholetext` 和通用的 `ignoreCorruptFiles`。

## 使用

### 表与分区

Catalog 默认提供以下表：

```text
logfile.default.spark_log_file
```

表包含日志数据列和三个分区列：

| 分区列 | 类型 | 来源 |
| --- | --- | --- |
| `dt` | `string` | 文件修改日期，格式为 `yyyy-MM-dd` |
| `hour` | `string` | 文件修改小时，格式为 `HH` |
| `app_id` | `string` | 日志文件名或日志根目录下的一级目录名 |

查看表、分区并查询日志：

```sql
SHOW TABLES IN logfile;
SHOW PARTITIONS logfile.default.spark_log_file;

SELECT *
FROM logfile.default.spark_log_file
WHERE dt = '2026-08-26'
  AND hour = '09'
  AND app_id = 'application_123';
```

对 `dt`、`hour` 和 `app_id` 的常用比较、`IN` 及前缀过滤会在读取文件前下推。

### Schema

默认数据 schema 为 `value STRING`。JSON/CSV 可以显式配置 schema：

```properties
spark.sql.catalog.logfile.schema=`Event` STRING, `Job ID` LONG
```

也可以设置 `inferSchema=true`，连接器会从最多 100 个日志文件中采样推断。显式 schema 的优先级高于自动推断，且数据 schema 不能包含保留分区列 `dt`、`hour`、`app_id`。

运行时可以为后续查询更新 schema：

```sql
SET spark.sql.catalog.logfile.schema=`Event` STRING, `Job ID` LONG;
```

### Spark Event Log 使用案例

以下案例假设 `logDir` 指向 Spark Event Log 目录。每个案例会按查询字段设置对应的 schema，执行时可根据实际 Spark 版本调整字段类型。
Spark 3.5 和 Spark 4.2 均支持读取以 `.zstd` 结尾的 Zstd compressed Spark Event Log，包括 rolling event log 中由 flush 产生的连续 Zstd frame。

#### 1. 查询任务失败信息

```sql
SET spark.sql.catalog.logfile.schema=
  `Event` STRING,
  `Job ID` LONG,
  `Completion Time` LONG,
  `Job Result` STRUCT<
    `Result`: STRING,
    `Exception`: STRUCT<`Message`: STRING, `Stack Trace`: STRING>
  >;

SELECT
  app_id,
  `Job ID`,
  substring_index(`Job Result`.`Exception`.`Message`, '\n', 1) AS error_message
FROM logfile.default.spark_log_file
WHERE dt = '2022-11-03'
  AND `Event` = 'SparkListenerJobEnd'
  AND `Job Result`.`Result` = 'JobFailed'
LIMIT 10;
```

#### 2. 查询倾斜 Stage

```sql
SET spark.sql.catalog.logfile.schema=
  `Event` STRING,
  `Stage ID` INT,
  `Stage Attempt ID` INT,
  `Task Metrics` STRUCT<`Executor Run Time`: LONG>;

SELECT
  app_id,
  `Stage ID`,
  percentile(`Task Metrics`.`Executor Run Time`, 0.75) AS p75,
  max(`Task Metrics`.`Executor Run Time`) AS max_runtime
FROM logfile.default.spark_log_file
WHERE dt = '2022-11-08'
  AND hour = '10'
  AND `Event` = 'SparkListenerTaskEnd'
  AND `Task Metrics`.`Executor Run Time` IS NOT NULL
GROUP BY app_id, `Stage ID`
ORDER BY max_runtime DESC
LIMIT 100;
```

#### 3. 查询 Task 反序列化耗时

```sql
SET spark.sql.catalog.logfile.schema=
  `Event` STRING,
  `Stage ID` INT,
  `Stage Attempt ID` INT,
  `Task Info` STRUCT<`Task ID`: LONG>,
  `Task Metrics` STRUCT<`Executor Deserialize Time`: LONG>;

SELECT
  app_id,
  `Stage ID`,
  `Stage Attempt ID`,
  `Task Info`.`Task ID` AS task_id,
  `Task Metrics`.`Executor Deserialize Time` AS deserialize_time
FROM logfile.default.spark_log_file
WHERE dt = '2023-04-24'
  AND `Event` = 'SparkListenerTaskEnd'
ORDER BY deserialize_time DESC
LIMIT 50;
```

#### 4. 查询 Kyuubi 应用

```sql
SET spark.sql.catalog.logfile.schema=
  `Event` STRING,
  `Spark Properties` MAP<STRING, STRING>;

SELECT DISTINCT app_id
FROM logfile.default.spark_log_file
WHERE dt = '2023-06-13'
  AND `Event` = 'SparkListenerEnvironmentUpdate'
  AND `Spark Properties`['spark.yarn.tags'] = 'KYUUBI'
LIMIT 10;
```

#### 5. 查询 Kyuubi 倾斜 Stage

```sql
SET spark.sql.catalog.logfile.schema=
  `Event` STRING,
  `Stage ID` INT,
  `Task Metrics` STRUCT<`Executor Run Time`: LONG>,
  `Spark Properties` MAP<STRING, STRING>;

WITH kyuubi_apps AS (
  SELECT DISTINCT app_id
  FROM logfile.default.spark_log_file
  WHERE dt = '2023-06-13'
    AND `Event` = 'SparkListenerEnvironmentUpdate'
    AND `Spark Properties`['spark.yarn.tags'] = 'KYUUBI'
),
skew_stages AS (
  SELECT
    app_id,
    `Stage ID`,
    percentile(`Task Metrics`.`Executor Run Time`, 0.75) AS p75,
    max(`Task Metrics`.`Executor Run Time`) AS max_runtime
  FROM logfile.default.spark_log_file
  WHERE dt = '2023-06-13'
    AND `Event` = 'SparkListenerTaskEnd'
    AND `Task Metrics`.`Executor Run Time` IS NOT NULL
  GROUP BY app_id, `Stage ID`
)
SELECT s.*
FROM kyuubi_apps k
JOIN skew_stages s ON k.app_id = s.app_id
ORDER BY s.max_runtime DESC
LIMIT 100;
```

#### 6. 分析 Stage 数据处理速率

```sql
SET spark.sql.catalog.logfile.schema=
  `Event` STRING,
  `Stage ID` INT,
  `Stage Attempt ID` INT,
  `Task End Reason` STRUCT<`Reason`: STRING>,
  `Task Metrics` STRUCT<
    `Executor Run Time`: LONG,
    `Input Metrics`: STRUCT<
      `Bytes Read`: LONG
    >,
    `Shuffle Read Metrics`: STRUCT<
      `Remote Bytes Read`: LONG,
      `Local Bytes Read`: LONG
    >,
    `Shuffle Write Metrics`: STRUCT<
      `Shuffle Bytes Written`: LONG
    >,
    `Output Metrics`: STRUCT<
      `Bytes Written`: LONG
    >
  >;

WITH tasks AS (
  SELECT
    app_id,
    `Stage ID` AS stage_id,
    `Stage Attempt ID` AS stage_attempt_id,
    `Task Metrics`.`Executor Run Time` AS executor_run_time_ms,
    `Task Metrics`.`Input Metrics`.`Bytes Read` AS input_bytes,
    `Task Metrics`.`Shuffle Read Metrics`.`Remote Bytes Read`
      + `Task Metrics`.`Shuffle Read Metrics`.`Local Bytes Read`
      AS shuffle_read_bytes,
    `Task Metrics`.`Shuffle Write Metrics`.`Shuffle Bytes Written` AS shuffle_write_bytes,
    `Task Metrics`.`Output Metrics`.`Bytes Written` AS output_bytes
  FROM logfile.default.spark_log_file
  WHERE app_id = 'application_123'
    AND `Event` = 'SparkListenerTaskEnd'
    AND `Task End Reason`.`Reason` = 'Success'
    AND `Task Metrics`.`Executor Run Time` IS NOT NULL
),
stage_totals AS (
  SELECT
    app_id,
    stage_id,
    stage_attempt_id,
    COUNT(*) AS task_count,
    SUM(executor_run_time_ms) / 1000.0 AS executor_run_seconds,
    (SUM(input_bytes) + SUM(shuffle_read_bytes)
      + SUM(shuffle_write_bytes) + SUM(output_bytes)) / 1048576.0 AS processed_mib
  FROM tasks
  GROUP BY app_id, stage_id, stage_attempt_id
)
SELECT
  app_id,
  stage_id,
  stage_attempt_id,
  task_count,
  executor_run_seconds,
  processed_mib,
  ROUND(processed_mib / executor_run_seconds, 2)
    AS processed_mib_per_sec
FROM stage_totals
WHERE executor_run_seconds > 0
  AND processed_mib >= 10 * 1024
ORDER BY app_id, stage_id, stage_attempt_id;
```

### 日志目录

- 根目录下的文件以文件名作为 `app_id`，常见压缩后缀会被移除。
- 根目录下的普通目录以目录名作为 `app_id`，目录内文件会被递归读取。
- `eventlog_v2_<app_id>` 目录只读取名称以 `events_` 开头的文件。
- 名称以 `.`、`_` 开头或以 `.inprogress` 结尾的路径会被忽略。

不存在或为空的日志目录返回空结果。连接器仅支持批量读取，不支持写入和 Structured Streaming。
