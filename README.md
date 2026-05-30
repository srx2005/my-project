# Spark-Kafka 实验项目

## 项目概述与目的

本项目是一个完整的 Apache Spark 和 Apache Kafka 大数据实验项目，用于学习和实践流式数据处理、RDD操作、Spark SQL等技术。项目包含多个示例程序，展示了大数据处理的典型应用场景。

### 主要功能模块

1. **Kafka 消息队列**：包含生产者和消费者实现，模拟传感器数据的实时发送和消费
2. **Spark RDD 操作**：学生成绩数据分析，包括统计学生人数、课程数、平均分等
3. **Spark SQL 与 MySQL 集成**：数据读写 MySQL 数据库，执行复杂查询和统计
4. **Spark Streaming**：实时词频统计，使用状态管理功能维护累积计数

## 技术架构与组件关系

```
┌─────────────────────────────────────────────────────────────────┐
│                         Application Layer                       │
├─────────────────┬─────────────────┬─────────────────┬───────────┤
│  KafkaProducer  │  KafkaConsumer  │      RDD       │   SQL     │
│  (Data Source)  │  (Data Sink)    │  (Data Processing)  │ (SQL)  │
└────────┬────────┴────────┬────────┴────────┬────────┴─────┬─────┘
         │                 │                 │              │
         └─────────────────┴────────┬────────┴──────────────┘
                                    │
┌───────────────────────────────────┴─────────────────────────────┐
│                     Spark Core & Streaming                       │
│  (SparkContext, SparkSession, StreamingContext)                  │
└───────────────────────────────────┬─────────────────────────────┘
                                    │
         ┌──────────────────────────┼──────────────────────────┐
         │                          │                          │
┌────────▼─────────┐      ┌────────▼─────────┐       ┌────────▼─────────┐
│    Apache Kafka  │      │     MySQL        │       │   HDFS/Local FS  │
│  (Message Queue) │      │   (Database)     │       │  (Data Storage)  │
└──────────────────┘      └──────────────────┘       └──────────────────┘
```

### 组件说明

| 组件 | 作用 | 版本 |
| --- | --- | --- |
| Apache Kafka | 消息队列，负责数据缓冲和传输 | 3.8.1 |
| Apache Spark | 大数据处理核心框架 | 3.3.4 |
| MySQL | 关系型数据库，存储和查询结构化数据 | 8.x |
| Hadoop | 分布式存储支持 | 3.3.6 |

## 详细的安装与配置说明

### 环境要求

- **JDK**: 8 或更高版本
- **Scala**: 2.12.x
- **Maven**: 3.6+
- **Kafka**: 3.8.1
- **Spark**: 3.3.4
- **MySQL**: 8.0+ (可选，用于 SQL 模块)

### 安装步骤

#### 1. 安装 JDK

```bash
# 下载并安装 JDK 8+
# 设置 JAVA_HOME 环境变量
export JAVA_HOME=/path/to/java
export PATH=$JAVA_HOME/bin:$PATH

# 验证安装
java -version
```

#### 2. 安装 Maven

```bash
# 下载并解压 Maven
# 设置 MAVEN_HOME 环境变量
export MAVEN_HOME=/path/to/maven
export PATH=$MAVEN_HOME/bin:$PATH

# 验证安装
mvn -version
```

#### 3. 安装 Kafka

```bash
# 下载 Kafka 3.8.1
wget https://archive.apache.org/dist/kafka/3.8.1/kafka_2.13-3.8.1.tgz
tar -xzf kafka_2.13-3.8.1.tgz
cd kafka_2.13-3.8.1

# 配置 server.properties (可选)
vi config/server.properties
```

#### 4. 安装 Spark

```bash
# 下载 Spark 3.3.4
wget https://archive.apache.org/dist/spark/spark-3.3.4/spark-3.3.4-bin-hadoop3.tgz
tar -xzf spark-3.3.4-bin-hadoop3.tgz
cd spark-3.3.4-bin-hadoop3

# 设置 SPARK_HOME
export SPARK_HOME=/path/to/spark
export PATH=$SPARK_HOME/bin:$PATH
```

#### 5. 安装 MySQL (可选)

```bash
# 安装 MySQL Server 8.0+
# 启动 MySQL 服务
# 创建数据库 spark_db
CREATE DATABASE spark_db;
```

#### 6. 项目依赖安装

```bash
# 进入项目目录
cd Demo

# 使用 Maven 安装依赖
mvn clean install
```

## 配置要求与环境变量

### 环境变量配置

创建 `config/env.sh` 或直接设置以下环境变量：

```bash
# Kafka 配置
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC="testtopic"

# MySQL 配置
export MYSQL_URL="jdbc:mysql://localhost:3306/spark_db?useSSL=false&serverTimezone=UTC"
export MYSQL_USERNAME="root"
export MYSQL_PASSWORD="your_password"

# Spark 配置
export SPARK_MASTER="local[*]"
export SPARK_CHECKPOINT_DIR="checkpoint"
```

### 配置文件说明

#### Kafka 配置项

**生产者配置** (`KafkaProducer.scala`):

| 配置项 | 值 | 说明 |
| --- | --- | --- |
| `bootstrap.servers` | `localhost:9092` | Kafka 集群地址 |
| `key.serializer` | `StringSerializer` | Key 序列化器 |
| `value.serializer` | `StringSerializer` | Value 序列化器 |
| `acks` | `all` | 发送确认机制（所有副本确认） |
| `retries` | `3` | 重试次数 |

**消费者配置** (`KafkaConsumer.scala`):

| 配置项 | 值 | 说明 |
| --- | --- | --- |
| `bootstrap.servers` | `localhost:9092` | Kafka 集群地址 |
| `group.id` | `test-consumer-group` | 消费者组ID |
| `key.deserializer` | `StringDeserializer` | Key 反序列化器 |
| `value.deserializer` | `StringDeserializer` | Value 反序列化器 |
| `auto.offset.reset` | `earliest` | 无偏移量时从最早开始 |
| `enable.auto.commit` | `false` | 关闭自动提交 |

#### MySQL 配置

在 `SQL.scala` 中配置：

```scala
val jdbcUrl = "jdbc:mysql://localhost:3306/spark_db?useSSL=false&serverTimezone=UTC"
val jdbcUsername = "root"
val jdbcPassword = "your_password"
```

**安全提示**：生产环境请使用环境变量或配置文件管理敏感信息，不要硬编码密码。

## 使用示例

### 1. Kafka 应用

#### 1.1 启动 Kafka 服务

```bash
# 进入 Kafka 目录
cd kafka_2.13-3.8.1

# 启动 ZooKeeper (Kafka 3.8+ 内置 ZooKeeper)
bin/zookeeper-server-start.sh config/zookeeper.properties

# 新开终端，启动 Kafka
bin/kafka-server-start.sh config/server.properties
```

#### 1.2 创建主题

```bash
# 创建 testtopic 主题
bin/kafka-topics.sh --create \
  --topic testtopic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# 查看主题列表
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

#### 1.3 运行 Kafka 生产者

```bash
# 使用 Maven 运行
mvn exec:java -Dexec.mainClass="KafkaProducer"

# 或编译后运行
mvn compile
mvn exec:java -Dexec.mainClass="KafkaProducer"
```

生产者会每秒发送一条模拟传感器数据，格式为：
```
sensor-{id},{timestamp},{temperature}
```

#### 1.4 运行 Kafka 消费者

新开终端，运行消费者：

```bash
mvn exec:java -Dexec.mainClass="KafkaConsumer"
```

消费者会实时监听并消费消息，显示每条消息的详细信息。

---

### 2. RDD 学生成绩分析

#### 2.1 准备数据文件

在项目根目录创建 `data.txt` 文件，格式如下：

```
张三 数学 85
张三 英语 90
李四 数学 78
李四 英语 82
王五 数学 95
王五 英语 88
```

#### 2.2 运行 RDD 程序

```bash
mvn exec:java -Dexec.mainClass="Rdd"
```

输出结果包括：
- 学生人数统计
- 课程数统计
- 总成绩和平均分
- 每名同学的选修课程门数
- 课程的选修人数
- 各门课程的平均分
- 使用累加器统计结果

---

### 3. Spark SQL 与 MySQL 集成

#### 3.1 准备数据文件

创建 `data1.txt` 文件，格式为：

```
1,张三,18,男,计算机科学
2,李四,19,女,软件工程
3,王五,20,男,网络工程
```

#### 3.2 配置 MySQL 连接

在 `SQL.scala` 中修改数据库连接信息（或使用环境变量）。

#### 3.3 运行 SQL 程序

```bash
mvn exec:java -Dexec.mainClass="SQL"
```

程序会：
1. 从文件读取数据创建 DataFrame
2. 将数据写入 MySQL 数据库
3. 从 MySQL 读取数据
4. 执行多种 SQL 查询统计

---

### 4. Spark Streaming 实时词频统计

#### 4.1 启动 Netcat 服务器

```bash
# Linux/Mac
nc -lk 9999

# Windows (使用 ncat)
ncat -lk 9999
```

#### 4.2 运行 Streaming 程序

新开终端：

```bash
mvn exec:java -Dexec.mainClass="com.spark.streaming.WordCountStreaming"
```

#### 4.3 输入文本测试

在 Netcat 终端输入文本，按行发送：

```
hello world
hello spark
spark streaming
```

Streaming 程序会每5秒输出一次词频统计结果，包括累积计数。

## 故障排查指南与常见问题

### 常见问题与解决方案

#### 1. Kafka 连接失败

**问题**：`Connection refused: localhost/127.0.0.1:9092`

**解决方案**：
- 确认 Kafka 服务已启动
- 检查 `server.properties` 中的 `listeners` 配置
- 验证防火墙设置

#### 2. 主题不存在

**问题**：`LEADER_NOT_AVAILABLE` 或主题不存在

**解决方案**：
```bash
# 创建主题
bin/kafka-topics.sh --create --topic testtopic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

#### 3. MySQL 连接失败

**问题**：`Communications link failure`

**解决方案**：
- 确认 MySQL 服务已启动
- 检查连接 URL、用户名、密码
- 验证 MySQL 用户权限

#### 4. Spark Streaming 端口冲突

**问题**：`BindException: Address already in use`

**解决方案**：
- 确认端口 9999 未被占用
- 更换端口号并修改 `WordCountStreaming.scala`
- 检查防火墙设置

#### 5. 依赖下载失败

**问题**：Maven 依赖下载超时或失败

**解决方案**：
```bash
# 配置 Maven 镜像源（修改 settings.xml）
# 或使用本地 Maven 仓库
mvn clean install -Dmaven.repo.local=/path/to/local/repo
```

#### 6. 内存不足错误

**问题**：`OutOfMemoryError`

**解决方案**：
- 增加 JVM 内存：`export MAVEN_OPTS="-Xmx4g -Xms2g"`
- 减少 Spark 分区数
- 使用 `local[2]` 而非 `local[*]`

#### 7. Checkpoint 目录问题

**问题**：Checkpoint 目录不存在或权限问题

**解决方案**：
```bash
# 创建 checkpoint 目录
mkdir -p checkpoint
# 确保有写入权限
chmod 755 checkpoint
```

### 日志调试

启用详细日志：

```bash
# 修改 log4j.properties 或设置日志级别
spark.sparkContext.setLogLevel("DEBUG")
```

### 性能优化建议

1. **Kafka 优化**：
   - 增加主题分区数提高并行度
   - 调整批处理大小和缓冲区

2. **Spark 优化**：
   - 合理设置分区数（`spark.sql.shuffle.partitions`）
   - 使用持久化（`persist()`、`cache()`）
   - 启用自适应查询执行（`spark.sql.adaptive.enabled`）

## 贡献指南与代码规范

### 开发工作流

1. **Fork 仓库**
2. **创建功能分支**
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **编写代码**，遵循以下规范
4. **提交代码**
   ```bash
   git add .
   git commit -m "feat: add new feature"
   ```
5. **推送到远程**
   ```bash
   git push origin feature/your-feature-name
   ```
6. **创建 Pull Request**

### 代码规范

#### Scala 代码规范

- 使用 2 空格缩进
- 类名使用大驼峰（`PascalCase`）
- 方法和变量使用小驼峰（`camelCase`）
- 常量使用全大写下划线分隔（`UPPER_SNAKE_CASE`）
- 添加适当的文档注释

```scala
/**
 * Kafka 生产者示例
 * 模拟传感器数据发送
 */
object KafkaProducer {
  val MAX_RETRIES = 3  // 常量

  def sendMessage(message: String): Unit = {  // 方法
    // 实现
  }
}
```

#### Git 提交信息规范

使用语义化提交信息：

```
<type>(<scope>): <subject>

类型：
- feat: 新功能
- fix: 修复
- docs: 文档更新
- style: 格式调整
- refactor: 重构
- test: 测试
- chore: 构建/工具
```

示例：
```
feat(kafka): add async send with callback
fix(sql): correct MySQL connection config
docs: update README with troubleshooting guide
```

### 代码审查

- 所有 PR 需要至少一人审查
- 确保所有测试通过
- 代码符合项目规范
- 文档同步更新

### 测试

运行测试：

```bash
mvn test
```

## 项目结构

```
Demo/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── org/example/
│   │   │       └── App.java              # Java 示例程序
│   │   ├── scala/
│   │   │   ├── KafkaProducer.scala      # Kafka 生产者
│   │   │   ├── KafkaConsumer.scala      # Kafka 消费者
│   │   │   ├── Rdd.scala                # RDD 学生成绩分析
│   │   │   ├── SQL.scala                # Spark SQL 与 MySQL 集成
│   │   │   └── WordCountStreaming.scala # Spark Streaming 词频统计
│   │   └── resources/
│   └── test/
│       └── java/
│           └── org/example/
│               └── AppTest.java         # 测试文件
├── checkpoint/                          # Spark checkpoint 目录
├── pom.xml                             # Maven 依赖配置
├── .gitignore                          # Git 忽略配置
├── config/                             # 配置文件目录（需创建）
│   └── env.sh                          # 环境变量配置
└── README.md                           # 项目说明文档
```

## 技术栈

| 技术 | 版本 | 说明 |
| --- | --- | --- |
| Java | 8+ | 基础编程语言 |
| Scala | 2.12.10 | 主要开发语言 |
| Apache Spark | 3.3.4 | 大数据处理框架 |
| Apache Kafka | 3.8.1 | 消息队列系统 |
| Apache Hadoop | 3.3.6 | 分布式存储支持 |
| MySQL | 8.0+ | 关系型数据库 |
| Maven | 3.x | 项目构建工具 |

## 许可证

本项目仅供学习和实验使用。

## 联系方式

如有问题或建议，请通过 GitHub Issues 联系。
