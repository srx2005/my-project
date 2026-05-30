# Spark-Kafka 实验项目

## 项目概述

本项目是一个基于 Apache Spark 和 Apache Kafka 的大数据实验项目，主要用于学习和实践流式数据处理技术。项目包含 Kafka 消息消费者实现，展示了如何使用 Scala 编写 Kafka 消费者程序来实时处理流式数据。

## 功能说明

### 核心功能

1. **Kafka 消息消费**：实现了完整的 Kafka 消费者，支持：
   - 配置消费者属性（bootstrap servers、group id、序列化器等）
   - 订阅指定主题并监听消息
   - 轮询拉取消息并处理
   - 手动提交偏移量以保证消息处理的可靠性

2. **Spark 集成**：项目配置了 Spark 相关依赖，支持后续扩展为 Spark Streaming 或 Structured Streaming 应用

## 技术栈

| 技术 | 版本 | 说明 |
| --- | --- | --- |
| Java | 8+ | 主编程语言 |
| Scala | 2.12.10 | Kafka消费者实现语言 |
| Apache Spark | 3.3.4 | 大数据处理框架 |
| Apache Kafka | 3.8.1 | 消息队列系统 |
| Apache Hadoop | 3.3.6 | 分布式存储支持 |
| Maven | 3.x | 项目构建工具 |

## 项目结构

```
Demo/
├── src/
│   └── main/
│       ├── java/
│       │   └── org/example/
│       │       └── App.java          # Java 示例程序
│       ├── scala/
│       │   └── KafkaConsumer.scala   # Kafka 消费者实现
│       └── resources/
│           └── data.txt              # 示例数据文件
├── checkpoint/                       # Spark checkpoint 目录
├── pom.xml                          # Maven 依赖配置
├── .gitignore                       # Git 忽略配置
└── README.md                        # 项目说明文档
```

## 安装步骤

### 环境要求

- JDK 8 或更高版本
- Maven 3.x
- Kafka 3.8.1（需要单独部署）

### 依赖安装

```bash
# 进入项目目录
cd Demo

# 使用 Maven 安装依赖
mvn clean install
```

## 使用方法

### 1. 启动 Kafka

在运行消费者之前，需要确保 Kafka 服务已启动：

```bash
# 启动 ZooKeeper（Kafka 3.8+ 内置 ZooKeeper）
# 或者使用独立的 ZooKeeper

# 启动 Kafka
bin/kafka-server-start.sh config/server.properties
```

### 2. 创建主题

```bash
bin/kafka-topics.sh --create --topic testtopic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. 运行 Kafka 消费者

```bash
# 使用 Maven 运行
mvn exec:java -Dexec.mainClass="org.example.KafkaConsumer"

# 或者编译后运行
mvn compile
mvn exec:java -Dexec.mainClass="org.example.KafkaConsumer"
```

### 4. 发送测试消息

```bash
bin/kafka-console-producer.sh --topic testtopic --bootstrap-server localhost:9092
# 然后输入消息并回车
```

### 5. 运行 Java 示例

```bash
mvn exec:java -Dexec.mainClass="org.example.App"
```

## 核心代码说明

### KafkaConsumer.scala 关键配置

```scala
// 消费者配置
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")  // 从最早消息开始
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")    // 手动提交偏移量
```

### 消息处理流程

1. **配置属性**：设置 Kafka 集群地址、消费者组ID、序列化方式等
2. **创建消费者**：实例化 KafkaConsumer 对象
3. **订阅主题**：订阅指定的 Kafka 主题
4. **轮询消息**：使用 `poll()` 方法拉取消息
5. **处理消息**：遍历消息记录并处理
6. **提交偏移量**：手动同步提交偏移量确保可靠性

## 配置说明

### Kafka 配置项

| 配置项 | 值 | 说明 |
| --- | --- | --- |
| BOOTSTRAP_SERVERS_CONFIG | localhost:9092 | Kafka 集群地址 |
| GROUP_ID_CONFIG | test-consumer-group | 消费者组ID |
| KEY_DESERIALIZER_CLASS_CONFIG | StringDeserializer | Key 反序列化器 |
| VALUE_DESERIALIZER_CLASS_CONFIG | StringDeserializer | Value 反序列化器 |
| AUTO_OFFSET_RESET_CONFIG | earliest | 无偏移量时从最早开始 |
| ENABLE_AUTO_COMMIT_CONFIG | false | 关闭自动提交 |

## 注意事项

1. **Kafka 服务**：运行前确保 Kafka 服务在 localhost:9092 端口可用
2. **主题创建**：需要提前创建 `testtopic` 主题
3. **依赖版本**：确保 Maven 依赖版本与实际环境匹配
4. **网络配置**：如果 Kafka 部署在远程服务器，需要修改 bootstrap.servers 配置

## 扩展功能

本项目可扩展为以下功能：
- Spark Streaming 实时流处理
- Structured Streaming 结构化流处理
- 消息持久化到 HDFS 或数据库
- 消息处理结果可视化

## 许可证

本项目仅供学习和实验使用。