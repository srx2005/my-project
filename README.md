# Kafka 消息队列实验项目

## 项目概述与目的

本项目是一个基于 Apache Kafka 的消息队列实验项目，用于学习和实践 Kafka 消息的生产与消费。项目包含 Kafka 生产者和消费者的完整实现，模拟传感器数据的实时发送和消费场景。

### 主要功能模块

1. **Kafka 生产者**：模拟传感器数据生成，以异步方式发送到 Kafka 主题
2. **Kafka 消费者**：订阅指定主题，实时接收并处理消息，支持手动提交偏移量

## 技术架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
├─────────────────┬───────────────────────────────────────────┤
│  KafkaProducer  │           KafkaConsumer                   │
│  (Data Source)  │           (Message Processing)           │
└────────┬────────┴──────────────────────┬────────────────────┘
         │                               │
         └───────────────────────────────┼────────────────────┘
                                         │
┌────────────────────────────────────────┴─────────────────────┐
│                    Apache Kafka Cluster                      │
│                    (Message Queue System)                    │
└──────────────────────────────────────────────────────────────┘
```

### 组件说明

| 组件 | 作用 | 版本 |
| --- | --- | --- |
| Apache Kafka | 消息队列，负责数据缓冲和传输 | 3.8.1 |
| Scala | 主要开发语言 | 2.12.10 |
| Maven | 项目构建工具 | 3.x |

## 详细的安装与配置说明

### 环境要求

- **JDK**: 8 或更高版本
- **Scala**: 2.12.x
- **Maven**: 3.6+
- **Kafka**: 3.8.1

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

#### 4. 项目依赖安装

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

## 使用示例

### 1. 启动 Kafka 服务

```bash
# 进入 Kafka 目录
cd kafka_2.13-3.8.1

# 启动 ZooKeeper (Kafka 3.8+ 内置 ZooKeeper)
bin/zookeeper-server-start.sh config/zookeeper.properties

# 新开终端，启动 Kafka
bin/kafka-server-start.sh config/server.properties
```

### 2. 创建主题

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

### 3. 运行 Kafka 生产者

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

### 4. 运行 Kafka 消费者

新开终端，运行消费者：

```bash
mvn exec:java -Dexec.mainClass="KafkaConsumer"
```

消费者会实时监听并消费消息，显示每条消息的详细信息。

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

#### 3. 依赖下载失败

**问题**：Maven 依赖下载超时或失败

**解决方案**：
```bash
# 配置 Maven 镜像源（修改 settings.xml）
# 或使用本地 Maven 仓库
mvn clean install -Dmaven.repo.local=/path/to/local/repo
```

#### 4. 内存不足错误

**问题**：`OutOfMemoryError`

**解决方案**：
- 增加 JVM 内存：`export MAVEN_OPTS="-Xmx4g -Xms2g"`

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
docs: update README with troubleshooting guide
```

### 测试

运行测试：

```bash
mvn test
```

## 项目结构

```
Demo/
├── src/
│   └── main/
│       ├── java/
│       │   └── org/example/
│       │       └── App.java              # Java 示例程序
│       ├── scala/
│       │   ├── KafkaProducer.scala       # Kafka 生产者
│       │   └── KafkaConsumer.scala       # Kafka 消费者
│       └── resources/
├── pom.xml                               # Maven 依赖配置
├── .gitignore                            # Git 忽略配置
└── README.md                             # 项目说明文档
```

## 技术栈

| 技术 | 版本 | 说明 |
| --- | --- | --- |
| Java | 8+ | 基础编程语言 |
| Scala | 2.12.10 | 主要开发语言 |
| Apache Kafka | 3.8.1 | 消息队列系统 |
| Maven | 3.x | 项目构建工具 |

## 许可证

本项目仅供学习和实验使用。

## 联系方式

如有问题或建议，请通过 GitHub Issues 联系。
