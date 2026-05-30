import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import java.time.Duration
import java.util.{Collections, Properties}

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    // 1. 配置消费者属性
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092") // Kafka集群地址
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group") // 消费者组ID[citation:4]
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    // 设置如果没有偏移量可读时，从最早的消息开始消费
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    // 关闭自动提交偏移量，改为手动提交以获得更强的一致性
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    // 2. 创建Kafka消费者实例
    val consumer = new KafkaConsumer[String, String](props)

    // 3. 订阅主题
    val topic = "testtopic"
    consumer.subscribe(Collections.singletonList(topic))
    println(s"消费者已订阅主题 [$topic]，开始监听消息...")

    try {
      while (true) {
        // 4. 轮询拉取消息（超时时间100毫秒）
        // poll()方法是消费消息的核心，会返回一批消息记录[citation:4]
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))

        // 5. 处理收到的每一条消息
        records.forEach { record =>
          println(s"[消费记录] 主题: ${record.topic()}, " +
            s"分区: ${record.partition()}, " +
            s"偏移量: ${record.offset()}, " +
            s"键: ${record.key()}, " +
            s"值: ${record.value()}")
        }

        // 6. 手动同步提交偏移量，确保消息被成功处理[citation:4]
        if (!records.isEmpty) {
          consumer.commitSync()
          println(s"已成功处理并提交 ${records.count()} 条消息的偏移量。")
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // 7. 关闭消费者，释放资源
      consumer.close()
      println("消费者已关闭。")
    }
  }
}