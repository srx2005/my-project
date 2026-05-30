import org.apache.kafka.clients.producer._
import java.util.Properties
import scala.util.Random
object KafkaProducer {

  def main(args: Array[String]): Unit = {
    // 1. 配置生产者属性
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092") // Kafka集群地址
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // 设置发送确认机制为“所有副本确认”，保证可靠性[citation:4]
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    // 设置重试次数，应对可重试错误[citation:2]
    props.put(ProducerConfig.RETRIES_CONFIG, "3")

    // 2. 创建Kafka生产者实例
    val producer = new KafkaProducer[String, String](props)

    // 3. 模拟消息源，持续发送消息
    val topic = "testtopic"
    var messageCount = 0
    val rnd = new Random()

    try {
      while (true) {
        // 模拟生成消息
        val sensorId = s"sensor-${rnd.nextInt(10)}"
        val temperature = f"${20 + rnd.nextDouble() * 10}%.1f"
        val message = s"$sensorId,${System.currentTimeMillis()},$temperature"

        // 4. 构建ProducerRecord（消息）并发送
        // send()方法会将消息放入缓冲区，由后台线程发送[citation:2]
        val record = new ProducerRecord[String, String](topic, sensorId, message)

        // 使用带回调的异步发送，发送完成后会触发回调函数[citation:2]
        producer.send(record, new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (exception == null) {
              println(s"[发送成功] 主题: ${metadata.topic()}, " +
                s"分区: ${metadata.partition()}, " +
                s"偏移量: ${metadata.offset()}, " +
                s"消息: $message")
            } else {
              System.err.println(s"[发送失败] 消息: $message, 错误: ${exception.getMessage}")
            }
          }
        })

        messageCount += 1
        if (messageCount % 100 == 0) {
          println(s"已发送 $messageCount 条消息...")
        }

        // 控制发送频率
        Thread.sleep(1000)
      }
    } catch {
      case e: InterruptedException => println("生产者被中断。")
      case e: Exception => e.printStackTrace()
    } finally {
      // 5. 关闭生产者，释放资源
      producer.close()
      println("生产者已关闭。")
    }
  }
}