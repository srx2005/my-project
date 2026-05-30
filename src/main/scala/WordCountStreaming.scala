package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object WordCountStreaming {
  def main(args: Array[String]): Unit = {
    // 创建Spark配置
    val conf = new SparkConf()
      .setAppName("WordCountStreaming")
      .setMaster("local[2]") // 使用local模式，2个线程
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 优雅关闭
      .set("spark.sql.shuffle.partitions", "1") // 减少分区数，提高本地测试效率

    // 创建StreamingContext，批处理间隔为5秒
    val ssc = new StreamingContext(conf, Seconds(5))

    // 设置检查点目录（updateStateByKey需要）
    ssc.checkpoint("checkpoint")

    try {
      // 创建DStream，从nc服务器读取数据
      val lines = ssc.socketTextStream("localhost", 9999)

      // 对每一行进行分词
      val words = lines.flatMap(_.split("\\s+"))

      // 转换为(word, 1)格式
      val wordCounts = words.map(word => (word, 1))

      // 定义状态更新函数
      def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
        // 计算当前批次的新值总和
        val currentCount = newValues.sum
        // 获取之前的状态值（如果没有则为0）
        val previousCount = runningCount.getOrElse(0)
        // 返回更新后的状态
        Some(currentCount + previousCount)
      }

      // 使用updateStateByKey进行状态更新
      val runningCounts = wordCounts.updateStateByKey[Int](updateFunction _)

      // 打印前10个结果
      runningCounts.foreachRDD { rdd =>
        if (!rdd.isEmpty()) {
          println(s"\n===== 批处理时间: ${System.currentTimeMillis()} =====")
          println("单词计数结果:")

          // 按计数降序排序并取前10
          val topWords = rdd
            .map { case (word, count) => (count, word) }
            .sortByKey(ascending = false)
            .take(10)

          topWords.foreach { case (count, word) =>
            println(s"$word: $count")
          }

          // 显示总的不同单词数
          val totalWords = rdd.count()
          println(s"总的不同单词数: $totalWords")
        }
      }

      // 另一种输出方式：打印所有结果（可能有重复）
      runningCounts.print()

      // 启动流计算
      ssc.start()
      println("Spark Streaming应用已启动，正在监听 localhost:9999...")
      println("请在nc客户端输入文本，按行发送")
      println("按 Ctrl+C 停止应用")

      // 等待终止
      ssc.awaitTermination()

    } catch {
      case e: Exception =>
        println(s"发生错误: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // 确保StreamingContext正确关闭
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }
}