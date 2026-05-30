import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SQL {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("SparkSQL MySQL Integration")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    try {
      // (1) 定义Schema
      val schema = StructType(Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("gender", StringType, nullable = false),
        StructField("major", StringType, nullable = false)
      ))

      println("Schema定义完成")

      // (2) 从文本文件读取数据，创建RDD并转换为DataFrame
      val studentDF = spark.read
        .option("delimiter", ",")
        .option("header", "false")
        .schema(schema)
        .csv("data1.txt")

      println("数据读取完成，显示前5行：")
      studentDF.show(5)

      // 打印Schema
      println("DataFrame Schema:")
      studentDF.printSchema()

      // MySQL连接配置
      val jdbcUrl = "jdbc:mysql://localhost:3306/spark_db?useSSL=false&serverTimezone=UTC"
      val jdbcUsername = "root"
      val jdbcPassword = "587769Lzc"
      val tableName = "students"

      // (3) 将DataFrame数据写入MySQL数据库
      println("正在写入MySQL数据库...")
      studentDF.write
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("user", jdbcUsername)
        .option("password", jdbcPassword)
        .mode(SaveMode.Overwrite)
        .save()

      println("数据成功写入MySQL数据库")

      // (4) 从MySQL数据库读取数据到DataFrame
      println("从MySQL数据库读取数据...")
      val mysqlDF = spark.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("user", jdbcUsername)
        .option("password", jdbcPassword)
        .load()

      println("从MySQL读取的数据：")
      mysqlDF.show(10)

      // 创建临时视图用于Spark SQL查询
      mysqlDF.createOrReplaceTempView("students")

      // (5) 通过Spark SQL对学生信息进行查询统计

      // 5.1 统计不同系别的学生数量
      println("不同系别的学生数量：")
      spark.sql("""
        SELECT major, COUNT(*) as student_count
        FROM students
        GROUP BY major
        ORDER BY student_count DESC
      """).show()

      // 5.2 找出年龄最大的学生
      println("年龄最大的学生：")
      spark.sql("""
        SELECT * FROM students
        WHERE age = (SELECT MAX(age) FROM students)
      """).show()

      // 5.3 查询各个系别的平均年龄
      println("各个系别的平均年龄：")
      spark.sql("""
        SELECT major, ROUND(AVG(age), 2) as avg_age
        FROM students
        GROUP BY major
        ORDER BY avg_age DESC
      """).show()

      // 5.4 查询每个性别的学生数量
      println("每个性别的学生数量：")
      spark.sql("""
        SELECT gender, COUNT(*) as count
        FROM students
        GROUP BY gender
        ORDER BY count DESC
      """).show()

      // 5.5 查询年龄在18至20岁之间的学生
      println("年龄在18至20岁之间的学生：")
      spark.sql("""
        SELECT * FROM students
        WHERE age BETWEEN 18 AND 20
        ORDER BY age
      """).show()

      // 5.6 找出哪个系别的学生数量最多
      println("学生数量最多的系别：")
      spark.sql("""
        SELECT major, COUNT(*) as student_count
        FROM students
        GROUP BY major
        ORDER BY student_count DESC
        LIMIT 1
      """).show()

      // 5.7 将学生信息按年龄降序排列
      println("学生信息按年龄降序排列：")
      spark.sql("""
        SELECT * FROM students
        ORDER BY age DESC
      """).show()

    } catch {
      case e: Exception =>
        println(s"发生错误: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // 关闭SparkSession
      spark.stop()
      println("SparkSession已关闭")
    }
  }
}
