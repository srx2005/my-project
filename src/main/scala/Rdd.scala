import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object Rdd {
  def main(args: Array[String]): Unit = {
    // 创建Spark配置
    val conf = new SparkConf()
      .setAppName("StudentGradeAnalysis")
      .setMaster("local[*]")
    // 创建SparkContext
    val sc = new SparkContext(conf)
    try {
      // 读取数据文件
      val dataRDD: RDD[String] = sc.textFile("data.txt")
      // 数据预处理：将每行数据拆分为(学生姓名, 课程名称, 成绩)
      val gradeRDD: RDD[(String, String, Int)] = dataRDD.map { line =>
        val parts = line.split("\\s+")
        if (parts.length >= 3) {
          val studentName = parts(0)
          val courseName = parts(1)
          val grade = parts(2).toInt
          (studentName, courseName, grade)
        } else {
          // 处理格式不正确的行
          ("", "", 0)
        }
      }
      println("=== 实验数据集分析结果 ===")
      // (1) 统计学生人数
      val studentCount = gradeRDD.map(_._1).distinct().count()
      println(s"1. 学生人数: $studentCount")
      // (2) 统计课程数
      val courseCount = gradeRDD.map(_._2).distinct().count()
      println(s"2. 课程数: $courseCount")
      // (3) 计算总成绩和平均分
      val totalGrade = gradeRDD.map(_._3).sum()
      val recordCount = gradeRDD.count()
      val averageGrade = if (recordCount > 0) totalGrade / recordCount else 0
      println(s"3. 总成绩: $totalGrade, 平均分: ${"%.2f".format(averageGrade)}")
      // (4) 求每名同学的选修的课程门数
      val coursesPerStudent = gradeRDD.map { case (student, course, grade) =>
        (student, 1)
      }.reduceByKey(_ + _)
      println("4. 每名同学的选修课程门数:")
      coursesPerStudent.collect().foreach { case (student, count) =>
        println(s"   $student: $count 门")
      }
      // (5) 统计某课程的选修人数（示例：统计第一门课程的选修人数）
      val firstCourse = gradeRDD.map(_._2).distinct().first()
      val courseEnrollment = gradeRDD
        .filter(_._2 == firstCourse)
        .map(_._1)
        .distinct()
        .count()
      println(s"5. 课程 '$firstCourse' 的选修人数: $courseEnrollment")
      // (6) 统计各门课程的平均分
      val courseAverage = gradeRDD.map { case (student, course, grade) =>
        (course, (grade, 1))  // (课程, (成绩, 计数))
      }.reduceByKey { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }.map { case (course, (sum, count)) =>
        (course, sum.toDouble / count)
      }
      println("6. 各门课程的平均分:")
      courseAverage.collect().sortBy(_._1).foreach { case (course, avg) =>
        println(s"   $course: ${"%.2f".format(avg)}")
      }
      // (7) 使用累加器计算某门课的选修人数
      val accumulator: LongAccumulator = sc.longAccumulator("CourseEnrollmentAccumulator")
      println(s"7. 使用累加器统计课程 '$firstCourse' 的选修人数: ${accumulator.value}")
    } finally {
      // 关闭SparkContext
      sc.stop()
    }
  }
}