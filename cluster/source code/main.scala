package test

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    //建立Spark连接
    val spark = SparkSession.builder().appName("test").getOrCreate()
    // 将txt文件按照csv格式读入
    val cdinfo = spark.read.option("header", "false").csv(args(0))
    val infected = spark.read.option("header", "false").csv(args(1))
    //获取感染者的手机号
    val infected_tel = infected.select("_c0").collect().map(_.getString(0)).distinct
    // 筛选出被感染的基站的信息
    val infected_base_info = cdinfo.filter(col("_c3").isin(infected_tel: _*))
    // 记录基站开始及结束的污染时间
    val infected_Base_Start = infected_base_info.filter(col("_c2") === "1")
      .select(col("_c0").as("base"), col("_c1").as("start_time"), col("_c3").as("infected_tel"))
    val infected_Base_End = infected_base_info.filter(col("_c2") === "2")
      .select(col("_c0").as("base"), col("_c1").as("end_time"), col("_c3").as("infected_tel"))
    val potential_infected_Start = cdinfo.filter(col("_c2") === "1")
      .select(col("_c0").as("base"), col("_c1").as("p_start_time"), col("_c3").as("potential_tel"))
    val potential_infected_End = cdinfo.filter(col("_c2") === "2")
      .select(col("_c0").as("base"), col("_c1").as("p_end_time"), col("_c3").as("potential_tel"))

    val infected_Base_Period = infected_Base_Start.join(infected_Base_End, Seq("base", "infected_tel"))
    val potential_Infected_Period = potential_infected_Start.join(potential_infected_End, Seq("base", "potential_tel"))
    // 定义函数来判断时间是否在感染时间段内
    val is_Within_Period = udf((p_start_time: String, p_end_time: String, start_time: String, end_time: String) => {
      !((p_start_time.toLong > end_time.toLong) || (p_end_time.toLong < start_time.toLong))
    })
    // 过滤出潜在感染者并按手机号排序
    val potential_infected = potential_Infected_Period
      .join(infected_Base_Period, potential_Infected_Period("base") === infected_Base_Period("base"))
      .filter(is_Within_Period(col("p_start_time"), col("p_end_time"), col("start_time"), col("end_time")))
      .select("potential_tel").distinct().sort("potential_tel")
    // 将结果写入txt文件
    potential_infected.coalesce(1).write.text(args(2))
  }
}
