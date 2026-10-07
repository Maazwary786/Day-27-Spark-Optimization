import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day27App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Optimization")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    println("===== SPARK PERFORMANCE OPTIMIZATION =====")

    val sales = spark.range(0, 100000)
      .withColumn("store_id", (col("id") % 10).cast("int"))
      .withColumn("amount", (col("id") % 1000) + 100)

    val stores = Seq(
      (0,"Hyderabad"),
      (1,"Delhi"),
      (2,"Mumbai"),
      (3,"Bangalore"),
      (4,"Pune"),
      (5,"Chennai"),
      (6,"Kolkata"),
      (7,"Jaipur"),
      (8,"Noida"),
      (9,"Lucknow")
    ).toDF("store_id","city")

    println("ORIGINAL PARTITIONS:")
    println(sales.rdd.getNumPartitions)

    println("REPARTITIONED:")
    val repartitioned = sales.repartition(8, col("store_id"))
    println(repartitioned.rdd.getNumPartitions)

    println("BROADCAST JOIN:")
    val result = repartitioned
      .join(broadcast(stores), Seq("store_id"))

    result.groupBy("city")
      .agg(
        count("*").as("transactions"),
        sum("amount").as("revenue")
      )
      .orderBy(desc("revenue"))
      .show(false)

    println("EXPLAIN PLAN:")
    result.explain(true)

    println("CACHE:")
    result.cache()
    result.count()
    result.count()

    result.unpersist()

    println("OPTIMIZATION COMPLETE")

    spark.stop()
  }
}
