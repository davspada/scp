import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Co-Purchase Analysis")
      .getOrCreate()

    val sc = spark.sparkContext

    val inputFile = if (args.nonEmpty) args(0) else "data/orders.csv"
    val outputFile = if (args.length > 1) args(1) else "output/co_purchases.csv"

    // Compute partitions based on cluster configuration, fallback to 100
    val conf = sc.getConf
    val numExecutors = conf.get("spark.executor.instances", "4").toInt
    val numCores = conf.get("spark.executor.cores", "4").toInt
    val partitions = numExecutors * numCores * 3

    // 1. Read and parse data
    val rawData: RDD[(Int, Int)] = sc.textFile(inputFile)
      .map(_.split(","))
      .filter(_.length == 2)
      .map(arr => (arr(0).toInt, arr(1).toInt))
      .repartition(partitions) // Partition early

    // 2. Group products by order_id (shuffle step)
    val groupedByOrder: RDD[(Int, Iterable[Int])] =
      rawData.groupByKey(partitions) // shuffle with correct partitioning

    // 3. Generate product pairs per order (local to each partition)
    val productPairs: RDD[((Int, Int), Int)] = groupedByOrder.flatMap { case (_, products) =>
      val productList = products.toArray.sorted // sorted for consistent (i, j)
      for {
        i <- productList.indices
        j <- (i + 1) until productList.length
      } yield ((productList(i), productList(j)), 1)
    }

    // 4. Reduce by key to count co-occurrences
    val coPurchaseCounts: RDD[((Int, Int), Int)] =
      productPairs.reduceByKey(_ + _, partitions)

    // 5. Write output as requested, coalesced to a single file
    coPurchaseCounts
      .map { case ((p1, p2), count) => s"$p1,$p2,$count" }
      .repartition(1)
      .saveAsTextFile(outputFile)

    spark.stop()
  }
}