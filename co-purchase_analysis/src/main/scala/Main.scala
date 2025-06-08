import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object Main {
  def main(args: Array[String]): Unit = {

    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("Co-Purchase Analysis")
      .getOrCreate()

    val sc = spark.sparkContext

    // Input/output paths
    val inputFile = if (args.nonEmpty) args(0) else "data/orders.csv"
    val outputFile = if (args.length > 1) args(1) else "output/co_purchases.csv"

    // Number of partitions based on workers * cores * a multiplication factor (e.g., 3)
    val conf = sc.getConf
    val numNodes = conf.get("spark.executor.instances")
    val numCores = conf.get("spark.executor.cores")
    val partitions = (numNodes.toInt * numCores.toInt * 3)

    println(s"Nodes: $numNodes, Cores: $numCores, Using $partitions partitions")

    // Read and parse data, apply partitioning immediately after mapping
    val rawData: RDD[(Int, Int)] = sc.textFile(inputFile)
      .mapPartitions(iter => iter.map(_.split(",")))
      .map(arr => (arr(0).toInt, arr(1).toInt))
      .partitionBy(new HashPartitioner(partitions)) // <--- Partition early, this made a big difference when scaling to n workers

    // Group data by order_id
    val groupedData = rawData.groupByKey()

    // Generate product pairs for each order
    val productPairs: RDD[((Int, Int), Int)] = groupedData.flatMap { case (_, products) =>
      val productList = products.toList
      for {
        i <- productList
        j <- productList if i < j
      } yield ((i, j), 1)
    }

    // Optimized partitioning for reduceByKey
    val coPurchaseCounts = productPairs
      .partitionBy(new HashPartitioner(partitions))
      .reduceByKey(_ + _)

    // Save results
    coPurchaseCounts
      .map { case ((p1, p2), count) => s"$p1,$p2,$count" }
      .repartition(1)
      .saveAsTextFile(outputFile)

    spark.stop()
  }
}