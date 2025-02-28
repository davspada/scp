import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object Main {
  def main(args: Array[String]): Unit = {
    val initialTime = System.currentTimeMillis()

    val spark = SparkSession.builder()
      .appName("Co-Purchase Analysis")
      .master("local[*]")  // Usa tutti i core disponibili
      .config("spark.sql.shuffle.partitions", "100") // Ottimizzazione shuffle
      .getOrCreate()

    val sc = spark.sparkContext

    val inputFile = if (args.nonEmpty) args(0) else "data/orders.csv"
    val outputFile = if (args.length > 1) args(1) else "output/co_purchases.csv"

    val numPartitions = sc.defaultParallelism * 2

    // **1. Lettura e parsing iniziale**
    val rawData: RDD[(Int, Int)] = sc.textFile(inputFile)
      .map(_.split(","))
      .filter(_.length == 2) // Evita righe malformate
      .map(arr => (arr(0).toInt, arr(1).toInt)) // Converti ID ordine e prodotto in Int

    // **2. Partizionamento iniziale con HashPartitioner**
    val partitionedData = rawData.partitionBy(new HashPartitioner(numPartitions))

    // **3. Raggruppamento per ID ordine**
    val groupedData: RDD[(Int, Iterable[Int])] = partitionedData.groupByKey()

    // **4. Generazione coppie di prodotti acquistati insieme**
    val productPairs: RDD[((Int, Int), Int)] = groupedData.flatMap { case (_, products) =>
      val uniqueProducts = products.toSet.toArray.sorted // Set per evitare duplicati
      for (i <- uniqueProducts.indices; j <- i + 1 until uniqueProducts.length)
        yield ((uniqueProducts(i), uniqueProducts(j)), 1)
    }

    // **5. Nuovo partizionamento per ottimizzare ReduceByKey**
    val repartitionedPairs = productPairs.partitionBy(new HashPartitioner(numPartitions))

    // **6. Aggregazione con ReduceByKey**
    val coPurchaseCounts = repartitionedPairs.reduceByKey(_ + _)

    // **7. Ridistribuzione per avere un solo file in output**
    coPurchaseCounts
      .map { case ((p1, p2), count) => s"$p1,$p2,$count" }
      .repartition(1) // Un solo file di output
      .saveAsTextFile(outputFile)

    println(s"Analisi completata! Risultati salvati in: $outputFile")

    val finalTime = System.currentTimeMillis()
    val elapsedTime = (finalTime - initialTime) / 1000
    println(s"Tempo di esecuzione: $elapsedTime secondi")

    spark.stop()
  }
}
