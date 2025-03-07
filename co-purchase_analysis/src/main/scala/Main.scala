import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object Main {
  def main(args: Array[String]): Unit = {

    // Crea una sessione Spark
    val spark = SparkSession.builder()
      .appName("Co-Purchase Analysis")
      .master("local[*]")  // Usa tutti i core disponibili
      //.config("spark.driver.memory", "8g")
      //.config("spark.executor.memory", "8g")
      //.config("spark.sql.shuffle.partitions", "200") // Riduci il numero di partizioni di shuffle
      //.config("spark.memory.fraction", "0.8") // Usa più RAM
      .getOrCreate()

    val sc = spark.sparkContext

    // Percorsi input/output
    val inputFile = if (args.nonEmpty) args(0) else "data/orders.csv"
    val outputFile = if (args.length > 1) args(1) else "output/co_purchases.csv"

    // Numero di partizioni basato sul numero di core disponibili
    val numPartitions = 200//sc.defaultParallelism * 2 // con 200 fa 55 secondi

    // **1. Lettura e parsing dei dati**
    val rawData: RDD[(Int, Int)] = sc.textFile(inputFile)
      .mapPartitions(iter => iter.map(_.split(",")))
      // .filter(_.length == 2)
      .map(arr => (arr(0).toInt, arr(1).toInt))

    // **2. Partitioning iniziale con HashPartitioner**
    val partitionedData = rawData.partitionBy(new HashPartitioner(numPartitions))

    // **3. Raggruppamento dei dati per order_id usando groupByKey**
    val groupedData = partitionedData.groupByKey()

    // **4. FlatMap per generare coppie di prodotti senza ordinamento**
    val productPairs: RDD[((Int, Int), Int)] = groupedData.flatMap { case (_, products) =>
      // Per ogni ordine, genera tutte le coppie di prodotti (senza ordinare)
      val productList = products.toList
      for {
        i <- productList
        j <- productList if i < j
      } yield ((i, j), 1)
    }

    // **5. Partitioning ottimizzato per ReduceByKey**
    val repartitionedPairs = productPairs.partitionBy(new HashPartitioner(numPartitions))

    // **6. Riduzione finale per sommare i conteggi**
    val coPurchaseCounts = repartitionedPairs.reduceByKey(_ + _)

    // **7. Salvataggio senza coalesce, Spark gestirà automaticamente la scrittura su più file**
    coPurchaseCounts
      .map { case ((p1, p2), count) => s"$p1,$p2,$count" }
      .repartition(1)
      .saveAsTextFile(outputFile)

    spark.stop()
  }
}
