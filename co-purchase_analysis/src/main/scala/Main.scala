import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    // Creazione della sessione Spark
    val spark = SparkSession.builder()
      .appName("Co-Purchase Analysis")
      .master("local[*]") // Esecuzione in locale con tutti i core disponibili
      .getOrCreate()

    val sc = spark.sparkContext

    // Percorso del file CSV (da passare come argomento o impostare qui)
    val inputFile = if (args.nonEmpty) args(0) else "data/orders.csv"
    val outputFile = if (args.length > 1) args(1) else "output/co_purchases.csv"

    // Carica il dataset come RDD
    val data: RDD[(Int, Int)] = sc.textFile(inputFile)
      .map(line => line.split(","))
      .filter(arr => arr.length == 2)
      .map(arr => (arr(0).toInt, arr(1).toInt)) // (order_id, product_id)

    // Raggruppa i prodotti per ordine
    val groupedByOrder: RDD[(Int, Iterable[Int])] = data.groupByKey()

    // Genera tutte le coppie possibili di prodotti per ogni ordine e le conta
    val coPurchasePairs: RDD[((Int, Int), Int)] = groupedByOrder.flatMap { case (_, productList) =>
      productList.toList.sorted.combinations(2).map { case Seq(p1, p2) => ((p1, p2), 1) }
    }

    // Riduce per chiave sommando le occorrenze
    val coPurchaseCounts: RDD[(Int, Int, Int)] = coPurchasePairs
      .reduceByKey(_ + _)
      .map { case ((p1, p2), count) => (p1, p2, count) }

    // Salva il risultato su file CSV
    coPurchaseCounts
      .map { case (p1, p2, count) => s"$p1,$p2,$count" }
      .saveAsTextFile(outputFile)

    println(s"Analisi completata! Risultati salvati in: $outputFile")

    // Chiudi Spark
    spark.stop()
  }
}
