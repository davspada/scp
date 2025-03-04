# scp
University project for the Scalable and Cloud Computing course

## How to use
```sbt package```

`spark-submit --class Main --conf spark.eventLog.enabled=true 
--conf spark.eventLog.dir=file:/tmp/spark-events target/scala-2.12/cpa-scp_2.12-1.0.jar ./src/order_products.csv output`

//--master "local[*]"