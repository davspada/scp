# scp
University project for the Scalable and Cloud Computing course

## How to use
```sbt package```

```spark-submit --class Main \                                                                                                                (base)
                                          --master "local[*]" \
                                          target/scala-2.12/cpa-scp_2.12-1.0.jar ./src/order_products.csv output```