package org.yyff.esspark.wordcount;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import scala.Tuple2;

import java.util.Arrays;


public class WordCount {
    private SparkConf sparkConf;
    private String outDir;

    public WordCount(SparkConf sc, String dir) {
        sparkConf = sc;
        outDir = dir;
    }

    public void executeBySQL(String indexName, String field, String[] conditions) throws IllegalArgumentException {
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter
        SQLContext sql = new SQLContext(jsc);
//        DataSet<Row> df = sql.read().format("es").load("blogs");
        Dataset<Row> ds = JavaEsSparkSQL.esDF(sql, indexName);
        ds.printSchema();
//        ds = ds.select(ds.col(field)).where(ds.col("author").equalTo("Shay Banon"));
        ds = ds.select(ds.col(field));
        for (String cond : conditions) {
            ds = ds.where(cond);
        }

        JavaPairRDD<String, Integer> counts = ds.javaRDD().flatMap(
                row -> {
                    String line = row.getAs(field);
                    if (line == null) {
//                         System.out.printf("[DEBUG] null content. author: %s, title: %s\n", row.getAs("author"), row.getAs("title"));
                        System.out.printf("error: null content.\n"); // TODO: figure it out why there has null value
                        line = "";
                    }
                    return Arrays.asList(line.split(" ")).iterator();
                }).mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey((x, y) -> x + y);
        counts.saveAsTextFile(outDir);
        sparkSession.stop();
    }
}
