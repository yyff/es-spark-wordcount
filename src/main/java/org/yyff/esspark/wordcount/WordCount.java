package org.yyff.esspark.wordcount;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.EsSpark;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;


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
                        line = "";
                    }
                    return Arrays.asList(line.split(" ")).iterator();
                }).mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey((x, y) -> x + y);
        counts.saveAsTextFile(outDir);
        EsSpark.saveToEs(counts.rdd(), "spark-sql/_doc");
        sparkSession.stop();
    }

    public void executeByQuery(String indexName, String field, String query) {
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, indexName, query).values();
        JavaPairRDD<String, Integer> counts = esRDD.flatMap(doc -> {
           String fieldValue = (String)doc.get(field);
           if (fieldValue == null) {
               fieldValue = "";
           }
           return Arrays.asList(fieldValue.split(" ")).iterator();
        }).mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey((x, y) -> x + y);
        counts.saveAsTextFile(outDir);
        EsSpark.saveToEs(counts.rdd(), "spark-native/_doc");
    }
}
