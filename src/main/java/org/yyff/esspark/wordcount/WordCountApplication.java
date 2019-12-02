package org.yyff.esspark.wordcount;

import org.apache.spark.SparkConf;

import java.io.IOException;
import java.util.Properties;

public class WordCountApplication {
    public static void main(String[] args) throws IOException {

        // 1. read config
        Config config = new Config("wordcount.properties");
        Properties properties = config.getProperties();
        String esHost = properties.getProperty("ES_HOST");
        String esPort = properties.getProperty("ES_PORT");
        String password = properties.getProperty("ES_PASSWORD");
        if (esHost.equals("") || esPort.equals("") || password.equals("")) {
//            System.out.printf("conf: [ES_HOST | ES_PORT | ES_PASSWORD] is empty");
            throw new IllegalStateException("conf: [ES_HOST | ES_PORT | ES_PASSWORD] is empty");
        }

        // 2. set spark conf for es
        SparkConf sparkConf = new SparkConf().setAppName("wordcount")
                .setMaster("local[*]").set("es.index.auto.create", "true")
                .set("es.nodes", esHost).set("es.port", esPort).set("es.nodes.wan.only", "true")
                .set("es.net.http.auth.user", "elastic")
                .set("es.net.http.auth.pass", password);

        // 3. quering blogs its author is 'Shay Banon' and count words of 'content' field
        String[] conditions = new String[]{"author=\"Shay Banon\""};
        new WordCount(sparkConf, "output/wordcount").executeBySQL("blogs", "content", conditions);
    }
}
