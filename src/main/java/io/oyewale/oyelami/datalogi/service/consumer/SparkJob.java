package io.oyewale.oyelami.datalogi.service.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.oyewale.oyelami.datalogi.entity.DeviceData;
import io.oyewale.oyelami.datalogi.entity.IotData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;


@Component
public class SparkJob implements Serializable {

    @PostConstruct
    void init() throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("device-data-streaming");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));

        ObjectMapper mapper = new ObjectMapper();

        Collection<String> topics = Arrays.asList("devicedata");

        HashMap kafkaProperties = new HashMap();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jsc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaProperties));

        JavaDStream<String> lines = messages.map(stringStringConsumerRecord -> stringStringConsumerRecord.value());

        lines.foreachRDD(dataObjects -> {
            dataObjects.foreach(

                    dataPoint -> {
                        DeviceData deviceData = mapper.readValue(dataPoint, DeviceData.class);
                        IotData retrievedData = deviceData.getData();

                        System.out.println("Original Time: " + retrievedData.getTime() + " , " + "Converted Time: " + convertTimeStamp(retrievedData.getTime()));
                        System.out.println("Device Id: " + retrievedData.getDeviceId() + " ," + "Temperature: " + retrievedData.getTemperature());
                    }
            );
        });

        jsc.start();
        jsc.awaitTermination();
    }


    String convertTimeStamp(long time) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        return formatter.format(time);
    }
}
