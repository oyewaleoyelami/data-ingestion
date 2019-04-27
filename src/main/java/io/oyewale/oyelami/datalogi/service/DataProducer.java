package io.oyewale.oyelami.datalogi.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class DataProducer {

    private static final Logger logger = LoggerFactory.getLogger(DataProducer.class);

    private static final String KAFKA_TOPIC = "devicedata";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String deviceData) {
        logger.info("pushing device data  {}  to the topic  {}", deviceData, KAFKA_TOPIC);
        this.kafkaTemplate.send(KAFKA_TOPIC, deviceData);
    }

}
