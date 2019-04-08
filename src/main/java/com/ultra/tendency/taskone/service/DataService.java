package com.ultra.tendency.taskone.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.ultra.tendency.taskone.entity.DeviceData;
import com.ultra.tendency.taskone.entity.IotData;
import com.ultra.tendency.taskone.entity.Location;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class DataService {

    private final ObjectMapper mapper;

    private final DataProducer dataProducer;

    //simulated ID for the three devices.

    UUID firstDevice = UUID.randomUUID();

    UUID secondDevice = UUID.randomUUID();

    UUID thirdDevice = UUID.randomUUID();


    public DataService(DataProducer dataProducer, ObjectMapper mapper){
        this.mapper = mapper;
        this.dataProducer = dataProducer;
    }

    @Scheduled(fixedRate = 10000)
    public void simulateDevice() throws JsonProcessingException {
        long timestamp = Instant.now().getEpochSecond();

        //using the faker library to generate fake data
        Faker faker = new Faker();

        int temperature = faker.number().numberBetween(0,100);
        String fakeLongitude = faker.address().longitude();
        String fakeLatitude = faker.address().latitude();

        long latitude =  Double.valueOf(Double.parseDouble(fakeLatitude)).longValue();
        long longitude = Double.valueOf(Double.parseDouble(fakeLongitude)).longValue();
        Location location = Location.builder()
                .latitude(latitude)
                .longitude(longitude)
                .build();

        //first device
        pushData(location,temperature,timestamp,firstDevice);

        //second device
        pushData(location,temperature,timestamp,secondDevice);

        //third device
        pushData(location,temperature,timestamp,thirdDevice);
    }

    public void pushData(Location location, int temperature, long timestamp, UUID deviceId) throws JsonProcessingException {
        IotData deviceT = IotData.builder()
                .location(location)
                .temperature(temperature)
                .time(timestamp)
                .deviceId(deviceId).build();

        // push to kafka
        String dataObject = mapper.writeValueAsString(DeviceData.builder().data(deviceT).build());
        dataProducer.sendMessage(dataObject);
    }
}
