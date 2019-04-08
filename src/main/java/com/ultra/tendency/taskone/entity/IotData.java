package com.ultra.tendency.taskone.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IotData {

    UUID  deviceId;

    int temperature;

    Location location;

    long time;


}
