package io.oyewale.oyelami.datalogi.entity;

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

    private UUID deviceId;

    private int temperature;

    private double humidity;

    private double precipitation;

    private Location location;

    private long time;


}
