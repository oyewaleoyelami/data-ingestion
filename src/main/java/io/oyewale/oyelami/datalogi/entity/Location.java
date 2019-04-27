package io.oyewale.oyelami.datalogi.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
 * denotes the geo-location of a device
 * */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Location {

    private long latitude;

    private long longitude;
}
