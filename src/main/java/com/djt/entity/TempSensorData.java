package com.djt.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 温度传感器数据
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TempSensorData {
    /**
     * 传感器ID
     */
    private String sensorID;
    /**
     * 时间戳
     */
    private Long tp;
    /**
     * 温度
     */
    private Integer temp;
}
