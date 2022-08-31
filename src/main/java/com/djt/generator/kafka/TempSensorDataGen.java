package com.djt.generator.kafka;

import com.djt.entity.TempSensorData;
import com.github.javafaker.Faker;
import java.util.Locale;
import java.util.Random;

public class TempSensorDataGen {
    public static void main(String[] args) throws Exception {
        Faker fakerZH = new Faker(new Locale("zh-CN"));
        TempSensorData tempSensorData;
        Random random = new Random();
        while (true) {
            tempSensorData = TempSensorData.builder()
                    .sensorID("s-"+fakerZH.number().numberBetween(1,6))
                    .tp(Long.parseLong((System.currentTimeMillis()+"").substring(0,10)))
                    .temp(fakerZH.number().numberBetween(5,40))
                    .build();
            System.out.println(tempSensorData.getSensorID()+","+tempSensorData.getTp()+","+tempSensorData.getTemp());
            //随机sleep

            Thread.sleep(fakerZH.number().numberBetween(1,6)*1000);
        }
    }
}
