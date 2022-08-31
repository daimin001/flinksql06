package com.djt.entity;

import lombok.*;

@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Product {
    /**
     * 产品ID
     */
    private Long id;
    /**
     * 产品名称
     */
    private String name;
}
