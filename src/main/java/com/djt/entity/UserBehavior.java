package com.djt.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: 🐟lf🐟
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;
}
