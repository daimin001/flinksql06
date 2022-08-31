package com.djt.entity;

import lombok.*;

@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Orders {
    /**
     * 用户
     */
    private String user;
    /**
     * 产品ID
     */
    private Long productId;
    /**
     * 数量
     */
    private Integer amount;
    /**
     * 下单时间
     */
    private Long orderTp;
}
