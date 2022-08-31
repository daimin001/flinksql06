package com.djt.entity;

import lombok.*;

/**
 * 点击日志(在Flink中使用Java pojo)
 */
@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ClickLogs {
    /**
     * 用户名
     */
    private String user;
    /**
     * 用户访问的URL
     */
    private String url;
    /**
     * 访问时间
     */
    private String cTime;
}
