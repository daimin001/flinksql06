package com.djt.entity;

import lombok.*;

/**
 * 学生成绩
 */
@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class StudentScore {
    /**
     * id
     */
    private Integer id;
    /**
     * 姓名
     */
    private String name;
    /**
     * 学科
     */
    private String subject;
    /**
     * 分数
     */
    private Double score;
}
