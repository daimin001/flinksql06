package com.djt.chap06.udaf;

import lombok.*;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Table Functions
 */
public class FlinkAggFunction {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        Table scores = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("subject", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE())
                ),
                row(1, "zhangsan","Chinese","90"),
                row(1, "zhangsan","Math","74"),
                row(1, "zhangsan","English","100"),
                row(2L, "lisi","Chinese","86"),
                row(2L, "lisi","Math","99"),
                row(2L, "lisi","English","92")
        ).select($("id"), $("name"),$("subject"),$("score"));

        tEnv.createTemporaryView("scoresTable",scores);

        //注册函数
        tEnv.createTemporarySystemFunction("AvgFunction", new AvgFunction());

        // Table API使用：使用call函数调用已注册的UDF
        tEnv.from("scoresTable")
                .groupBy($("subject"))
                .select($("subject"), call("AvgFunction",$("score")).as("score_avg"))
                .execute()
                .print();

        // SQL使用
        tEnv.sqlQuery("SELECT subject, AvgFunction(score) as score_avg FROM scoresTable GROUP BY subject")
                .execute()
                .print();
    }

    /**
     * 可变累加器的数据结构
     * 商品分类的平均值（sum/count）
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AvgAccumulator {
        public double sum = 0.0;
        public int count = 0;
    }

    public static class AvgFunction extends AggregateFunction<Double,AvgAccumulator> {

        @Override
        public Double getValue(AvgAccumulator accumulator) {
            if (accumulator.count == 0) {
                return null;
            } else {
                return accumulator.sum / accumulator.count;
            }
        }

        @Override
        public AvgAccumulator createAccumulator() {
            return new AvgAccumulator();
        }

        public void accumulate(AvgAccumulator acc, Double price) {
            acc.setSum(acc.sum+price);
            acc.setCount(acc.count+1);
        }

        //在OVER windows上才是必须的
//        public void retract(AvgAccumulator acc, Double price) {
//            acc.setSum(acc.sum-price);
//            acc.setCount(acc.count-1);
//        }
        //有界聚合以及会话窗口和滑动窗口聚合都需要(对性能优化也有好处)
//        public void merge(AvgAccumulator acc, Iterable<AvgAccumulator> it) {
//            for (AvgAccumulator a : it) {
//                acc.setSum(acc.sum+a.getSum());
//                acc.setCount(acc.count+a.getCount());
//            }
//        }
//
//        public void resetAccumulator(AvgAccumulator acc) {
//            acc.count = 0;
//            acc.sum = 0.0d;
//        }
    }

}
