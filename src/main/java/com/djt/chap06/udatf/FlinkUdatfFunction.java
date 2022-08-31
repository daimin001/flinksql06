package com.djt.chap06.udatf;

import com.djt.chap06.udaf.FlinkAggFunction;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.*;

/**
 * TableAggregateFunction
 */
public class FlinkUdatfFunction {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        Table scores = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("subject", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE())
                ),
                row(1, "zhangsan","Chinese","90"),
                row(1, "zhangsan","Math","74"),
                row(1, "zhangsan","English","88"),
                row(2, "lisi","Chinese","86"),
                row(2, "lisi","Math","96"),
                row(2, "lisi","English","92"),
                row(3, "mary","Chinese","59"),
                row(3, "mary","Math","99"),
                row(3, "mary","English","100")
        ).select($("id"), $("name"),$("subject"),$("score"));
        scores.execute().print();

        tEnv.createTemporaryView("scoresTable",scores);

        //注册函数
        tEnv.createTemporarySystemFunction("Top2Func", new Top2Func());

        // Table API使用：使用call函数调用已注册的UDF
        tEnv.from("scoresTable")
                .groupBy($("subject"))//groupby不是必须的
                //必须咋flatAggregate中调用
                .flatAggregate(call("Top2Func",$("score")).as("score","rank"))
                .select($("subject"),$("score"),$("rank"))
//                .select($("score"),$("rank"))
                .execute()
                .print();

    }

    /**
     * 可变累加器的数据结构
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Top2Accumulator {
        //为啥不保存top n的记录的全部信息
        /**
         * top 1的值
         */
        public Double topOne = Double.MIN_VALUE;
        /**
         * top 2的值
         */
        public Double topTwo = Double.MIN_VALUE;
    }

    public static class Top2Func extends TableAggregateFunction<Tuple2<Double, Integer>, Top2Accumulator> {

        @Override
        public Top2Accumulator createAccumulator() {
            return new Top2Accumulator();
        }

        public void accumulate(Top2Accumulator acc, Double value) {
            if (value > acc.topOne) {
                acc.topTwo = acc.topOne;
                acc.topOne = value;
            } else if (value > acc.topTwo) {
                acc.topTwo = value;
            }
        }

        public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it) {
            for (Top2Accumulator otherAcc : it) {
                accumulate(acc, otherAcc.topOne);
                accumulate(acc, otherAcc.topTwo);
            }
        }

        public void emitValue(Top2Accumulator acc, Collector<Tuple2<Double, Integer>> out) {
            if (acc.topOne != Double.MIN_VALUE) {
                out.collect(Tuple2.of(acc.topOne, 1));
            }
            if (acc.topTwo != Double.MIN_VALUE) {
                out.collect(Tuple2.of(acc.topTwo, 2));
            }
        }
    }
}
