package xyz.hiubo.bigdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class CityJobCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 啟用 checkpoint 以支持持續監聽新文件
        env.enableCheckpointing(10000);

        // 使用 TextLineInputFormat 按行讀取文本文件
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("/home/hiubo/data/recruit/stream1")
                )
                .monitorContinuously(Duration.ofSeconds(5))
                .build();

        DataStream<String> lines = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source");

        // 解析每行為 (city, job, count)
        DataStream<Tuple3<String, String, Integer>> parsed = lines.flatMap(
                new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple3<String, String, Integer>> out) {
                        String[] parts = line.split(",");
                        if (parts.length == 2) {
                            String city = parts[0].trim();
                            String job = parts[1].trim();
                            out.collect(Tuple3.of(city, job, 1));
                        }
                    }
                }
        );

        // 按照 (city, job) 分組並統計數量
        parsed.keyBy(value -> value.f0 + ":" + value.f1)
                .sum(2)
                .print();

        env.execute("City Job Count Streaming");
    }
}