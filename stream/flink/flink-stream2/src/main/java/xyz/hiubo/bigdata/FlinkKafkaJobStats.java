package xyz.hiubo.bigdata;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class FlinkKafkaJobStats {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-consumer-group");
        properties.setProperty("auto.offset.reset", "earliest");

        // 自定义 Kafka Deserialization Schema
        KafkaDeserializationSchema<String> deserializer = new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) {
                if (record.value() == null) {
                    return null;
                }
                return new String(record.value(), StandardCharsets.UTF_8);
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        };

        DataStream<String> kafkaStream = env.addSource(
                new FlinkKafkaConsumer<>("industry-job", deserializer, properties)
        );

        DataStream<Tuple2<String, Integer>> parsedStream = kafkaStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        String industry = value.trim();
                        if (!industry.isEmpty()) {
                            out.collect(Tuple2.of(industry, 1));
                        }
                    }
                });

        parsedStream.keyBy(value -> value.f0)
                .timeWindow(Time.seconds(5))
                .sum(1)
                .print();

        env.execute("Flink Kafka Industry Job Count");
    }
}