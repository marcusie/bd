package xyz.hiubo.bigdata;

import org.apache.kafka.clients.producer.*;
import com.opencsv.CSVReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaCsvProducer {

    private static final String TOPIC = "industry-job";
    private static final String CSV_FILE_PATH = "/home/hiubo/data/recruit/industry_job.csv";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            try (CSVReader reader = new CSVReader(new FileReader(CSV_FILE_PATH))) {
                int count = 0;
                String[] nextLine;
                while ((nextLine = reader.readNext()) != null && count++ < 1000) {
                    String industry = nextLine[0]; // 第一列是行业
                    producer.send(new ProducerRecord<>(TOPIC, industry));
                }
                System.out.println("Sent 1000 records to Kafka.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 5, TimeUnit.SECONDS);
    }
}