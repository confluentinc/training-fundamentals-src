package clients;

import java.lang.InterruptedException;
import java.lang.Math;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BasicProducer {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("*** Starting Readings Producer ***");

        Properties settings = new Properties();
        settings.put("client.id", "readings-producer");
        settings.put("bootstrap.servers", "kafka:9092");
        settings.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        settings.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        settings.put("interceptor.classes", "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

        final KafkaProducer<String, String> producer = new KafkaProducer<>(settings);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("### Stopping Readings Producer ###");
            producer.close();
        }));

        final String topic = "readings";
        final int numberOfStations = 500;
        final double min = -20.0;
        final double max = 20.0;
        final double[] avgTemp = new double[numberOfStations];
        for(int i=0; i<numberOfStations; i++){
            avgTemp[i] = (Math.random() * ((max - min) + 1)) + min;
        }
        while(true){
            for(int i=1; i<=numberOfStations; i++){
                final String key = "station-" + i;
                final double temp = avgTemp[i-1] + ((Math.random()-0.5) * 20);
                final String value = Double.toString(temp);
                final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record);
            }
            final int interval = (int)Math.random() * 1000;     // wait a fraction of a second
            TimeUnit.MILLISECONDS.sleep(interval);
        }
    }
}
