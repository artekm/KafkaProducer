package artur;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class KafkaProducerAM {
    private static final String DICTIONARY = "crsto10.txt";
    private static final String PROPERTIES = "producer.properties";
    private static final String TOPIC = "samsung";

    public static void main(String[] args) throws Exception {
        List<String> words = Files.readAllLines(Paths.get(DICTIONARY));

        Properties properties = new Properties();
        properties.load(new FileInputStream(PROPERTIES));

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        System.out.println("Press ENTER to start");
        new Scanner(System.in).nextLine();

        for (String word : words) {
            System.out.println("Sending " + word);
            producer.send(new ProducerRecord<>(TOPIC, word));
            Thread.sleep(500);
        }
    }
}
