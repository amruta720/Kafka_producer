import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerWithKeyDemo {

    public static void main(String[] args) throws IOException {
        // Load producer properties from file
        Properties properties = loadProperties("config.txt");

        // Create Kafka producer
        Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);

        // Example topic and message
        String topic = "topic_3";
        String message = "Hello, Kafka!";
        String file = "C:\\Users\\Amruta\\IdeaProjects\\Kafka_producer\\src\\main\\resources\\product_data.csv";
        String data;
        String[] csvValues = new String[0];
        Schema avroSchema = readAvroSchema("C:\\Users\\Amruta\\IdeaProjects\\Kafka_producer\\src\\main\\resources\\schema.avsc");

        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))){
            while ((data=br.readLine())!=null){
                System.out.println(data);
                csvValues = data.split("\\|");
                GenericRecord avroRecord = new GenericData.Record(avroSchema);
                System.out.println(csvValues);
                avroRecord.put("device_id", csvValues[0]);
                avroRecord.put("upc", csvValues[1]);
                System.out.println(avroRecord);
                // Produce a message to the topic
                ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, avroRecord);
                try {
                    producer.send(record).get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        System.out.println(csvValues);




        // Close the producer
        producer.close();
    }

    private static Properties loadProperties(String fileName) {
        Properties properties = new Properties();
        try (InputStream input = KafkaProducerWithKeyDemo.class.getClassLoader().getResourceAsStream(fileName)) {
            if (input == null) {
                System.out.println("Sorry BABU, unable to find " + fileName);
                return properties;
            }

            // Load a properties file from class path
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return properties;
    }
    private static Schema readAvroSchema(String filePath) throws IOException {
        // Parse the Avro schema from the file
        Parser parser = new Schema.Parser();
        File file = new File(filePath);

        return parser.parse(file);
    }
}
