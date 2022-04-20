package avro.producer;

import com.example.AvroRecord;
import avro.properties.ProducerAndConsumerProperties;
import avro.topic.TopicCreator;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {

  // MAIN THREAD TO TRIGGER PRODUCER TO SEND RECORDS
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Producer.sendMessages();
  }

  // SEND MESSAGE METHODS
  public static void sendMessages() throws ExecutionException, InterruptedException {

    // INITIALIZE VARIABLES
    int PARTITION_COUNT = 3;
    String TOPIC_NAME1 = "avroTesting3";
    String TOPIC_NAME2 = "avroTesting4";
    int MSG_COUNT = 4;

    // CHECK TOPIC CREATION
    TopicCreator.createTopic(TOPIC_NAME1, PARTITION_COUNT);

    // CHECK TOPIC CREATION
    TopicCreator.createTopic(TOPIC_NAME2, PARTITION_COUNT);

    // INITIALIZE KAFKA PRODUCER AND SET PRODUCER PROPERTIES
    KafkaProducer<String,AvroRecord> producer = new KafkaProducer<>(ProducerAndConsumerProperties.getProducerProperties());

    // INITIALIZE KEY VARIABLE
    int key = 0;
    // LOOP THROUGH EACH MESSAGE
    for (int i = 0; i < MSG_COUNT; i++) {
      // SEND THE MESSAGE TO EACH PARTITION
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        // INITIALIZE AVRO RECORDS AND SET FIELDS TO BE SENT
        AvroRecord avroRecord1 = AvroRecord.newBuilder().setFirstName("Omar").setLastName("Kazimi").build();
        // INCREMENTAL KEY VALUE
        key++;
        // PRINT OUT INFORMATION TO BE SENT
        System.out.printf("%nSending Message%nTopic: %s%n Key: %s%n Value: %s%n Partition ID: %s%n", TOPIC_NAME1, key, avroRecord1, partitionId);
        System.out.printf("%nSending Message%nTopic: %s%n Key: %s%n Value: %s%n Partition ID: %s%n", TOPIC_NAME2, key, avroRecord1, partitionId);

        try{
          // INITIALIZE PRODUCER RECORDS
          ProducerRecord<String,AvroRecord> topic1Record = new ProducerRecord<>(TOPIC_NAME1, partitionId, Integer.toString(key), avroRecord1);
          ProducerRecord<String,AvroRecord> topic2Record = new ProducerRecord<>(TOPIC_NAME2, partitionId, Integer.toString(key), avroRecord1);
          // ADD CUSTOM HEADERS TO RECORDS
          topic1Record.headers().add("City", ("Atlanta").getBytes());
          topic2Record.headers().add("City", ("Dallas").getBytes());
          // SEND RECORD
          producer.send(topic1Record);
          // SEND RECORD
          producer.send(topic2Record);
        } catch (SerializationException e) {
          System.out.println("Error in sending message");
          e.printStackTrace();
        }
      }
    }
    // CLOSE AND FLUSH PRODUCER
    producer.close();
  }

}
