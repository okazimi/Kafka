package avro.consumer;

import avro.properties.ProducerAndConsumerProperties;
import avro.topic.TopicCreator;
import com.example.AvroRecord;
import java.time.Duration;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {

  // MAIN THREAD TO TRIGGER CONSUMER CREATION AND RUN
  public static void main(String[] args) throws ExecutionException, InterruptedException {

    // INITIALIZE STRING ARRAY OF 3 CONSUMER GROUPS
    String[] consumerGroups = new String[3];

    // LOOP THROUGH AND INCREMENT i VALUE TO CREATE THREE DISTINCT CONSUMER GROUPS
    for (int i = 0; i < consumerGroups.length; i++) {
      consumerGroups[i] ="test-consumer-group-"+i;
    }
    // CALL THE RUN METHOD TO CREATE MULITHREADED CONSUMERS
    Consumer.run(3, consumerGroups);
  }

  // INITIALIZE VARIABLES
  private final static int PARTITION_COUNT = 3;
  private final static String TOPIC_NAME1 = "avroTesting3";
  private final static String TOPIC_NAME2 = "avroTesting4";
  private final static int MSG_COUNT = 4;
  private static int totalMsgToSend;
  private static AtomicInteger msg_received_counter = new AtomicInteger(0);

  // MULTI-THREADING CONSUMERS FUNCTION
  public static void run(int consumerCount, String[] consumerGroups) throws InterruptedException, ExecutionException {
    // CHECK TOPIC CREATION
    TopicCreator.createTopic(TOPIC_NAME1, PARTITION_COUNT);
    // CHECK TOPIC CREATION
    TopicCreator.createTopic(TOPIC_NAME2, PARTITION_COUNT);
    // USE TREE SET THAT EXTENDS SET INTERFACE TO COUNT THE NUMBER OF DISTINCT GROUPS A.K.A DOES NOT COUNT DUPLICATES
    int distinctGroups = new TreeSet<>(Arrays.asList(consumerGroups)).size();
    // CALCULATE TOTAL MESSAGES TO SEND BASED ON MSGCOUNT, PARTITION COUNT, GROUPS
    totalMsgToSend = MSG_COUNT * PARTITION_COUNT * distinctGroups;
    // INITIALIZE EXECUTOR SERVICE
    ExecutorService executorService = Executors.newFixedThreadPool(distinctGroups*consumerCount);
    // LOOP THROUGH EACH GROUP
    for (int j = 0; j < distinctGroups; j++) {
      // START 3 CONSUMERS FOR GROUP
      for (int i = 0; i < consumerCount; i++) {
        // SET A CONSUMER ID FOR EACH CONSUMER
        String consumerId = Integer.toString(i+1);
        // CREATE EFFECTIVELY FINAL i VARIABLE FOR BELOW LAMBDA EXPRESSION
        int finalJ = j;
        // START CONSUMER
        executorService.execute(() -> startConsumer(consumerId, consumerGroups[finalJ]));
      }
    }
    // SHUTDOWN EXECUTOR SERVICE
    executorService.shutdown();
    // WAIT 10 MIN TO SHUTDOWN
    executorService.awaitTermination(10, TimeUnit.MINUTES);
  }

  // START CONSUMER
  private static void startConsumer(String consumerId, String consumerGroup) {
    // PRINT OUT THE CURRENT CONSUMER THAT IS STARTING
    System.out.printf("Starting consumer: %s, Group: %s%n", consumerId, consumerGroup);
    // CREATE KAFKA CONSUMER AND SET PROPERTIES FOR CONSUMER
    KafkaConsumer<String, AvroRecord> consumer = new KafkaConsumer<>(
        ProducerAndConsumerProperties.getConsumerProperties(consumerGroup));
    // SUBSCRIBE TO TOPICS THAT COMPLY WITH PATTERN
    consumer.subscribe(Pattern.compile("[A-Za-z1-9].+"));
    // WHILE LOOP TO POLL AND OBTAIN RECORDS
    while (true) {
      // POLL FOR 2 SECONDS AND OBTAIN RECORDS
      ConsumerRecords<String,AvroRecord> records = consumer.poll(Duration.ofMillis(1000));
      // LOOP THROUGH EACH RECORD IN RECORDS
      for (ConsumerRecord<String,AvroRecord> record : records) {
        // INCREMENT MESSAGE RECEIVED COUNTER
        msg_received_counter.incrementAndGet();
        // PRINT OUT CONSUMER INFO
        System.out.printf("%nConsumer Info %nConsumer Group: %s%n Consumer ID: %s%n Topic: %s%n Header: Key = %s, Value = %s%n Partition ID = %s%n Key = %s%n Value = %s%n"
                + " Offset = %s%n",
            consumerGroup, consumerId, record.topic(), record.headers().iterator().next().key(), new String(record.headers().iterator().next().value()), record.partition(), record.key(), record.value(), record.offset());
      }
      // SYNCHRONOUS OFFSET COMMIT
      consumer.commitSync();
      // IF MESSAGES RECEIVED = TOTAL MESSAGES TO SEND BREAK
      if (msg_received_counter.get() == totalMsgToSend) {
        break;
      }
    }
  }

}
