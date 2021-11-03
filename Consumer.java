package kafka;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class Consumer {
	private final static String TOPIC = "page-view-event";
    private final static String BOOTSTRAP_SERVERS =
            "172.29.0.212:9092";
    private final static String registry = "http://172.29.0.212:8081";
    
    private static KafkaConsumer<GenericRecord, GenericRecord> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                                    "KafkaExampleConsumer1");
        //props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        //        IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        		"io.confluent.kafka.serializers.KafkaAvroDeserializer");
        
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        		"io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", registry);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Create the consumer using props.
        final KafkaConsumer<GenericRecord, GenericRecord> consumer =
                                    new KafkaConsumer<GenericRecord, GenericRecord>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        
        return consumer;
    }
    
    public static void main(String[] args) throws InterruptedException {
        final KafkaConsumer<GenericRecord, GenericRecord> consumer = createConsumer();

        final int giveUp = 100;   int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<GenericRecord, GenericRecord> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
                printHeaders(record);
            });

            consumer.commitAsync();
        }
        consumer.close();
        
        System.out.println("DONE");
    }
    
    private static void printHeaders(final ConsumerRecord<?, ?> consumerRecord) {
        final Map<String, String> attributes = new HashMap<>();
        
        for (final Header header : consumerRecord.headers()) {
        	if(header.key().contains("CorelationId")) {
            System.out.printf("Header:(%s %s)\n",
            		header.key(), asUuid(header.value()));
        	} else {
        		System.out.printf("Header:(%s %s)\n",
                		header.key(), new String(header.value()));
        	}
        }
    }
    
    private static UUID asUuid(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long firstLong = bb.getLong();
        long secondLong = bb.getLong();
        return new UUID(firstLong, secondLong);
      }
}
