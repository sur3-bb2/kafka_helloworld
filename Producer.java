package kafka;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.*;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class Producer {
	public static void main(String[] args) throws Exception {
		// kafka broker list.
		String brokers = "172.29.0.212:9092";
		// schema registry url.
		String registry = "http://172.29.0.212:8081";
		Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		producerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
		producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
		producerProps.put(ProducerConfig.RETRIES_CONFIG, "0");
		producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "150");
		producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0");
		producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "500");
		producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
		
		// construct kafka producer.
		KafkaProducer<Integer, GenericRecord> producer = new KafkaProducer<Integer, GenericRecord>(producerProps);
		
		for(int i = 0; i <= 10; i++) {
			// message key.
			int userIdInt = 1;
			// message value, avro generic record.
			GenericRecord record = buildRecord();
			// send avro message to the topic page-view-event.
			producer.send(new ProducerRecord<Integer, GenericRecord>("page-view-event", userIdInt, record), 
					 new Callback() {
			            public void onCompletion(RecordMetadata metadata, Exception e) {
			                if(e != null) {
			                   e.printStackTrace();
			                } else {
			                   System.out.println("The offset of the record we just sent is: " + metadata.offset());
			                }
			            }
	        		});
			
			
			System.out.println("send record");
		}
		producer.flush();
		producer.close();
		
		System.out.println("produced records");
	}
	
	public static GenericRecord buildRecord() throws Exception {
	    // avro schema avsc file path.
	    String schemaPath = "C:\\Users\\suresh.babu\\eclipse-workspace\\kafka\\src\\page-view-event.avsc";
	    // avsc json string.
	    String schemaString = null;

	    FileInputStream inputStream = new FileInputStream(schemaPath);
	    try {
	        schemaString = IOUtils.toString(inputStream);
	    } finally {
	        inputStream.close();
	    }
	    // avro schema.
	    Schema schema = new Schema.Parser().parse(schemaString);   
	    // generic record for page-view-event.
	    GenericData.Record record = new GenericData.Record(schema);
	    // put the elements according to the avro schema.
	    record.put("itemId", "itemId1");
	    record.put("itemTitle", "simple item title");
	  
	    return record;
	}
}
