package kafka;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

// https://docs.confluent.io/platform/current/streams/code-examples.html

public class Streams {
	public static void main(final String[] args) {
	    final String bootstrapServers = args.length > 0 ? args[0] : "172.29.0.212:9092";
	    final Properties streamsConfiguration = new Properties();
	    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
	    // against which the application is run.
	    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-lambda-example");
	    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "map-function-lambda-example-client");
	    // Where to find Kafka broker(s).
	    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    // Specify default (de)serializers for record keys and for record values.
	    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
	    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
	    streamsConfiguration.put("schema.registry.url", "http://172.29.0.212:8081");
	    
	 // When you want to override serdes explicitly/selectively
	    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
	                                                                     "http://172.29.0.212:8081");
	    final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
	    keyGenericAvroSerde.configure(serdeConfig, true); // `true` for record keys
	    final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
	    valueGenericAvroSerde.configure(serdeConfig, false); // `false` for record values

	    StreamsBuilder builder = new StreamsBuilder();
	    KStream<GenericRecord, GenericRecord> pageEvents =
	      builder.stream("page-view-event", Consumed.with(keyGenericAvroSerde, valueGenericAvroSerde));
	    
	    final KStream<GenericRecord, GenericRecord> filteredRecords = 
	    pageEvents.filter((k, v) -> {
	    	System.out.println((int) k.get("id"));
	    	
	    	if((int) k.get("id") > 5) return true;
	    	return false;
	    });
	    
	    filteredRecords.process(new EnrichProcessorSupplier());

	    // Write (i.e. persist) the results to a new Kafka topic called "UppercasedTextLinesTopic".
	    //
	    // In this case we can rely on the default serializers for keys and values because their data
	    // types did not change, i.e. we only need to provide the name of the output topic.
	    filteredRecords.to("page-view-event-stream");

	    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
	    // Always (and unconditionally) clean local state prior to starting the processing topology.
	    // We opt for this unconditional call here because this will make it easier for you to play around with the example
	    // when resetting the application for doing a re-run (via the Application Reset Tool,
	    // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
	    //
	    // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
	    // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
	    // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
	    // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
	    // See `ApplicationResetExample.java` for a production-like example.
	    streams.cleanUp();
	    streams.start();

	    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
	    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	  }
}