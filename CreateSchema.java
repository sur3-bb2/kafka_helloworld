package kafka;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public class CreateSchema {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, RestClientException {
		// schema registry url.
		String url = "http://172.29.0.212:8081";
		// associated topic name.
		String topic = "page-view-event";
		// avro schema avsc file path.
		String schemaPath = "C:\\Users\\suresh.babu\\eclipse-workspace\\kafka\\src\\page-view-event.avsc";
		// subject convention is "<topic-name>-value"
		String subject = topic + "-value";
		// avsc json string.
		String schema = null;

		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(schemaPath);
			schema = IOUtils.toString(inputStream);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
		    if(inputStream != null) inputStream.close();
		}

		Schema avroSchema = new Schema.Parser().parse(schema);

		CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(url, 20);

		client.register(subject, avroSchema);
		
		System.out.println("Created schema");
	}
}
