package kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

public class EnrichProcessor implements Processor<GenericRecord, GenericRecord, Void, Void> {
	@Override
	public void process(Record<GenericRecord, GenericRecord> record) {
		record.headers().add(new Header() {
			@Override
			public byte[] value() {
				// TODO Auto-generated method stub
				return "KStream".getBytes();
			}
			
			@Override
			public String key() {
				// TODO Auto-generated method stub
				return "KStream";
			}
		});
	}
}