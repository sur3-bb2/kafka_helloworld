package kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class EnrichProcessorSupplier implements ProcessorSupplier<GenericRecord, GenericRecord, Void, Void> {
	@Override
	public Processor<GenericRecord, GenericRecord, Void, Void> get() {
		// TODO Auto-generated method stub
		return new EnrichProcessor();
	}
}
