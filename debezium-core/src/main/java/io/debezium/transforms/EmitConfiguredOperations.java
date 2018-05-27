package io.debezium.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;

public class EmitConfiguredOperations<R extends ConnectRecord<R>> implements Transformation<R> {

	private String operationsIncluded;
	private String operationsExcluded;

	private static final String OPERATION_WHITELIST_NAME = "operation.whitelist";
	private static final String OPERATION_BLACKLIST_NAME = "operation.blacklist";

	/**
	 * A comma separated list of operations that are to be monitored. May not be
	 * used with {@link #OPERATION_WHITELIST}
	 */
	public static final Field OPERATION_BLACKLIST = Field.create(OPERATION_BLACKLIST_NAME)
			.withDisplayName("operations")
			.withType(Type.LIST)
			.withWidth(Width.LONG)
			.withImportance(Importance.LOW)
			.withDescription("The operations for which changes are to be excluded");

	/**
	 * A comma separated list of operations that are to be monitored. May not be
	 * used with {@link #OPERATION_BLACKLIST}
	 */
	public static final Field OPERATION_WHITELIST = Field.create(OPERATION_WHITELIST_NAME)
			.withDisplayName("operations")
			.withType(Type.LIST)
			.withWidth(Width.LONG)
			.withImportance(Importance.LOW)
			.withDescription("The operations for which changes are to be captured");

	@Override
	public void configure(Map<String, ?> configs) {

		final Configuration config = Configuration.from(configs);
		operationsIncluded = config.getString(OPERATION_WHITELIST);
		operationsExcluded = config.getString(OPERATION_BLACKLIST);
	}

	@Override
	public R apply(R record) {
		if (record.value() != null) {
			final Operation operation = Envelope.operationFor(record);
			if (operationsExcluded.contains(operation.code()) || !operationsIncluded.contains(operation.code()))
				return null;
		}
		return record;
	}

	@Override
	public ConfigDef config() {
		return null;
	}

	@Override
	public void close() {

	}

}
