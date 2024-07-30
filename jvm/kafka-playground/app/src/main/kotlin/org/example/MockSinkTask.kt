package org.example

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.util.*

class MockSinkTask {
    private lateinit var connectorName: String
    private lateinit var sinkTopic: String
    private lateinit var producer: KafkaProducer<String, String>
    private val objectMapper = ObjectMapper()
    private val logger: Logger = LogManager.getLogger(SinkTask::class.java)

    constructor(connectorName: String, sinkTopic: String) {
        this.connectorName = connectorName
        this.sinkTopic = sinkTopic
        val producerProps = Properties()
//        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "http://kafka-connect-connect.streaming:8083"
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "kafka-kafka-bootstrap.streaming.svc.cluster.local:9092"
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
//        producerProps[ProducerConfig.MAX_REQUEST_SIZE_CONFIG] = ""

        // NOTE: 커넥트 프로듀서 각각 고유해야한다. Multi Task 는 고려되지 않았다.
//        producerProps[ProducerConfig.TRANSACTIONAL_ID_CONFIG] = String.format("kafka-sink-connector-%s", connectorName)

        producer = KafkaProducer(producerProps)

//        producer.initTransactions()
    }

    fun put(records: Collection<SinkRecord>) {
//        producer.beginTransaction()
        try {
            for (record in records) {
                val after = transform(record)
                val key = createSchemaPayloadJson(record.key(), after.keySchema())
                val value = createSchemaPayloadJson(record.value(), after.valueSchema())
//                print(String.format("\"gogo\", sinkTopic: %s", sinkTopic))
                producer.send(ProducerRecord(sinkTopic, key, value)) { metadata, exception ->
                    if (exception == null) {
                        println("Sent record(key=${record.key()}, value=${record.value()}) " +
                                "meta(partition=${metadata.partition()}, offset=${metadata.offset()})")
                    } else {
                        exception.printStackTrace()
                    }
                }
            }
//            producer.commitTransaction()
        } catch (e: Exception) {
//            logger.error(e.message + " / " + connectorName, e)
//            producer.abortTransaction()
            producer.close()
            throw e
        }
    }

    // NOTE: 원본 메시지를 원하는 형태로 변환한다. Debezium, KafakEvent decorator 메시지 형태에 의존한다.
    private fun transform(record: SinkRecord): SinkRecord {
        val valueStruct = record.value() as Struct
        val afterStruct = valueStruct.getStruct("after")
        val metadataStruct = afterStruct.getString("metadata")

        // NOTE: metadata 존재하면 metadata 내용을 기반으로 메시지에 변형을 가한다.
        if (metadataStruct != null) {
            val metadata = objectMapper.readTree(metadataStruct)

            // NOTE: 1. EventType prefix 설정 (InvoiceConfirmEvent -> PaymentInvoiceConfirmEvent)
            val prefix = metadata["prefix"]?.asText()
            if (!prefix.isNullOrEmpty()) {
                val type = afterStruct.getString("type")
                afterStruct.put("type", String.format("%s%s", prefix, type))
            }
        }

        return SinkRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                valueStruct,
                record.kafkaOffset()
        )
    }

    private fun createSchemaPayloadJson(data: Any, schema: Schema): String {
        val schemaMap = convertSchemaToJson(schema)
        val payload = convertDataToJson(data)
        val resultMap = mapOf("schema" to schemaMap, "payload" to payload)
        return objectMapper.writeValueAsString(resultMap)
    }

    private fun convertDataToJson(data: Any?): Any? {
        return when (data) {
            is Struct -> structToMap(data)
            else -> data
        }
    }

    private fun structToMap(struct: Struct): Map<String, Any?> {
        val map = mutableMapOf<String, Any?>()
        val schema = struct.schema()
        for (field in schema.fields()) {
            val fieldValue = struct.get(field)
            map[field.name()] = if (fieldValue is Struct) structToMap(fieldValue) else fieldValue
        }
        return map
    }

    private fun convertSchemaToJson(schema: Schema): Map<String, Any?> {
        val schemaMap = mutableMapOf<String, Any?>()
        schemaMap["type"] = schema.type().name.lowercase()
        schemaMap["name"] = schema.name()
        schemaMap["version"] = schema.version()
        schemaMap["parameters"] = schema.parameters()
        schemaMap["default"] = schema.defaultValue()
        schemaMap["optional"] = schema.isOptional
        if (schema.type() == Schema.Type.STRUCT) {
            val fields = schema.fields().map { field ->
                val fieldMap = convertSchemaToJson(field.schema()).toMutableMap()
                fieldMap["field"] = field.name()
                fieldMap
            }
            schemaMap["fields"] = fields
        }

        return schemaMap.filterValues { it != null }
    }

    fun createMockSinkRecord(jsonString: String): SinkRecord {
        // JSON 파싱
        val jsonObject: JsonNode = objectMapper.readTree(jsonString)
        val payload = jsonObject["payload"]
        val after = payload["after"]
        val source = payload["source"]

        // Schema 정의
        val afterSchema: Schema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("type", Schema.STRING_SCHEMA)
                .field("occurredAt", Schema.INT64_SCHEMA)
                .field("txId", Schema.STRING_SCHEMA)
                .field("createdAt", Schema.INT64_SCHEMA)
                .field("updatedAt", Schema.INT64_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("metadata", Schema.STRING_SCHEMA)
                .field("actorId", Schema.STRING_SCHEMA)
                .build()

        val sourceSchema: Schema = SchemaBuilder.struct()
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("snapshot", Schema.OPTIONAL_STRING_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.OPTIONAL_STRING_SCHEMA)
                .field("server_id", Schema.INT64_SCHEMA)
                .field("gtid", Schema.OPTIONAL_STRING_SCHEMA)
                .field("file", Schema.STRING_SCHEMA)
                .field("pos", Schema.INT64_SCHEMA)
                .field("row", Schema.INT32_SCHEMA)
                .field("thread", Schema.OPTIONAL_INT64_SCHEMA)
                .field("query", Schema.OPTIONAL_STRING_SCHEMA)
                .build()

        val envelopeSchema: Schema = SchemaBuilder.struct()
                .field("after", afterSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                .build()

        // Struct 생성
        val afterStruct = Struct(afterSchema)
                .put("id", after["id"].asLong())
                .put("type", after["type"].asText())
                .put("occurredAt", after["occurredAt"].asLong())
                .put("txId", after["txId"].asText())
                .put("createdAt", after["createdAt"].asLong())
                .put("updatedAt", after["updatedAt"].asLong())
                .put("data", after["data"].asText())
                .put("metadata", after["metadata"]?.asText() ?: "")
                .put("actorId", after["actorId"].asText())

        val sourceStruct = Struct(sourceSchema)
                .put("version", source["version"].asText())
                .put("connector", source["connector"].asText())
                .put("name", source["name"].asText())
                .put("ts_ms", source["ts_ms"].asLong())
                .put("snapshot", source["snapshot"]?.asText())
                .put("db", source["db"].asText())
                .put("sequence", source["sequence"]?.asText())
                .put("table", source["table"]?.asText())
                .put("server_id", source["server_id"].asLong())
                .put("gtid", source["gtid"]?.asText() ?: null)
                .put("file", source["file"].asText())
                .put("pos", source["pos"].asLong())
                .put("row", source["row"].asInt())
                .put("thread", source["thread"]?.asLong())
                .put("query", source["query"]?.asText())

        val envelopeStruct = Struct(envelopeSchema)
                .put("after", afterStruct)
                .put("source", sourceStruct)
                .put("op", payload["op"].asText())
                .put("ts_ms", payload["ts_ms"]?.asLong())


        // Key Schema 정의 (예: "id"를 키로 사용)
        val keySchema: Schema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .build()

        // Key Struct 생성
        val keyStruct = Struct(keySchema)
                .put("id", after["id"].asLong())

        // SinkRecord 생성
        return SinkRecord(
                "topic",   // Kafka 토픽 이름
                0,         // 파티션 번호
                keySchema, // 키 스키마
                keyStruct, // 키
                envelopeSchema, // 값 스키마
                envelopeStruct, // 값
                0L,        // 오프셋
                System.currentTimeMillis(), // 타임스탬프
                TimestampType.CREATE_TIME  // 타임스탬프 타입
        )
    }

    fun test1(): Collection<SinkRecord> {
        return listOf(createMockSinkRecord("""{
	"schema": {
		"type": "struct",
		"fields": [
			{
				"type": "struct",
				"fields": [
					{
						"type": "int64",
						"optional": false,
						"field": "id"
					},
					{
						"type": "string",
						"optional": false,
						"field": "type"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.Timestamp",
						"version": 1,
						"field": "occurredAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "txId"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "createdAt"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "updatedAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "data"
					},
					{
						"type": "string",
						"optional": false,
						"field": "actorId"
					}
				],
				"optional": true,
				"name": "debezium.haulla.ddd_event.Value",
				"field": "before"
			},
			{
				"type": "struct",
				"fields": [
					{
						"type": "int64",
						"optional": false,
						"field": "id"
					},
					{
						"type": "string",
						"optional": false,
						"field": "type"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.Timestamp",
						"version": 1,
						"field": "occurredAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "txId"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "createdAt"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "updatedAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "data"
					},
					{
						"type": "string",
						"optional": false,
						"field": "actorId"
					}
				],
				"optional": true,
				"name": "debezium.haulla.ddd_event.Value",
				"field": "after"
			},
			{
				"type": "struct",
				"fields": [
					{
						"type": "string",
						"optional": false,
						"field": "version"
					},
					{
						"type": "string",
						"optional": false,
						"field": "connector"
					},
					{
						"type": "string",
						"optional": false,
						"field": "name"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "ts_ms"
					},
					{
						"type": "string",
						"optional": true,
						"name": "io.debezium.data.Enum",
						"version": 1,
						"parameters": {
							"allowed": "true,last,false,incremental"
						},
						"default": "false",
						"field": "snapshot"
					},
					{
						"type": "string",
						"optional": false,
						"field": "db"
					},
					{
						"type": "string",
						"optional": true,
						"field": "sequence"
					},
					{
						"type": "string",
						"optional": true,
						"field": "table"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "server_id"
					},
					{
						"type": "string",
						"optional": true,
						"field": "gtid"
					},
					{
						"type": "string",
						"optional": false,
						"field": "file"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "pos"
					},
					{
						"type": "int32",
						"optional": false,
						"field": "row"
					},
					{
						"type": "int64",
						"optional": true,
						"field": "thread"
					},
					{
						"type": "string",
						"optional": true,
						"field": "query"
					}
				],
				"optional": false,
				"name": "io.debezium.connector.mysql.Source",
				"field": "source"
			},
			{
				"type": "string",
				"optional": false,
				"field": "op"
			},
			{
				"type": "int64",
				"optional": true,
				"field": "ts_ms"
			},
			{
				"type": "struct",
				"fields": [
					{
						"type": "string",
						"optional": false,
						"field": "id"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "total_order"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "data_collection_order"
					}
				],
				"optional": true,
				"name": "event.block",
				"version": 1,
				"field": "transaction"
			}
		],
		"optional": false,
		"name": "debezium.haulla.ddd_event.Envelope",
		"version": 1
	},
	"payload": {
		"before": null,
		"after": {
			"id": 1304926,
			"type": "ReplicateServiceCommand",
			"occurredAt": 1721884234000,
			"txId": "d02352dd-9ea9-4ce8-a936-b83aec160e73",
			"createdAt": 1721884234070590,
			"updatedAt": 1721884234070590,
            "metadata": "{\"topic\":\"Test\",\"prefix\":\"Payment\",\"source\":\"Payment\"}",
			"data": "{}",
			"actorId": "ow3QKY8hyY"
		},
		"source": {
			"version": "2.1.3.Final",
			"connector": "mysql",
			"name": "debezium",
			"ts_ms": 1721884234000,
			"snapshot": "false",
			"db": "haulla",
			"sequence": null,
			"table": "ddd_event",
			"server_id": 1132716479,
			"gtid": null,
			"file": "mysql-bin-changelog.001162",
			"pos": 125028869,
			"row": 0,
			"thread": 854067,
			"query": null
		},
		"op": "c",
		"ts_ms": 1721884234076,
		"transaction": null
	}
}"""), createMockSinkRecord("""{
	"schema": {
		"type": "struct",
		"fields": [
			{
				"type": "struct",
				"fields": [
					{
						"type": "int64",
						"optional": false,
						"field": "id"
					},
					{
						"type": "string",
						"optional": false,
						"field": "type"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.Timestamp",
						"version": 1,
						"field": "occurredAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "txId"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "createdAt"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "updatedAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "data"
					},
					{
						"type": "string",
						"optional": false,
						"field": "actorId"
					}
				],
				"optional": true,
				"name": "debezium.haulla.ddd_event.Value",
				"field": "before"
			},
			{
				"type": "struct",
				"fields": [
					{
						"type": "int64",
						"optional": false,
						"field": "id"
					},
					{
						"type": "string",
						"optional": false,
						"field": "type"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.Timestamp",
						"version": 1,
						"field": "occurredAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "txId"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "createdAt"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "updatedAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "data"
					},
					{
						"type": "string",
						"optional": false,
						"field": "actorId"
					}
				],
				"optional": true,
				"name": "debezium.haulla.ddd_event.Value",
				"field": "after"
			},
			{
				"type": "struct",
				"fields": [
					{
						"type": "string",
						"optional": false,
						"field": "version"
					},
					{
						"type": "string",
						"optional": false,
						"field": "connector"
					},
					{
						"type": "string",
						"optional": false,
						"field": "name"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "ts_ms"
					},
					{
						"type": "string",
						"optional": true,
						"name": "io.debezium.data.Enum",
						"version": 1,
						"parameters": {
							"allowed": "true,last,false,incremental"
						},
						"default": "false",
						"field": "snapshot"
					},
					{
						"type": "string",
						"optional": false,
						"field": "db"
					},
					{
						"type": "string",
						"optional": true,
						"field": "sequence"
					},
					{
						"type": "string",
						"optional": true,
						"field": "table"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "server_id"
					},
					{
						"type": "string",
						"optional": true,
						"field": "gtid"
					},
					{
						"type": "string",
						"optional": false,
						"field": "file"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "pos"
					},
					{
						"type": "int32",
						"optional": false,
						"field": "row"
					},
					{
						"type": "int64",
						"optional": true,
						"field": "thread"
					},
					{
						"type": "string",
						"optional": true,
						"field": "query"
					}
				],
				"optional": false,
				"name": "io.debezium.connector.mysql.Source",
				"field": "source"
			},
			{
				"type": "string",
				"optional": false,
				"field": "op"
			},
			{
				"type": "int64",
				"optional": true,
				"field": "ts_ms"
			},
			{
				"type": "struct",
				"fields": [
					{
						"type": "string",
						"optional": false,
						"field": "id"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "total_order"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "data_collection_order"
					}
				],
				"optional": true,
				"name": "event.block",
				"version": 1,
				"field": "transaction"
			}
		],
		"optional": false,
		"name": "debezium.haulla.ddd_event.Envelope",
		"version": 1
	},
	"payload": {
		"before": null,
		"after": {
			"id": 1304926,
			"type": "ReplicateServiceCommand",
			"occurredAt": 1721884234000,
			"txId": "d02352dd-9ea9-4ce8-a936-b83aec160e73",
			"createdAt": 1721884234070590,
			"updatedAt": 1721884234070590,
			"data": "{}",
			"actorId": "ow3QKY8hyY"
		},
		"source": {
			"version": "2.1.3.Final",
			"connector": "mysql",
			"name": "debezium",
			"ts_ms": 1721884234000,
			"snapshot": "false",
			"db": "haulla",
			"sequence": null,
			"table": "ddd_event",
			"server_id": 1132716479,
			"gtid": null,
			"file": "mysql-bin-changelog.001162",
			"pos": 125028869,
			"row": 0,
			"thread": 854067,
			"query": null
		},
		"op": "c",
		"ts_ms": 1721884234076,
		"transaction": null
	}
}"""), createMockSinkRecord("""{
	"schema": {
		"type": "struct",
		"fields": [
			{
				"type": "struct",
				"fields": [
					{
						"type": "int64",
						"optional": false,
						"field": "id"
					},
					{
						"type": "string",
						"optional": false,
						"field": "type"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.Timestamp",
						"version": 1,
						"field": "occurredAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "txId"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "createdAt"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "updatedAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "data"
					},
					{
						"type": "string",
						"optional": false,
						"field": "actorId"
					}
				],
				"optional": true,
				"name": "debezium.haulla.ddd_event.Value",
				"field": "before"
			},
			{
				"type": "struct",
				"fields": [
					{
						"type": "int64",
						"optional": false,
						"field": "id"
					},
					{
						"type": "string",
						"optional": false,
						"field": "type"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.Timestamp",
						"version": 1,
						"field": "occurredAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "txId"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "createdAt"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "updatedAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "data"
					},
					{
						"type": "string",
						"optional": false,
						"field": "actorId"
					}
				],
				"optional": true,
				"name": "debezium.haulla.ddd_event.Value",
				"field": "after"
			},
			{
				"type": "struct",
				"fields": [
					{
						"type": "string",
						"optional": false,
						"field": "version"
					},
					{
						"type": "string",
						"optional": false,
						"field": "connector"
					},
					{
						"type": "string",
						"optional": false,
						"field": "name"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "ts_ms"
					},
					{
						"type": "string",
						"optional": true,
						"name": "io.debezium.data.Enum",
						"version": 1,
						"parameters": {
							"allowed": "true,last,false,incremental"
						},
						"default": "false",
						"field": "snapshot"
					},
					{
						"type": "string",
						"optional": false,
						"field": "db"
					},
					{
						"type": "string",
						"optional": true,
						"field": "sequence"
					},
					{
						"type": "string",
						"optional": true,
						"field": "table"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "server_id"
					},
					{
						"type": "string",
						"optional": true,
						"field": "gtid"
					},
					{
						"type": "string",
						"optional": false,
						"field": "file"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "pos"
					},
					{
						"type": "int32",
						"optional": false,
						"field": "row"
					},
					{
						"type": "int64",
						"optional": true,
						"field": "thread"
					},
					{
						"type": "string",
						"optional": true,
						"field": "query"
					}
				],
				"optional": false,
				"name": "io.debezium.connector.mysql.Source",
				"field": "source"
			},
			{
				"type": "string",
				"optional": false,
				"field": "op"
			},
			{
				"type": "int64",
				"optional": true,
				"field": "ts_ms"
			},
			{
				"type": "struct",
				"fields": [
					{
						"type": "string",
						"optional": false,
						"field": "id"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "total_order"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "data_collection_order"
					}
				],
				"optional": true,
				"name": "event.block",
				"version": 1,
				"field": "transaction"
			}
		],
		"optional": false,
		"name": "debezium.haulla.ddd_event.Envelope",
		"version": 1
	},
	"payload": {
		"before": null,
		"after": {
			"id": 1304926,
			"type": "ReplicateServiceCommand",
			"occurredAt": 1721884234000,
			"txId": "d02352dd-9ea9-4ce8-a936-b83aec160e73",
			"createdAt": 1721884234070590,
			"updatedAt": 1721884234070590,
            "metadata": "{\"topic\":\"Test\",\"source\":\"Payment\"}",
			"data": "{}",
			"actorId": "ow3QKY8hyY"
		},
		"source": {
			"version": "2.1.3.Final",
			"connector": "mysql",
			"name": "debezium",
			"ts_ms": 1721884234000,
			"snapshot": "false",
			"db": "haulla",
			"sequence": null,
			"table": "ddd_event",
			"server_id": 1132716479,
			"gtid": null,
			"file": "mysql-bin-changelog.001162",
			"pos": 125028869,
			"row": 0,
			"thread": 854067,
			"query": null
		},
		"op": "c",
		"ts_ms": 1721884234076,
		"transaction": null
	}
}"""), createMockSinkRecord("""{
	"schema": {
		"type": "struct",
		"fields": [
			{
				"type": "struct",
				"fields": [
					{
						"type": "int64",
						"optional": false,
						"field": "id"
					},
					{
						"type": "string",
						"optional": false,
						"field": "type"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.Timestamp",
						"version": 1,
						"field": "occurredAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "txId"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "createdAt"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "updatedAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "data"
					},
					{
						"type": "string",
						"optional": false,
						"field": "actorId"
					}
				],
				"optional": true,
				"name": "debezium.haulla.ddd_event.Value",
				"field": "before"
			},
			{
				"type": "struct",
				"fields": [
					{
						"type": "int64",
						"optional": false,
						"field": "id"
					},
					{
						"type": "string",
						"optional": false,
						"field": "type"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.Timestamp",
						"version": 1,
						"field": "occurredAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "txId"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "createdAt"
					},
					{
						"type": "int64",
						"optional": false,
						"name": "io.debezium.time.MicroTimestamp",
						"version": 1,
						"default": 0,
						"field": "updatedAt"
					},
					{
						"type": "string",
						"optional": false,
						"field": "data"
					},
					{
						"type": "string",
						"optional": false,
						"field": "actorId"
					}
				],
				"optional": true,
				"name": "debezium.haulla.ddd_event.Value",
				"field": "after"
			},
			{
				"type": "struct",
				"fields": [
					{
						"type": "string",
						"optional": false,
						"field": "version"
					},
					{
						"type": "string",
						"optional": false,
						"field": "connector"
					},
					{
						"type": "string",
						"optional": false,
						"field": "name"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "ts_ms"
					},
					{
						"type": "string",
						"optional": true,
						"name": "io.debezium.data.Enum",
						"version": 1,
						"parameters": {
							"allowed": "true,last,false,incremental"
						},
						"default": "false",
						"field": "snapshot"
					},
					{
						"type": "string",
						"optional": false,
						"field": "db"
					},
					{
						"type": "string",
						"optional": true,
						"field": "sequence"
					},
					{
						"type": "string",
						"optional": true,
						"field": "table"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "server_id"
					},
					{
						"type": "string",
						"optional": true,
						"field": "gtid"
					},
					{
						"type": "string",
						"optional": false,
						"field": "file"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "pos"
					},
					{
						"type": "int32",
						"optional": false,
						"field": "row"
					},
					{
						"type": "int64",
						"optional": true,
						"field": "thread"
					},
					{
						"type": "string",
						"optional": true,
						"field": "query"
					}
				],
				"optional": false,
				"name": "io.debezium.connector.mysql.Source",
				"field": "source"
			},
			{
				"type": "string",
				"optional": false,
				"field": "op"
			},
			{
				"type": "int64",
				"optional": true,
				"field": "ts_ms"
			},
			{
				"type": "struct",
				"fields": [
					{
						"type": "string",
						"optional": false,
						"field": "id"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "total_order"
					},
					{
						"type": "int64",
						"optional": false,
						"field": "data_collection_order"
					}
				],
				"optional": true,
				"name": "event.block",
				"version": 1,
				"field": "transaction"
			}
		],
		"optional": false,
		"name": "debezium.haulla.ddd_event.Envelope",
		"version": 1
	},
	"payload": {
		"before": null,
		"after": {
			"id": 1304926,
			"type": "ReplicateServiceCommand",
			"occurredAt": 1721884234000,
			"txId": "d02352dd-9ea9-4ce8-a936-b83aec160e73",
			"createdAt": 1721884234070590,
			"updatedAt": 1721884234070590,
            "metadata": "{\"topic\":\"Test\",\"prefix\":\"\",\"source\":\"Payment\"}",
			"data": "{}",
			"actorId": "ow3QKY8hyY"
		},
		"source": {
			"version": "2.1.3.Final",
			"connector": "mysql",
			"name": "debezium",
			"ts_ms": 1721884234000,
			"snapshot": "false",
			"db": "haulla",
			"sequence": null,
			"table": "ddd_event",
			"server_id": 1132716479,
			"gtid": null,
			"file": "mysql-bin-changelog.001162",
			"pos": 125028869,
			"row": 0,
			"thread": 854067,
			"query": null
		},
		"op": "c",
		"ts_ms": 1721884234076,
		"transaction": null
	}
}"""))
    }
}


