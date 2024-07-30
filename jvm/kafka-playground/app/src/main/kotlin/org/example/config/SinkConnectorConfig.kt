package org.example.config

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance

class SinkConnectorConfig(props: Map<String, String>) : AbstractConfig(CONFIG, props) {
    companion object {
        const val SOURCE_TOPIC: String = "topics"
        const val SOURCE_TOPIC_DOC: String = "Define source topic"

        const val SINK_TOPIC: String = "kafka.sink.topic"
        const val SINK_TOPIC_DOC: String = "Define sink topic"

        const val SINK_BOOTSTRAP_SERVER: String = "kafka.sink.bootstrap"
        const val SINK_BOOTSTRAP_SERVER_DOC: String = "Define sink bootstrap"

        const val PRODUCER_MAX_REQUEST_SIZE: String = "producer.max.request.size"
        const val PRODUCER_MAX_REQUEST_SIZE_DEFAULT: Int = 1048576 // https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#max-request-size
        const val PRODUCER_MAX_REQUEST_SIZE_DOC: String = "producer.max.request.size"

        var CONFIG: ConfigDef = ConfigDef()
                .define(SOURCE_TOPIC, ConfigDef.Type.STRING, "", Importance.HIGH, SOURCE_TOPIC_DOC)
                .define(SINK_TOPIC, ConfigDef.Type.STRING, "", Importance.HIGH, SINK_TOPIC_DOC)
                .define(SINK_BOOTSTRAP_SERVER, ConfigDef.Type.STRING, "", Importance.HIGH, SINK_BOOTSTRAP_SERVER_DOC)
                .define(PRODUCER_MAX_REQUEST_SIZE, ConfigDef.Type.INT, PRODUCER_MAX_REQUEST_SIZE_DEFAULT, Importance.LOW, PRODUCER_MAX_REQUEST_SIZE_DOC)
    }
}