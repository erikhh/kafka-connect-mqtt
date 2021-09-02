package be.jovacon.kafka.connect;

import be.jovacon.kafka.connect.config.MQTTSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Converts a MQTT message to a Kafka message
 */
public class MQTTSourceConverter {

    private MQTTSourceConnectorConfig mqttSourceConnectorConfig;
    private String kafkaTopic;
    private Converter payloadConverter;

    private Logger log = LoggerFactory.getLogger(MQTTSourceConverter.class);

    public MQTTSourceConverter(MQTTSourceConnectorConfig mqttSourceConnectorConfig) {
        this.mqttSourceConnectorConfig = mqttSourceConnectorConfig;
        this.kafkaTopic = this.mqttSourceConnectorConfig.getString(MQTTSourceConnectorConfig.KAFKA_TOPIC);
        this.payloadConverter = mqttSourceConnectorConfig.getConfiguredInstance(MQTTSourceConnectorConfig.MQTT_PAYLOAD_CONVERTER, Converter.class);
    }

    protected SourceRecord convert(String topic, MqttMessage mqttMessage) {
        log.debug("Converting MQTT message: " + mqttMessage);
        // Kafka 2.3
        ConnectHeaders headers = new ConnectHeaders();
        headers.addInt("mqtt.message.id", mqttMessage.getId());
        headers.addInt("mqtt.message.qos", mqttMessage.getQos());
        headers.addBoolean("mqtt.message.duplicate", mqttMessage.isDuplicate());

        // Kafka 2.3
        final SchemaAndValue payload = payloadConverter.toConnectData(kafkaTopic, mqttMessage.getPayload());
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(),
                new HashMap<>(),
                kafkaTopic,
                (Integer) null,
                Schema.STRING_SCHEMA,
                topic,
                payload.schema(),
                payload.value(),
                System.currentTimeMillis(),
                headers);
        log.debug("Converted MQTT Message: " + sourceRecord);
        return sourceRecord;
    }
}
