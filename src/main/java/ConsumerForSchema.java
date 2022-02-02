import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class ConsumerForSchema {

	public static void main(String[] args) {
		// Configuring Normal Consumer

				Properties properties = new Properties();
				properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
				properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

//		      DeSerializer Should be used as Kafka Avro DeSerializer
				properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
				properties.setProperty("specific.avro.reader", "true");

				properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "groupone");
				properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
				properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//		        The Actual Schema Registry Server URL
				properties.setProperty("schema.registry.url", "http://localhost:8081");

				KafkaConsumer<String, Test> kafkaConsumer = new KafkaConsumer<String,Test>(properties);

			String topic = "schema";
				
				 
				kafkaConsumer.subscribe(Collections.singleton(topic));

				
				while (true) {
					
					ConsumerRecords<String, Test> records = kafkaConsumer.poll(1000);

					for (ConsumerRecord<String, Test> record : records) {
						Test  test = record.value();
						System.out.println(test);
					}

					kafkaConsumer.commitSync();
				}
			}

		}