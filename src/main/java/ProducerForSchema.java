import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
//import schemaRegistry.Mobile;

public class ProducerForSchema {

	public static void main(String[] args) {		//creating properties
				Properties properties = new Properties();
				properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
				properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		        
//		      Serializer Should be used as Kafka Avro Serializer
		      
		      properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
//		    The Actual Schema Registry Server URL
		    properties.setProperty("schema.registry.url", "http://localhost:8081");
		    
		 //   phone phone = new phone("sandya","veerappa");
		    Test mobile = new Test("iphone","white",600000);
		    
		    
		    KafkaProducer<String,Test> producer = new KafkaProducer<String, Test>(properties);
		    ProducerRecord<String, Test> producerRecord = new ProducerRecord<String, Test>("schema",mobile);
		    
		    System.out.println(mobile);
		    
		//  Sending the Serialized Data to the Topic
		  
		  producer.send(producerRecord, new Callback(){
		      public void onCompletion(RecordMetadata metadata, Exception exception) {
		          if (exception == null) {
		              System.out.println(metadata);
		          } else {
		              exception.printStackTrace();
		          }
		      }
		  });
		  producer.flush();
		  producer.close();

		   

			}
}
		

