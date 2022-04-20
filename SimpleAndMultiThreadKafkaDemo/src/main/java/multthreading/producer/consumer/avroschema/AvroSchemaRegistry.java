package multthreading.producer.consumer.avroschema;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class AvroSchemaRegistry {

  public static GenericRecord createSchema() throws IOException {
    // INITIALIZE SCHEMA PARSER
    Schema.Parser parser = new Schema.Parser();
    // PASS SCHEMA TO THE PARSER
    Schema schema = parser.parse(new File("C:/Users/955446/OneDrive - Cognizant/Documents/IntelliJ/IPMUdemyTraining/CognizantKafkaDemo/src/main/resources/AvroSchema.avsc"));
    // INITIALIZE GENERIC RECORD
    GenericRecord avroRecord = new GenericData.Record(schema);
    // RETURN GENERIC RECORD
    return avroRecord;
  }



}
