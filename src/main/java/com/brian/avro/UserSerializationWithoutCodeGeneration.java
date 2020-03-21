package com.brian.avro;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Without code generation, GenericData can be used to create data, serialize/deserialized data.
 * https://avro.apache.org/docs/1.9.2/gettingstartedjava.html
 */
public class UserSerializationWithoutCodeGeneration {

    private static final Logger logger = LoggerFactory.getLogger(UserSerializationWithoutCodeGeneration.class);

    private Schema schema;
    private GenericRecord user1;
    private GenericRecord user2;
    private File file;

    public void createUserWithSchema() throws IOException {

        this.schema = new Schema.Parser().parse(new File("/Users/bmoon/Documents/mygit/kafka_basics/src/main/avro/user-schema.avsc"));

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);
        // Leave favorite color null
        logger.info("user1: {}", user1);
        this.user1 = user1;

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");
        logger.info("user2: {}", user2);
        this.user2 = user2;
    }

    public void serializeWithSchemaOnly() throws IOException {
        // Serialize user1 and user 2 to disk
        file = new File("usersWithoutCodeGeneration.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
    }

    public void deserializeWithSchemaOnly() throws IOException {
        // Deserialize users from disk
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next();
            logger.info("Deserialized User: {}", user);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        UserSerializationWithoutCodeGeneration demo = new UserSerializationWithoutCodeGeneration();

        demo.createUserWithSchema();

        demo.serializeWithSchemaOnly();

        Thread.sleep(1000);

        demo.deserializeWithSchemaOnly();
    }
}
