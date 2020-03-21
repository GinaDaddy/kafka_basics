package com.brian.avro;

import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 *
 */
public class AvroSchemaBuilder {

    public Schema createUserSchema() {
        Schema user = SchemaBuilder
            .record("User")
            .namespace("com.brian.avro.model")
            .fields()
            .requiredString("name")
            .name("favorite_number").type().nullable().intType().noDefault()
            .name("favorite_color").type().nullable().stringType().noDefault()
            .endRecord();
        return user;
    }

    public static void main(String[] args) {
        System.out.println(new AvroSchemaBuilder().createUserSchema().toString());
    }
}

/*
class User {
    private String name;
    private int[] favorite_number;
    private String[] favorite_color;
}

Aimed to create below schema:

{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}
 */