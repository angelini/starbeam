package com.angelini.starbeam;

import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class Attribute implements Serializable {
    String source;
    String fn;

    public String getSourceTable() {
        return source.split("\\.")[0];
    }

    public String getSourceColumn() {
        return source.split("\\.")[1];
    }

    public Function<Object, Object> getFunction() {
        switch (fn) {
            case "string->int":
                return str -> Integer.parseInt(str.toString());
            case "string->string":
                return str -> str;
        }

        return null;
    }

    public String toString() {
        return "source: " + source;
    }
}

public class Entity implements Serializable {
    String name;
    Map<String, Attribute> attributes;

    public Set<String> getSourceTables() {
        return attributes.values()
                .stream()
                .map(attr -> attr.getSourceTable())
                .collect(Collectors.toSet());
    }

    public GenericRecord fromSource(String schemaStr, GenericRecord source) {
        Schema schema = new Schema.Parser().parse(schemaStr);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);

        for (Map.Entry<String, Attribute> entry : attributes.entrySet()) {
            Attribute attr = entry.getValue();
            String sourceColumn = attr.getSourceColumn();

            builder.set(entry.getKey(), attr.getFunction().apply(source.get(sourceColumn)));
        }

        return builder.build();
    }

    public String toString() {
        String output = "Entity: " + name + "\n";
        output += attributes.entrySet()
                .stream()
                .map(entry -> "  " + entry.getKey() + " -> " + entry.getValue().toString())
                .collect(Collectors.joining("\n"));
        return output;
    }
}