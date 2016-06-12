package com.angelini.starbeam;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class Source implements Serializable {
    String table;
    String[] columns;

    public String toString() {
        String cols = Arrays.stream(columns)
                .collect(Collectors.joining(","));
        return table + ".[" + cols + "]";
    }
}

class Attribute implements Serializable {
    String fn;
    Source[] sources;

    String getFnClass() {
        return fn.split("/")[0];
    }

    String getFnName() {
        return fn.split("/")[1];
    }

    Function<List<Object>, Object> getDynamicFunction() {
        ClassLoader classLoader = Attribute.class.getClassLoader();
        try {
            Class clazz = classLoader.loadClass(getFnClass());
            return (Function) clazz.getMethod(getFnName()).invoke(null);
        } catch (ClassNotFoundException |
                NoSuchMethodException |
                InvocationTargetException |
                IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public String toString() {
        return Arrays.stream(sources)
                .map(Source::toString)
                .collect(Collectors.joining(" | "));
    }
}

class Entity implements Serializable {
    String name;
    Map<String, Attribute> attributes;

    Set<String> getSourceTables() {
        return attributes.values()
                .stream()
                .flatMap(attr -> Arrays.stream(attr.sources)
                        .map(s -> s.table))
                .collect(Collectors.toSet());
    }

    GenericRecord fromSourceRecords(String schemaStr, Iterable<GenericRecord> sources) {
        Schema schema = new Schema.Parser().parse(schemaStr);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);

        Map<String, GenericRecord> sourcesMap = StreamSupport
                .stream(sources.spliterator(), false)
                .collect(Collectors.toMap(s -> s.getSchema().getName(), Function.identity()));

        attributes.entrySet().stream()
                .forEach(entry -> {
                    Attribute attr = entry.getValue();
                    Function<List<Object>, Object> fn = attr.getDynamicFunction();

                    List<Object> fnArgs = Arrays.stream(attr.sources)
                            .flatMap(source -> inputValuesFromSource(source, sourcesMap))
                            .collect(Collectors.toList());

                    builder.set(entry.getKey(), fn.apply(fnArgs));
                });

        return builder.build();
    }

    public String toString() {
        String output = "Entity: " + name + "\n";
        output += attributes.entrySet().stream()
                .map(entry -> "  " + entry.getKey() + " -> " + entry.getValue().toString())
                .collect(Collectors.joining("\n"));
        return output;
    }

    private static Stream<Object> inputValuesFromSource(Source source, Map<String, GenericRecord> sourceRecords) {
        GenericRecord record = sourceRecords.get(source.table);
        return Arrays.stream(source.columns)
                .map(record::get);
    }
}