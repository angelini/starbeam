package com.angelini.starbeam;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

class Source implements Serializable {
    String table;
    String[] columns;

    public String toString() {
        String cols = Arrays.stream(columns)
                .map(c -> c.toString())
                .collect(Collectors.joining(","));
        return "table: " + table + ", columns: [" + cols + "]";
    }
}

class Attribute implements Serializable {
    String fn;
    Map<String, Source> sources;

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
        return sources.entrySet()
                .stream()
                .map(e -> e.getKey() + ": " + e.getValue())
                .collect(Collectors.joining(" | "));
    }
}

class Entity implements Serializable {
    String name;
    Map<String, Attribute> attributes;

    List<String> getSourceTables() {
        return attributes.values()
                .stream()
                .flatMap(attr -> attr.sources.values()
                        .stream()
                        .map(s -> s.table))
                .collect(Collectors.toList());
    }

    GenericRecord fromSource(String schemaStr, Iterable<GenericRecord> sources) {
        Schema schema = new Schema.Parser().parse(schemaStr);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);

        Map<String, GenericRecord> sourcesMap = new HashMap();
        for (GenericRecord source : sources) {
            sourcesMap.put(source.getSchema().getName(), source);
        }

        for (Map.Entry<String, Attribute> entry : attributes.entrySet()) {
            Attribute attr = entry.getValue();
            // FIXME: Only using the first source
            Source source = attr.sources.values().stream().findFirst().get();

            GenericRecord record = sourcesMap.get(source.table);
            Function<List<Object>, Object> fn = attr.getDynamicFunction();

            List<Object> args = Arrays.stream(source.columns)
                    .map(column -> record.get(column))
                    .collect(Collectors.toList());
            builder.set(entry.getKey(), fn.apply(args));
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