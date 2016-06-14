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

class TableView {
    enum Aggregate {
        ALL, UNIQUE, SUM
    }

    Map<String, Aggregate> aggMap;

    TableView(String[] columns, String[] aggregates) {
        if (aggregates != null && columns.length != aggregates.length) {
            throw new AssertionError("Unmatched aggregations");
        }

        aggMap = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            if (aggregates != null) {
                aggMap.put(columns[i], Aggregate.valueOf(aggregates[i]));
            } else {
                aggMap.put(columns[i], Aggregate.ALL);
            }
        }
    }

    void merge(TableView other) {
        other.aggMap.forEach((k, v) ->
                aggMap.merge(k, v, (v1, v2) -> {
                    if (v1 != v2) {
                        throw new RuntimeException("Mixed aggregations for column: " + k);
                    }
                    return v1;
                }));
    }
}

class Source implements Serializable {
    String table;
    String[] columns;
    String[] aggregates;

    public TableView getTableView() {
        return new TableView(columns, aggregates);
    }

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

    Map<String, TableView> getSourceTables() {
        Map<String, TableView> aggregates = new HashMap<>();

        for (Attribute attr : attributes.values()) {
            for (Source source : attr.sources) {
                if (aggregates.containsKey(source.table)) {
                    TableView combinedView = aggregates.get(source.table);
                    TableView sourceView = source.getTableView();
                    combinedView.merge(sourceView);
                } else {
                    aggregates.put(source.table, source.getTableView());
                }
            }
        }

        return aggregates;
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