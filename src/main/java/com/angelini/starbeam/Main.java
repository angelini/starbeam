package com.angelini.starbeam;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.options.DirectPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.values.*;

import com.moandjiezana.toml.Toml;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

class DependencyGraph {
    final Entity[] entities;
    DirectedAcyclicGraph<Entity, DefaultEdge> graph;

    DependencyGraph(Entity[] entities) {
        this.entities = entities;
        graph = new DirectedAcyclicGraph<>(DefaultEdge.class);

        Map<String, Entity> map = Arrays.stream(entities)
                .collect(Collectors.toMap(e -> e.name, Function.identity()));

        Arrays.stream(entities)
                .forEach(entity -> graph.addVertex(entity));

        Arrays.stream(entities)
                .forEach(entity -> entity.getSourceTables().keySet().stream()
                            .filter(map::containsKey)
                            .forEach(table -> graph.addEdge(map.get(table), entity)));
    }

    List<Entity> getBuildOrder() {
        List<Entity> order = new ArrayList<>();
        Iterator<Entity> iter = graph.iterator();

        while (iter.hasNext()) {
            order.add(iter.next());
        }
        return order;
    }
}

public class Main {
    static Schema loadSchema(String name) {
        try {
            return new Schema.Parser().parse(new File("data/" + name + ".avsc"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Entity loadEntity(String name) {
        return new Toml().read(new File(name + ".toml")).to(Entity.class);
    }

    static Pipeline createLocalPipeline() {
        DirectPipelineOptions options = PipelineOptionsFactory.create()
                .as(DirectPipelineOptions.class);
        options.setRunner(DirectPipelineRunner.class);
        return Pipeline.create(options);
    }

    static PCollection<GenericRecord> buildEntity(
            Pipeline p, Entity entity, Schema schema, SourceLoader loader) {
        List<PCollection<KV<Integer, GenericRecord>>> tables =
                entity.getSourceTables().keySet().stream()
                        .map(table -> {
                            Schema tableSchema = loadSchema(table);
                            return loader.loadTable(p, table, tableSchema);
                        })
                        .collect(Collectors.toList());

        String schemaStr = schema.toString();
        return PCollectionList.of(tables)
                        .apply(Flatten.pCollections())
                        .apply(GroupByKey.create())
                        .apply(MapElements
                                .via((KV<Integer, Iterable<GenericRecord>> kv) ->
                                        entity.fromSourceRecords(schemaStr, kv.getValue()))
                                .withOutputType(new TypeDescriptor<GenericRecord>() {}))
                        .setCoder(AvroCoder.of(GenericRecord.class, schema));
    }

    static void writeEntity(PCollection<GenericRecord> collection, String name, Schema schema) {
        collection.apply(AvroIO.Write.to(name + "_results")
                .withSchema(schema)
                .withSuffix(".avro"));
    }

    static void buildAndWriteEntities(Pipeline p, Entity[] entities) {
        DependencyGraph graph = new DependencyGraph(entities);
        SourceLoader loader = new SourceLoader();

        graph.getBuildOrder().stream().forEach(entity -> {
            System.out.println(entity);
            Schema schema = loadSchema(entity.name);

            PCollection<GenericRecord> collection = buildEntity(p, entity, schema, loader);
            loader.put(entity.name, collection);
            writeEntity(collection, entity.name, schema);
        });
    }

    public static void main(String[] args) throws IOException {
        Entity example = loadEntity("example");
        Entity other = loadEntity("other");
        Entity[] entities = {example, other};

        Pipeline p = createLocalPipeline();
        buildAndWriteEntities(p, entities);
        p.run();
    }
}