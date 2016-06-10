package com.angelini.starbeam;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    static Schema loadSchema(String name) {
        try {
            return new Schema.Parser().parse(new File("data/" + name + ".avsc"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Pipeline createLocalPipeline() {
        DirectPipelineOptions options = PipelineOptionsFactory.create()
                .as(DirectPipelineOptions.class);
        options.setRunner(DirectPipelineRunner.class);
        return Pipeline.create(options);
    }

    static PCollection<KV<Integer, GenericRecord>> loadAndKeySource(Pipeline p, String table) {
        Schema schema = loadSchema(table);
        return p.apply(AvroIO.Read
                        .from("data/" + table + ".avro")
                        .withSchema(schema))
                .apply(MapElements
                        .via((GenericRecord record) -> KV.of((Integer) record.get("id"), record))
                        .withOutputType(new TypeDescriptor<KV<Integer, GenericRecord>>() {}))
                .setCoder(KvCoder.of(
                        AvroCoder.of(Integer.class),
                        AvroCoder.of(GenericRecord.class, schema)));
    }

    static PCollection<GenericRecord> buildEntity(Pipeline p, Schema schema, Entity entity) {
        List<PCollection<KV<Integer, GenericRecord>>> tables =
                entity.getSourceTables().stream()
                        .map(table -> loadAndKeySource(p, table))
                        .collect(Collectors.toList());

        String schemaStr = schema.toString();
        return PCollectionList.of(tables)
                        .apply(Flatten.pCollections())
                        .apply(GroupByKey.create())
                        .apply(MapElements
                                .via((KV<Integer, Iterable<GenericRecord>> kv) ->
                                        entity.fromSource(schemaStr, kv.getValue()))
                                .withOutputType(new TypeDescriptor<GenericRecord>() {}))
                        .setCoder(AvroCoder.of(GenericRecord.class, schema));
    }

    public static void main(String[] args) throws IOException {
        Entity entity = new Toml().read(new File("example.toml")).to(Entity.class);
        System.out.println(entity.toString());

        Pipeline p = createLocalPipeline();

        Schema entitySchema = loadSchema(entity.name);
        PCollection<GenericRecord> entityColl = buildEntity(p, entitySchema, entity);

        entityColl.apply(AvroIO.Write.to("results")
                .withSchema(entitySchema)
                .withSuffix(".avro"));

        p.run();
    }
}