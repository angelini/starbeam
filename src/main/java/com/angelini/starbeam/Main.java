package com.angelini.starbeam;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.options.DirectPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import com.moandjiezana.toml.Toml;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public class Main {
    static Schema loadSchema(String name) throws IOException {
        return new Schema.Parser().parse(new File(name + ".avsc"));
    }

    static Pipeline createLocalPipeline() {
        DirectPipelineOptions options = PipelineOptionsFactory.create()
                .as(DirectPipelineOptions.class);
        options.setRunner(DirectPipelineRunner.class);
        return Pipeline.create(options);
    }

    static PCollection<GenericRecord> buildEntity(Pipeline p, Schema schema, Entity entity) throws IOException {
        Set<String> tables = entity.getSourceTables();
        assert tables.size() == 1;

        String table = tables.toArray(new String[1])[0];
        String schemaStr = schema.toString();

        PCollection<GenericRecord> tableColl = p.apply(AvroIO.Read
                .from(table + "_sample.avro")
                .withSchema(loadSchema(table)));

        return tableColl.apply(MapElements
                .via((GenericRecord source) -> entity.fromSource(schemaStr, source))
                .withOutputType(new TypeDescriptor<GenericRecord>() {}))
                .setCoder(AvroCoder.of(GenericRecord.class, schema));
    }

    public static void main(String[] args) throws IOException {
        Entity entity = new Toml().read(new File("example.toml")).to(Entity.class);
        System.out.println(entity.toString());

        Pipeline p = createLocalPipeline();

        Schema entitySchema = loadSchema(entity.name);
        PCollection<GenericRecord> entity_coll = buildEntity(p, entitySchema, entity);

        entity_coll.apply(AvroIO.Write.to("results")
                .withSchema(entitySchema)
                .withSuffix(".avro"));

        p.run();
    }
}