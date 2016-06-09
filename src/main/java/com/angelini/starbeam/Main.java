package com.angelini.starbeam;

import com.angelini.starbeam.avro.Example;
import com.angelini.starbeam.avro.Raw;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.options.DirectPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import com.moandjiezana.toml.Toml;

import java.io.File;
import java.util.Map;
import java.util.stream.Collectors;

class Attribute {
    String source;

    public String toString() {
        return "source: " + source;
    }
}

class Entity {
    String name;
    Map<String, Attribute> attributes;

    public String toString() {
        String output = "Entity: " + name + "\n";
        output += attributes.entrySet()
                .stream()
                .map(entry -> "  " + entry.getKey() + " -> " + entry.getValue().toString())
                .collect(Collectors.joining("\n"));
        return output;
    }
}

public class Main {
    static Pipeline createLocalPipeline() {
        DirectPipelineOptions options = PipelineOptionsFactory.create()
                .as(DirectPipelineOptions.class);
        options.setRunner(DirectPipelineRunner.class);
        return Pipeline.create(options);
    }

    public static void main(String[] args) {
        File example_file = new File("example.toml");
        Entity example = new Toml().read(example_file).to(Entity.class);
        System.out.println(example.toString());

        Pipeline p = createLocalPipeline();

        PCollection<Raw> raw_records = p.apply(AvroIO.Read.from("raw_sample.avro")
                .withSchema(Raw.class));

        PCollection<Example> example_records = raw_records.apply(MapElements
                .via((Raw raw) -> Example.newBuilder()
                        .setId(Integer.parseInt(raw.getId().toString()))
                        .setTime(Integer.parseInt(raw.getCreatedAt().toString()))
                        .setFirst(raw.getFirst())
                        .setSecond(raw.getSecond())
                        .build())
                .withOutputType(new TypeDescriptor<Example>() {}));

        example_records.apply(AvroIO.Write.to("results.avro")
                .withSchema(Example.class));

        p.run();
    }
}