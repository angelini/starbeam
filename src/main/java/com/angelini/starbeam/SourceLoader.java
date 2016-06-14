package com.angelini.starbeam;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

public class SourceLoader {
    Map<String, PCollection<GenericRecord>> buildCache;

    static PCollection<KV<Integer, GenericRecord>> keyById(PCollection<GenericRecord> collection, Schema schema) {
        return collection.apply(MapElements
                .via((GenericRecord record) -> KV.of((Integer) record.get("id"), record))
                .withOutputType(new TypeDescriptor<KV<Integer, GenericRecord>>() {}))
                .setCoder(KvCoder.of(
                        AvroCoder.of(Integer.class),
                        AvroCoder.of(GenericRecord.class, schema)));
    }


    SourceLoader() {
        buildCache = new HashMap<>();
    }

    PCollection<KV<Integer, GenericRecord>> loadTable(Pipeline p, String table, Schema schema) {
        if (buildCache.containsKey(table)) {
            return keyById(buildCache.get(table), schema);
        } else {
            return keyById(p.apply(AvroIO.Read
                    .from("data/" + table + ".avro")
                    .withSchema(schema))
                    .setCoder(AvroCoder.of(GenericRecord.class, schema)), schema);
        }
    }

    void put(String table, PCollection<GenericRecord> collection) {
        buildCache.put(table, collection);
    }
}
