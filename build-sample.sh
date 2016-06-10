java -jar ./avro-tools-1.8.1.jar fromjson --schema-file data/raw_first.avsc data/raw_first.json > data/raw_first.avro
java -jar ./avro-tools-1.8.1.jar fromjson --schema-file data/raw_second.avsc data/raw_second.json > data/raw_second.avro
