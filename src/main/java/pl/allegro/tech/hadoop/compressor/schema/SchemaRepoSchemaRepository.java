package pl.allegro.tech.hadoop.compressor.schema;

import org.apache.avro.Schema;

public class SchemaRepoSchemaRepository implements SchemaRepository {


    @Override
    public Schema findLatestSchema(String topic) {
        return null;
    }
}
