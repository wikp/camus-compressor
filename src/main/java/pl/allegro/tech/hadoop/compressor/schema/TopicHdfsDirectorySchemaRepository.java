package pl.allegro.tech.hadoop.compressor.schema;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class TopicHdfsDirectorySchemaRepository implements SchemaRepository {

    private final FileSystem fileSystem;
    private final String rootDirectory;

    public TopicHdfsDirectorySchemaRepository(FileSystem fileSystem, String rootDirectory) {
        this.fileSystem = fileSystem;
        this.rootDirectory = rootDirectory;
    }

    @Override
    public Schema findLatestSchema(String topic) {
        try {
            return new Schema.Parser().parse(fileSystem.open(makePath(topic)));
        } catch (IOException e) {
            throw new SchemaNotFoundException("Schema was not found for given topic", topic, e);
        }
    }

    private Path makePath(String topic) {
        final String topicFolder = topic.replace(".", "_").concat("_avro");
        return new Path(rootDirectory + Path.SEPARATOR + topicFolder);
    }
}
