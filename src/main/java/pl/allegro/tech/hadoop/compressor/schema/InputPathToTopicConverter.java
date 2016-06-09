package pl.allegro.tech.hadoop.compressor.schema;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InputPathToTopicConverter {

    private static final Pattern pattern = Pattern.compile("(hdfs://[a-zA-Z0-9]+)?/[-a-zA-Z0-9_\\.]+/[-a-zA-Z0-9_\\.]+/([-a-zA-Z0-9_\\.]+)(/.*)?$");

    public String toTopicName(String inputPath) {

        final Matcher matcher = pattern.matcher(inputPath);
        if (matcher.find()) {
            return matcher.group(2).replace("_avro", "").replace("_", ".");
        } else {
            throw new SchemaNotFoundException("Could not convert inputPath to topic", inputPath);
        }
    }
}
