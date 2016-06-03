package pl.allegro.tech.hadoop.compressor.schema;

import java.io.IOException;

public class SchemaNotFoundException extends Throwable {

    private static final long serialVersionUID = -5873618512938340074L;

    private final String topic;

    public SchemaNotFoundException(String message, String topic, IOException cause) {
        super(message, cause);
        this.topic = topic;
    }

    @Override
    public String toString() {
        return super.toString() + " -- topic: " + topic;
    }
}
