package pl.allegro.tech.hadoop.compressor;

import java.io.IOException;

public interface Compress {
    void compress(String inputDir) throws IOException;
}
