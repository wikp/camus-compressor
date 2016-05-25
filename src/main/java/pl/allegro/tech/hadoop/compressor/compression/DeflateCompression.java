package pl.allegro.tech.hadoop.compressor.compression;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DeflateCompression implements Compression {

    public static final Logger logger = Logger.getLogger(DeflateCompression.class);

    private final JavaSparkContext sparkContext;
    private long inputBlockSize;

    public DeflateCompression(JavaSparkContext sparkContext, FileSystem fileSystem) {
        this.sparkContext = sparkContext;
        this.inputBlockSize = fileSystem.getDefaultBlockSize(new Path("/")) * 2;
        logger.warn("BlockSize = " + this.inputBlockSize);
    }

    @Override
    public void compress(JavaRDD<String> content, String outputDir) throws IOException {
        content.saveAsTextFile(outputDir, DeflateCodec.class);
    }

    @Override
    public JavaRDD<String> decompress(String inputPath) throws IOException {
        return sparkContext.textFile(String.format("%s/*.%s", inputPath, getExtension()));
    }

    @Override
    public int getSplits(long size) {
        return (int) ((size + inputBlockSize - 1) / inputBlockSize);
    }

    @Override
    public String getExtension() {
        return "deflate";
    }
}
