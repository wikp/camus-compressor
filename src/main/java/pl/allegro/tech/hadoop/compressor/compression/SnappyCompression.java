package pl.allegro.tech.hadoop.compressor.compression;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

class SnappyCompression<K, V, I extends InputFormat<K, V>, O extends OutputFormat<K, V>>
        extends AbstractCompression<K, V, I, O> {

    private static final Logger logger = Logger.getLogger(SnappyCompression.class);

    private long inputBlockSize;

    public SnappyCompression(JavaSparkContext sparkContext, FileSystem fileSystem, Class<K> keyClass,
                             Class<V> valueClass, Class<I> inputFormatClass, Class<O> outputFormatClass) {

        super(sparkContext, keyClass, valueClass, inputFormatClass, outputFormatClass);
        this.inputBlockSize = fileSystem.getDefaultBlockSize(new Path("/")) * 2;
        logger.warn("BlockSize = " + this.inputBlockSize);
    }

    @Override
    public int getSplits(long size) {
        return (int) ((size + inputBlockSize - 1) / inputBlockSize);
    }

    @Override
    public String getExtension() {
        return "snappy";
    }

    @Override
    protected void setupJobConf(JobConf jobConf) {
        jobConf.setBoolean(MAPRED_COMPRESS_KEY, true);
        jobConf.set(COMPRESSION_CODEC_KEY, SnappyCodec.class.getName());
        jobConf.set(COMPRESSION_TYPE_KEY, COMPRESSION_TYPE_BLOCK);
    }
}
