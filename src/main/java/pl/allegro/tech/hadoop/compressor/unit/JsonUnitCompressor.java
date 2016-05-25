package pl.allegro.tech.hadoop.compressor.unit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.allegro.tech.hadoop.compressor.InputAnalyser;
import pl.allegro.tech.hadoop.compressor.compression.Compression;

import java.io.IOException;

public class JsonUnitCompressor extends UnitCompressor {
    private final JavaSparkContext context;
    private final Compression compression;

    public JsonUnitCompressor(JavaSparkContext context, FileSystem fileSystem, Compression compression, InputAnalyser inputAnalyser) {
        super(fileSystem, inputAnalyser);
        this.compression = compression;
        this.context = context;
    }

    @Override
    protected void repartition(String inputPath, String outputDir, String jobGroup, int inputSplits)
            throws IOException {

        final JavaRDD<String> rdd = context.textFile(inputPath).repartition(inputSplits);
        context.setJobGroup("compression", jobGroup);
        compression.compress(rdd, outputDir);
    }
}
