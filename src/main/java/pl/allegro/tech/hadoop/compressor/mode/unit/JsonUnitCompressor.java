package pl.allegro.tech.hadoop.compressor.mode.unit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.allegro.tech.hadoop.compressor.util.InputAnalyser;
import pl.allegro.tech.hadoop.compressor.compression.Compression;

import java.io.IOException;

public class JsonUnitCompressor extends UnitCompressor {

    private final JavaSparkContext context;
    private final Compression<LongWritable, Text> compression;

    public JsonUnitCompressor(JavaSparkContext context, FileSystem fileSystem,
                              Compression<LongWritable, Text> compression, InputAnalyser inputAnalyser) {

        super(fileSystem, inputAnalyser);
        this.compression = compression;
        this.context = context;
    }

    @Override
    protected void repartition(String inputPath, String outputDir, String jobGroup, int inputSplits)
            throws IOException {

        final JavaPairRDD<LongWritable, Text> rdd = compression.openUncompressed(inputPath)
                .repartition(inputSplits);

        JobConf jobConf = new JobConf(context.hadoopConfiguration());
        context.setJobGroup("compression", jobGroup);
        compression.compress(rdd, outputDir, jobConf);
    }
}
