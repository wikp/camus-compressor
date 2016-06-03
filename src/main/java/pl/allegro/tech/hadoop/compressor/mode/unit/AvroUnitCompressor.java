package pl.allegro.tech.hadoop.compressor.mode.unit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.allegro.tech.hadoop.compressor.util.InputAnalyser;
import pl.allegro.tech.hadoop.compressor.compression.Compression;

import java.io.IOException;

public class AvroUnitCompressor extends UnitCompressor {

    private static final Logger logger = Logger.getLogger(AvroUnitCompressor.class);

    private final JavaSparkContext sparkContext;
    private final Compression<AvroWrapper<GenericRecord>, NullWritable> compression;

    public AvroUnitCompressor(JavaSparkContext sparkContext, FileSystem fileSystem, InputAnalyser inputAnalyser,
                              Compression<AvroWrapper<GenericRecord>, NullWritable> compression) {

        super(fileSystem, inputAnalyser);
        this.sparkContext = sparkContext;
        this.compression = compression;
    }

    @Override
    protected void repartition(String inputPath, String outputDir, String jobGroup, int inputSplits)
            throws IOException {

        final JavaPairRDD<AvroWrapper<GenericRecord>, NullWritable> repartitionedRDD = compression
                .openUncompressed(inputPath)
                .repartition(inputSplits);

        final JobConf jobConf = new JobConf(sparkContext.hadoopConfiguration());
        AvroJob.setOutputSchema(jobConf, getSchema(inputPath));

        sparkContext.setJobGroup("compression", jobGroup);
        compression.compress(repartitionedRDD, outputDir, jobConf);
    }

    private Schema getSchema(String inputPath) {
        try {
            final String schemaString = IOUtils.toString(fileSystem.open(new Path(inputPath + "/schemas/3")));
            return new Schema.Parser().parse(schemaString);
        } catch (IOException e) {
            logger.error("Unable to read schema from HDFS: " + e.getMessage());
            throw new RuntimeException("Unable to read schema", e);
        }
    }
}
