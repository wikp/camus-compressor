package pl.allegro.tech.hadoop.compressor.mode.unit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.allegro.tech.hadoop.compressor.compression.Compression;
import pl.allegro.tech.hadoop.compressor.schema.SchemaRepository;
import pl.allegro.tech.hadoop.compressor.util.InputAnalyser;

import java.io.IOException;

public class AvroUnitCompressor extends UnitCompressor<AvroWrapper<GenericRecord>, NullWritable> {

    private final JavaSparkContext sparkContext;
    private final SchemaRepository schemaRepository;
    private final Compression<AvroWrapper<GenericRecord>, AvroWrapper<GenericRecord>, NullWritable> compression;

    public AvroUnitCompressor(JavaSparkContext sparkContext, FileSystem fileSystem, InputAnalyser inputAnalyser,
                              String workingPath, String backupDir,
                              SchemaRepository schemaRepository,
                              Compression<AvroWrapper<GenericRecord>, AvroWrapper<GenericRecord>, NullWritable> compression,
                              boolean calculateCounts) {

        super(fileSystem, inputAnalyser, workingPath, backupDir, calculateCounts);
        this.sparkContext = sparkContext;
        this.schemaRepository = schemaRepository;
        this.compression = compression;
    }

    @Override
    protected JavaPairRDD<AvroWrapper<GenericRecord>, NullWritable> countOutputDir(String outputDir, String inputPath) throws IOException {
        final JobConf jobConf = new JobConf(sparkContext.hadoopConfiguration());
        final Schema schema = schemaRepository.findLatestSchema(inputPath);

        AvroJob.setOutputSchema(jobConf, schema);
        FileInputFormat.setInputPaths(jobConf, outputDir);

        return compression.openUncompressed(jobConf);
    }


    @Override
    protected void repartition(JavaPairRDD<AvroWrapper<GenericRecord>, NullWritable> rdd, String inputPath, String outputDir, String jobGroup, int inputSplits)
            throws IOException {

        final JobConf jobConf = new JobConf(sparkContext.hadoopConfiguration());
        final Schema schema = schemaRepository.findLatestSchema(inputPath);

        AvroJob.setOutputSchema(jobConf, schema);
        FileInputFormat.setInputPaths(jobConf, inputPath);

        final JavaPairRDD<AvroWrapper<GenericRecord>, NullWritable> repartitionedRDD = rdd.repartition(inputSplits);

        sparkContext.setJobGroup("compression", jobGroup);

        compression.compress(repartitionedRDD, outputDir, jobConf);
    }
}