package pl.allegro.tech.hadoop.compressor.unit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import pl.allegro.tech.hadoop.compressor.CompressorOptions;
import pl.allegro.tech.hadoop.compressor.InputAnalyser;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.IOException;

public class AvroUnitCompressor extends UnitCompressor {

    private static final Logger logger = Logger.getLogger(AvroUnitCompressor.class);

    private static final String AVRO_FORMAT = "com.databricks.spark.avro";
    private static final String AVRO_COMPRESSION_CODEC = "spark.sql.avro.compression.codec";
    private static final String AVRO_DEFLATE_LEVEL = "spark.sql.avro.deflate.level";

    private final JavaSparkContext sparkContext;
    private final CompressorOptions compressorOptions;

    public AvroUnitCompressor(JavaSparkContext sparkContext, FileSystem fileSystem, CompressorOptions compressorOptions,
                              InputAnalyser inputAnalyser) {
        super(fileSystem, inputAnalyser);
        this.sparkContext = sparkContext;
        this.compressorOptions = compressorOptions;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void repartition(String inputPath, String outputDir, String jobGroup, int inputSplits)
            throws IOException {
        final Job readJob = Job.getInstance(sparkContext.hadoopConfiguration());

        final Class<AvroKey<GenericRecord>> avroKeyClass = (Class<AvroKey<GenericRecord>>) (Class<?>) AvroKey.class;
        final Class<NullWritable> nullWritableClass = NullWritable.class;
        final Class<AvroKeyInputFormat<GenericRecord>> avroKeyInputFormatClass = (Class<AvroKeyInputFormat<GenericRecord>>) (Class<?>) AvroKeyInputFormat.class;
        final Class<AvroKeyOutputFormat<GenericRecord>> avroKeyOutputFormatClass = (Class<AvroKeyOutputFormat<GenericRecord>>) (Class<?>) AvroKeyOutputFormat.class;

        AvroJob.setInputKeySchema(readJob, getSchema(inputPath));
        final JavaPairRDD<AvroKey<GenericRecord>, NullWritable> writeRDD = sparkContext.newAPIHadoopFile(inputPath,
                avroKeyInputFormatClass,
                avroKeyClass,
                nullWritableClass,
                readJob.getConfiguration())/*
                .map(new Function<Tuple2<AvroKey<GenericRecord>, NullWritable>, GenericRecord>() {
                    @Override
                    public GenericRecord call(Tuple2<AvroKey<GenericRecord>, NullWritable> v1) throws Exception {
                        return v1._1().datum();
                    }
                })*/
                .repartition(inputSplits)
                /*.mapToPair(new PairFunction<GenericRecord, AvroKey<GenericRecord>, NullWritable>() {
                    @Override
                    public Tuple2<AvroKey<GenericRecord>, NullWritable> call(GenericRecord o) throws Exception {
                        return new Tuple2<>(new AvroKey<>(o), NullWritable.get());
                    }
                })*/;
//
        final Configuration conf = sparkContext.hadoopConfiguration();
        conf.set("mapred.output.compression.codec", SnappyCodec.class.getCanonicalName());
        final Job writeJob = Job.getInstance(conf);

        AvroJob.setOutputKeySchema(writeJob, getSchema(inputPath));
        writeJob.setOutputFormatClass(avroKeyOutputFormatClass);

        writeRDD.saveAsNewAPIHadoopFile(
                outputDir,
                avroKeyClass,
                nullWritableClass,
                avroKeyOutputFormatClass,
                writeJob.getConfiguration());


//        sparkContext.newAPIHadoopFile(inputPath).

//        final DataFrame repartitionedDF = sqlContext.read().format(AVRO_FORMAT)
//                .load(inputPath)
//                .repartition(inputSplits);
//
//        sqlContext.sparkContext().setJobGroup("compression", jobGroup, false);
//        configureCodec();
//        repartitionedDF.write().format(AVRO_FORMAT).save(outputDir);
    }

    private Schema getSchema(String inputPath) {
        return null;
    }

    private void configureCodec() {
        if (CompressorOptions.CompressionFormat.DEFLATE.equals(compressorOptions.getCompression())) {
//            sqlContext.setConf(AVRO_COMPRESSION_CODEC, "deflate");
//            sqlContext.setConf(AVRO_DEFLATE_LEVEL, "9");
        } else if (CompressorOptions.CompressionFormat.SNAPPY.equals(compressorOptions.getCompression())) {
//            sqlContext.setConf(AVRO_COMPRESSION_CODEC, "snappy");
        } else if (CompressorOptions.CompressionFormat.NONE.equals(compressorOptions.getCompression())) {
//            sqlContext.setConf(AVRO_COMPRESSION_CODEC, "uncompressed");
        } else {
            throw new IllegalArgumentException("Avro compression allow only deflate, snappy and none compression");
        }
    }
}
