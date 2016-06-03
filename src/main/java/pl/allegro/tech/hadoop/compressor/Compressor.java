package pl.allegro.tech.hadoop.compressor;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import pl.allegro.tech.hadoop.compressor.compression.Compression;
import pl.allegro.tech.hadoop.compressor.compression.CompressionBuilder;
import pl.allegro.tech.hadoop.compressor.mode.CamusCompressor;
import pl.allegro.tech.hadoop.compressor.mode.Compress;
import pl.allegro.tech.hadoop.compressor.mode.TopicCompressor;
import pl.allegro.tech.hadoop.compressor.option.CompressorOptions;
import pl.allegro.tech.hadoop.compressor.option.FilesFormat;
import pl.allegro.tech.hadoop.compressor.mode.unit.AvroUnitCompressor;
import pl.allegro.tech.hadoop.compressor.mode.unit.JsonUnitCompressor;
import pl.allegro.tech.hadoop.compressor.mode.unit.UnitCompressor;
import pl.allegro.tech.hadoop.compressor.util.FileSystemUtils;
import pl.allegro.tech.hadoop.compressor.util.InputAnalyser;
import pl.allegro.tech.hadoop.compressor.util.TopicDateFilter;

import java.io.IOException;
import java.util.EnumMap;

public final class Compressor {

    public static final Logger logger = Logger.getLogger(Compressor.class);

    private static FileSystem fileSystem;
    private static JavaSparkContext sparkContext;
    private static SparkConf sparkConf;
    private static CompressorOptions compressorOptions;

    private Compressor() { }

    public static void main(String... args) throws IOException {
        compressorOptions = new CompressorOptions(args);
        logger.info("Camus compressor spawned with: " + compressorOptions);
        init();
        prepareCompressors().get(compressorOptions.getMode()).compress(compressorOptions.getInputDir());
    }

    private static void init() throws IOException {
        sparkConf = new SparkConf()
                .setAppName(Compressor.class.getName())
                .set("spark.serializer", KryoSerializer.class.getName());

        sparkContext = new JavaSparkContext(sparkConf);
        final Configuration configuration = FileSystemUtils.getConfiguration(sparkContext);
        fileSystem = FileSystemUtils.getFileSystem(configuration);
    }

    private static EnumMap<CompressorOptions.Mode, Compress> prepareCompressors() {
        final TopicDateFilter topicFilter = new TopicDateFilter(compressorOptions.getDelay());

        final UnitCompressor unitCompressor = createUnitCompressor();
        final TopicCompressor topicCompressor = new TopicCompressor(fileSystem, unitCompressor, topicFilter);
        final CamusCompressor camusCompressor = new CamusCompressor(fileSystem, topicCompressor,
                Integer.valueOf(sparkConf.get("spark.executor.instances")));

        final EnumMap<CompressorOptions.Mode, Compress> compressors = new EnumMap<>(CompressorOptions.Mode.class);
        compressors.put(CompressorOptions.Mode.ALL, camusCompressor);
        compressors.put(CompressorOptions.Mode.TOPIC, topicCompressor);
        compressors.put(CompressorOptions.Mode.UNIT, unitCompressor);
        return compressors;
    }

    private static UnitCompressor createUnitCompressor() {
        if (FilesFormat.AVRO.equals(compressorOptions.getFormat())) {
            final Compression<AvroWrapper<GenericRecord>, NullWritable> avroCompression = getAvroCompression();
            final InputAnalyser inputAnalyser = createInputAnalyser(avroCompression);
            return new AvroUnitCompressor(sparkContext, fileSystem, inputAnalyser, avroCompression);
        } else if (FilesFormat.JSON.equals(compressorOptions.getFormat())) {
            final Compression<LongWritable, Text> jsonCompression = getJsonCompression();
            final InputAnalyser inputAnalyser = createInputAnalyser(jsonCompression);
            return new JsonUnitCompressor(sparkContext, fileSystem, jsonCompression, inputAnalyser);
        }

        throw new IllegalArgumentException("Invalid format specified");
    }

    private static InputAnalyser createInputAnalyser(Compression<?, ?> compression) {
        return new InputAnalyser(fileSystem, compression, compressorOptions.isForceSplit());
    }

    private static Compression<LongWritable, Text> getJsonCompression() {
        return CompressionBuilder.forSparkContext(sparkContext)
                .onFileSystem(fileSystem)
                .andCompressorOfType(compressorOptions.getCompression())
                .forJsonFiles();
    }

    private static Compression<AvroWrapper<GenericRecord>, NullWritable> getAvroCompression() {
        return CompressionBuilder.forSparkContext(sparkContext)
                .onFileSystem(fileSystem)
                .andCompressorOfType(compressorOptions.getCompression())
                .forAvroFiles();
    }
}
