package pl.allegro.tech.hadoop.compressor;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SQLContext;
import pl.allegro.tech.hadoop.compressor.compression.Compression;
import pl.allegro.tech.hadoop.compressor.compression.DeflateCompression;
import pl.allegro.tech.hadoop.compressor.compression.LzoCompression;
import pl.allegro.tech.hadoop.compressor.compression.NoneCompression;
import pl.allegro.tech.hadoop.compressor.compression.SnappyCompression;
import pl.allegro.tech.hadoop.compressor.unit.UnitCompressor;
import pl.allegro.tech.hadoop.compressor.unit.AvroUnitCompressor;
import pl.allegro.tech.hadoop.compressor.unit.JsonUnitCompressor;
import pl.allegro.tech.hadoop.compressor.util.FileSystemUtils;

public final class Compressor {

    public static final Logger logger = Logger.getLogger(Compressor.class);

    private static FileSystem fileSystem;
    private static SQLContext sqlContext;
    private static JavaSparkContext sparkContext;
    private static Compression compression;
    private static InputAnalyser inputAnalyser;
    private static SparkConf sparkConf;
    private static CompressorOptions compressorOptions;

    private Compressor() { }

    public static void main(String[] args) throws IOException {
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
        sqlContext = new SQLContext(sparkContext);
        final Configuration configuration = FileSystemUtils.getConfiguration(sparkContext);
        fileSystem = FileSystemUtils.getFileSystem(configuration);
        compression = getCompression(compressorOptions.getCompression());
        inputAnalyser = createInputAnalyser();
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
        if (CompressorOptions.FilesFormat.AVRO.equals(compressorOptions.getFormat())) {
            return new AvroUnitCompressor(sqlContext, fileSystem, compressorOptions, inputAnalyser);
        } else if (CompressorOptions.FilesFormat.JSON.equals(compressorOptions.getFormat())) {
            return new JsonUnitCompressor(sparkContext, fileSystem, compression, inputAnalyser);
        }

        throw new IllegalArgumentException("Invalid format specified");
    }

    private static Compression getCompression(CompressorOptions.CompressionFormat compressor) {
        if (CompressorOptions.CompressionFormat.NONE.equals(compressor)) {
            return new NoneCompression(fileSystem, sparkContext);
        } else if (CompressorOptions.CompressionFormat.LZO.equals(compressor)) {
            return new LzoCompression(sparkContext, fileSystem);
        } else if (CompressorOptions.CompressionFormat.DEFLATE.equals(compressor)) {
            return new DeflateCompression(sparkContext, fileSystem);
        }
        return new SnappyCompression(fileSystem, sparkContext);
    }

    private static InputAnalyser createInputAnalyser() {
        return new InputAnalyser(fileSystem, compression, compressorOptions.isForceSplit());
    }
}
