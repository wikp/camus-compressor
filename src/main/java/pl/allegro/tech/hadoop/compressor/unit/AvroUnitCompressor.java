package pl.allegro.tech.hadoop.compressor.unit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import pl.allegro.tech.hadoop.compressor.CompressorOptions;
import pl.allegro.tech.hadoop.compressor.InputAnalyser;

import java.io.IOException;

public class AvroUnitCompressor extends UnitCompressor {

    private static final String AVRO_FORMAT = "com.databricks.spark.avro";
    private static final String AVRO_COMPRESSION_CODEC = "spark.sql.avro.compression.codec";
    private static final String AVRO_DEFLATE_LEVEL = "spark.sql.avro.deflate.level";

    private final SQLContext sqlContext;
    private final CompressorOptions compressorOptions;

    public AvroUnitCompressor(SQLContext sqlContext, FileSystem fileSystem, CompressorOptions compressorOptions,
                              InputAnalyser inputAnalyser) {
        super(fileSystem, inputAnalyser);
        this.sqlContext = sqlContext;
        this.compressorOptions = compressorOptions;
    }

    @Override
    protected void repartition(String inputPath, String outputDir, String jobGroup, int inputSplits)
            throws IOException {

        final DataFrame repartitionedDF = sqlContext.read().format(AVRO_FORMAT)
                .load(inputPath)
                .repartition(inputSplits);

        sqlContext.sparkContext().setJobGroup("compression", jobGroup, false);
        configureCodec();
        repartitionedDF.write().format(AVRO_FORMAT).save(outputDir);
    }

    private void configureCodec() {
        if (CompressorOptions.CompressionFormat.DEFLATE.equals(compressorOptions.getCompression())) {
            sqlContext.setConf(AVRO_COMPRESSION_CODEC, "deflate");
            sqlContext.setConf(AVRO_DEFLATE_LEVEL, "9");
        } else if (CompressorOptions.CompressionFormat.SNAPPY.equals(compressorOptions.getCompression())) {
            sqlContext.setConf(AVRO_COMPRESSION_CODEC, "snappy");
        } else if (CompressorOptions.CompressionFormat.NONE.equals(compressorOptions.getCompression())) {
            sqlContext.setConf(AVRO_COMPRESSION_CODEC, "uncompressed");
        } else {
            throw new IllegalArgumentException("Avro compression allow only deflate, snappy and none compression");
        }
    }
}
