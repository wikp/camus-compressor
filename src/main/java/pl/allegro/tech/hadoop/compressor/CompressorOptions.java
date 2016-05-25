package pl.allegro.tech.hadoop.compressor;

import java.io.Serializable;
import java.util.Arrays;

public class CompressorOptions implements Serializable {

    private final Mode mode;
    private final String inputDir;
    private final int delay;
    private final CompressionFormat compression;
    private final FilesFormat format;
    private final boolean forceSplit;

    public CompressorOptions(String[] args) {
        mode = Mode.fromString(args[0]);
        inputDir = args[1];
        compression = CompressionFormat.fromString(args[2]);
        delay = Integer.valueOf(args[3]);
        format = FilesFormat.fromString(args[4]);
        forceSplit = Arrays.asList(args).contains("--force");
    }

    public Mode getMode() {
        return mode;
    }

    public String getInputDir() {
        return inputDir;
    }

    public int getDelay() {
        return delay;
    }

    public CompressionFormat getCompression() {
        return compression;
    }

    public FilesFormat getFormat() {
        return format;
    }

    public boolean isForceSplit() {
        return forceSplit;
    }

    @Override
    public String toString() {
        return "CompressorOptions{" +
                "mode=" + mode +
                ", inputDir='" + inputDir + '\'' +
                ", delay=" + delay +
                ", compression=" + compression +
                ", format=" + format +
                ", forceSplit=" + forceSplit +
                '}';
    }

    public enum FilesFormat {
        AVRO, JSON;

        static FilesFormat fromString(String format) {
            return FilesFormat.valueOf(format.toUpperCase());
        }
    }

    public enum Mode {
        ALL, TOPIC, UNIT;

        static Mode fromString(String mode) {
            return Mode.valueOf(mode.toUpperCase());
        }
    }

    public enum CompressionFormat {
        SNAPPY, DEFLATE, LZO, NONE;

        static CompressionFormat fromString(String format) {
            return CompressionFormat.valueOf(format.toUpperCase());
        }
    }
}
