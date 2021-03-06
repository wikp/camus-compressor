package pl.allegro.tech.hadoop.compressor.mode;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import pl.allegro.tech.hadoop.compressor.mode.unit.UnitCompressor;
import pl.allegro.tech.hadoop.compressor.option.CompressorOptions;
import pl.allegro.tech.hadoop.compressor.util.TopicDateFilter;

import java.io.IOException;
import java.util.List;

public class TopicCompressor implements Compress {

    private final List<String> directoriesToProcess;
    private FileSystem fileSystem;
    private UnitCompressor unitCompressor;
    private final TopicDateFilter topicFilter;

    private static final Logger logger = Logger.getLogger(TopicCompressor.class);

    public TopicCompressor(FileSystem fileSystem, UnitCompressor unitCompressor, TopicDateFilter topicFilter,
                           CompressorOptions options) {
        this.fileSystem = fileSystem;
        this.unitCompressor = unitCompressor;
        this.topicFilter = topicFilter;
        this.directoriesToProcess = options.getTopicModePatterns();
    }

    public void compress(String topicDir) throws IOException {
        logger.info(String.format("Compress topic %s", topicDir));

        for (String dir : directoriesToProcess) {
            compress(dir, topicDir);
        }
    }

    public void compress(Path topicDir) throws IOException {
        compress(topicDir.toString());
    }

    private void compress(String unitPattern, String topicDir) throws IOException {
        String pattern = String.format("%s/%s", topicDir, unitPattern);

        final FileStatus[] fileStatuses = fileSystem.globStatus(new Path(pattern));

        for (FileStatus unitStatus : fileStatuses) {
            if (topicFilter.shouldCompressTopicDir(unitStatus.getPath().toString().replace(topicDir, ""))) {
                unitCompressor.compress(unitStatus.getPath());
            }
        }
    }
}
