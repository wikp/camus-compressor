package pl.allegro.tech.hadoop.compressor.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static pl.allegro.tech.hadoop.compressor.Utils.checkDecompress;
import static pl.allegro.tech.hadoop.compressor.Utils.fileStatusForPath;


@RunWith(MockitoJUnitRunner.class)
public class DeflateCompressionTest {

    private static final Logger logger = Logger.getLogger(DeflateCompressionTest.class);
    private static final String CODEC = DeflateCodec.class.getName();
    private static final String OUTPUT_DIR_NAME = "output_dir_name";
    private static final Path OUTPUT_PATH = new Path(OUTPUT_DIR_NAME);
    private static final Path COMPRESSED_FILE_PATH = OUTPUT_PATH.suffix("file.deflate");
    private static final Path SECOND_COMPRESSED_FILE_PATH = OUTPUT_PATH.suffix("file2.deflate");
    private static final String INPUT_FILE = "test_file";
    private static final org.apache.hadoop.fs.FileStatus[] TEST_STATUSES = new FileStatus[] {
            fileStatusForPath(COMPRESSED_FILE_PATH), fileStatusForPath(SECOND_COMPRESSED_FILE_PATH)
    };

    @Mock
    private Configuration configuration;

    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private FileSystem fileSystem;

    @Mock
    private JavaRDD<String> content;

    private DeflateCompression deflateCompression;

    @Before
    public void setUp() throws Exception {
        deflateCompression = new DeflateCompression(sparkContext, fileSystem);
    }

    @Test
    public void shouldCompressWithDeflateCodec() throws Exception {
        // given
        when(configuration.get("io.compression.codecs")).thenReturn(CODEC);
        when(configuration.getBoolean(eq("hadoop.native.lib"), anyBoolean())).thenReturn(true);

        when(configuration.getClassByName(CODEC)).thenAnswer(new Answer<Class<?>>() {
            @Override
            public Class<?> answer(InvocationOnMock invocation) throws Throwable {
                return DeflateCodec.class;
            }
        });

        when(fileSystem.getConf()).thenReturn(configuration);
        when(fileSystem.listStatus(eq(OUTPUT_PATH), any(GlobFilter.class))).thenReturn(TEST_STATUSES);

        // when
        deflateCompression.compress(content, OUTPUT_DIR_NAME);

        // then
        verify(content).saveAsTextFile(OUTPUT_DIR_NAME, DeflateCodec.class);
    }

    @Test
    public void shouldDecompressDeflate() throws Exception {
        checkDecompress(INPUT_FILE, deflateCompression, sparkContext);
    }
}