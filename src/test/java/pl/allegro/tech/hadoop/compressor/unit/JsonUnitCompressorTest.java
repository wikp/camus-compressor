package pl.allegro.tech.hadoop.compressor.unit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import pl.allegro.tech.hadoop.compressor.InputAnalyser;
import pl.allegro.tech.hadoop.compressor.compression.Compression;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static pl.allegro.tech.hadoop.compressor.Utils.fileStatusForEmptyFile;
import static pl.allegro.tech.hadoop.compressor.Utils.fileStatusForPath;

@RunWith(MockitoJUnitRunner.class)
public class JsonUnitCompressorTest {

    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private FileSystem fileSystem;

    @Mock
    private Compression compression;

    @Mock
    private JavaRDD<String> testRDD;

    private UnitCompressor unitCompressor;

    @Before
    public void setUp() {
        InputAnalyser analyser = new InputAnalyser(fileSystem, compression, false);
        unitCompressor = new JsonUnitCompressor(sparkContext, fileSystem, compression, analyser);
    }

    @Test
    public void shouldCompress() throws Exception {
        // given
        when(sparkContext.textFile(eq(UNIT_PATH_NAME))).thenReturn(testRDD);
        when(testRDD.repartition(anyInt())).thenReturn(testRDD);
        when(fileSystem.globStatus(any(Path.class))).thenReturn(TEST_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);
        when(compression.getSplits(anyLong())).thenReturn(TEST_NUM_SPLITS);

        // when
        unitCompressor.compress(UNIT_PATH);

        // then
        verify(compression).compress(same(testRDD), anyString());
    }

    @Test
    public void shouldContinueWhenSuccessFileNotExists() throws Exception {
        // given
        when(sparkContext.textFile(eq(UNIT_PATH_NAME))).thenReturn(testRDD);
        when(testRDD.repartition(anyInt())).thenReturn(testRDD);
        when(fileSystem.globStatus(any(Path.class))).thenReturn(TEST_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);
        when(compression.getSplits(anyLong())).thenReturn(TEST_NUM_SPLITS);

        when(fileSystem.exists(any(Path.class))).thenAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                return !isSuccessFile((Path) args[0]);
            }
        });

        // when
        unitCompressor.compress(UNIT_PATH);

        // then
        verify(compression).compress(same(testRDD), anyString());
        verifyCleanup();
        verify(fileSystem).delete(not(eq(UNIT_PATH)), eq(true));
    }

    @Test
    public void shouldNotCompressWhenNoFiles() throws IOException {
        // given
        when(fileSystem.globStatus(any(Path.class))).thenReturn(EMPTY_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);

        // when
        unitCompressor.compress(UNIT_PATH);

        // then
        verify(compression, never()).compress(any(JavaRDD.class), anyString());
    }

    @Test
    public void shouldNotCompressWhenEmptyFiles() throws Exception {
        // given
        when(fileSystem.globStatus(any(Path.class))).thenReturn(EMPTY_FILES_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);

        // when
        unitCompressor.compress(UNIT_PATH);

        // then
        verify(compression, never()).compress(any(JavaRDD.class), anyString());
    }

    @Test
    public void shouldNotCompressAlreadyCompressedFiles() throws Exception {
        // given
        when(fileSystem.globStatus(any(Path.class))).thenReturn(COMPRESSED_TEST_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);

        // when
        unitCompressor.compress(UNIT_PATH);

        // then
        verify(compression, never()).compress(any(JavaRDD.class), anyString());
    }

    @Test
    public void shouldNotCompressWhenSuccessFileExists() throws Exception {
        // given
        when(fileSystem.globStatus(any(Path.class))).thenReturn(TEST_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);
        when(fileSystem.exists(any(Path.class))).thenReturn(true);

        // when
        unitCompressor.compress(UNIT_PATH);

        // then
        verify(compression, never()).compress(any(JavaRDD.class), anyString());
        verifyCleanup();
    }

    private void verifyCleanup() throws IOException {
        verify(fileSystem).delete(eq(UNIT_PATH), eq(true));
        ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
        verify(fileSystem).delete(pathCaptor.capture(), eq(false));
        assertTrue(isSuccessFile(pathCaptor.getValue()));
    }

    private boolean isSuccessFile(Path path) {
        return path.toString().endsWith("_SUCCESS");
    }

    private static final String COMPRESSED_EXTENSION = "ext";
    private static final String UNCOMPRESSED_EXTENSION = "other_extn";
    private static final String UNIT_NAME = "test_unit";
    private static final Path UNIT_PATH = new Path(UNIT_NAME);
    private static final String UNIT_PATH_NAME = "test_unit/*";
    private static final String FILE_NAME_TO_COMPRESS = "test_file." + UNCOMPRESSED_EXTENSION;
    private static final Path FILE_NAME_TO_COMPRESS_PATH = new Path(FILE_NAME_TO_COMPRESS);
    private static final FileStatus[] TEST_STATUSES = {fileStatusForPath(FILE_NAME_TO_COMPRESS_PATH)};
    private static final int TEST_NUM_SPLITS = 4;
    private static final FileStatus[] EMPTY_FILES_STATUSES = {fileStatusForEmptyFile(FILE_NAME_TO_COMPRESS_PATH)};

    private static final String COMPRESSED_FILE_NAME = "test_file." + COMPRESSED_EXTENSION;
    private static final Path COMPRESSED_PATH = new Path(COMPRESSED_FILE_NAME);
    private static final FileStatus[] COMPRESSED_TEST_STATUSES = {fileStatusForPath(COMPRESSED_PATH)};

    private static final FileStatus[] EMPTY_STATUSES = {};


}
