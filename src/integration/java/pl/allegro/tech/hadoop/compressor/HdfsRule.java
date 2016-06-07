package pl.allegro.tech.hadoop.compressor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.rules.ExternalResource;
import pl.allegro.tech.hadoop.compressor.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class HdfsRule extends ExternalResource {

    private final String tempDirectoryPath;
    private File baseDirectory;
    private MiniDFSCluster hdfsCluster;
    private String hdfsURI;
    private FileSystem fileSystem;

    public HdfsRule(String tempDirectoryPath) {
        this.tempDirectoryPath = tempDirectoryPath;
    }

    @Override
    protected void before() throws Throwable {
        create();
    }

    @Override
    protected void after() {
        destroy();
    }

    public File getBaseDirectory() {
        return baseDirectory;
    }

    public MiniDFSCluster getHdfsCluster() {
        return hdfsCluster;
    }

    public String getHdfsURI() {
        return hdfsURI;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    private void create() throws IOException {
        baseDirectory = Files.createTempDirectory("hdfs").toFile();
        FileUtil.fullyDelete(baseDirectory);

        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDirectory.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
        fileSystem = FileSystemUtils.getFileSystem(conf);
    }

    private void destroy() {
        hdfsCluster.shutdown();
        FileUtil.fullyDelete(baseDirectory);
    }
}
