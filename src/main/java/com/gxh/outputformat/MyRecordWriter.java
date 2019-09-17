package com.gxh.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileNotFoundException;
import java.io.IOException;

public class MyRecordWriter extends RecordWriter {
    private FSDataOutputStream gxh;
    private FSDataOutputStream other;

    /**
     * 初始化方法
     * @throws FileNotFoundException
     */
    public void initialize(TaskAttemptContext job) throws IOException {

        String outdir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        gxh = fileSystem.create(new Path(outdir +"/gxh.log"));
        other = fileSystem.create(new Path(outdir +"/other.log"));
    }

    @Override
    public void write(Object key, Object value) throws IOException, InterruptedException {
        String s = value.toString() + "\n";
        if(s.contains("gxh")){
            gxh.write(s.getBytes());
        }else{
            other.write(s.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(gxh);
        IOUtils.closeStream(other);
    }
}
