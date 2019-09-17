package com.gxh.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

//自定义RecordReader 处理一个文件，将这个文件直接读成kv值
public class WholeFileRecordReader extends RecordReader {

    private Boolean notRead = true;

    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    private FSDataInputStream inputStream;

    private FileSplit fileSplit;

    /**
     * 初始化方法，框架会在开始的时候调用一次
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //转换切片类型到文件切片
        fileSplit = (FileSplit) split;
        //通过切片获取路径
        Path path = fileSplit.getPath();
        //通过路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        //通过文件系统开流
        inputStream = fileSystem.open(path);
    }

    /**
     * 读取下一组kv值
     * @return 如果读到，则返回true,否则返回false
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(notRead){
            //具体的读操作
            //读key
            key.set(fileSplit.getPath().toString());
            //读value
            byte[] buf= new byte[(int) fileSplit.getLength()];
            inputStream.read(buf);
            value.set(buf,0,buf.length);
            notRead = false;
            return true;
        }else{
            return false;
        }
    }

    /**
     * 读取当前key 的值
     * @return 返回key值
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 读取当前value的值
     * @return 返回当前value的值
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 当前数据的读取进度
     * @return 返回当前进度
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notRead ? 0:1;
    }

    /**
     * 关闭资源
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        IOUtils.closeStream(inputStream);
    }
}
