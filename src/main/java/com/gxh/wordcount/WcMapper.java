package com.gxh.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//Longwritable表示每一行的偏移量
public class WcMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    //context 表示任务执行的全过程
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到当前行数据
        String string = value.toString();
        //通过空格切分数据
        String[] words = string.split(" ");
        for (String word : words) {
            //将word交给框架
            this.word.set(word);
            context.write(this.word,one);
        }
    }
}
