package com.gxh.writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    private FlowBean flow = new FlowBean();
    private Text phone = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] files = string.split("\t");
        phone.set(files[0]);
        flow.setUpflow(Long.parseLong(files[1]));
        flow.setDownflow(Long.parseLong(files[2]));
        flow.setSumflow(Long.parseLong(files[3]));
        context.write(flow,phone);
    }
}
