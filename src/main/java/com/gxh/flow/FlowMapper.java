package com.gxh.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text, FlowBean> {

    private Text phone = new Text();
    private FlowBean flow = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] files = string.split("\t");
        phone.set(files[1]);
        long upflow = Long.parseLong(files[files.length-3]);
        long downflow = Long.parseLong(files[files.length-2]);
        flow.set(upflow,downflow);
        context.write(phone,flow);
    }
}
