package com.gxh.groupingcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    private OrderBean orderBean = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] fileds = string.split("\t");
        orderBean.setOrderId(fileds[0]);
        orderBean.setProductId(fileds[1]);
        orderBean.setPrice(Double.parseDouble(fileds[2]));
        context.write(orderBean,NullWritable.get());
    }
}
