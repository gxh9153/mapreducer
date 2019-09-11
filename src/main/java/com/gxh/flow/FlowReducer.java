package com.gxh.flow;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    private FlowBean sumFlow  = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
       long sumUpFlow = 0;
       long sumDownFlow = 0;
        for (FlowBean value : values) {
            sumUpFlow += value.getUpflow();
            sumDownFlow += value.getDownflow();
        }
        sumFlow.set(sumUpFlow,sumDownFlow);
        context.write(key,sumFlow);
    }
}
