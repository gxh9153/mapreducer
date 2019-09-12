package com.gxh.partitioner;

import com.gxh.flow.FlowBean;
import com.gxh.flow.FlowMapper;
import com.gxh.flow.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyPartitionerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取job对象
        Job job = Job.getInstance(new Configuration());

        //设置类路径
        job.setJarByClass(MyPartitionerDriver.class);

        //设置mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //设置分区大小和自定义partitioner的类路径
        job.setNumReduceTasks(5);
        job.setPartitionerClass(MyPartitioner.class);


        //设置mapper和reducer的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置输入输出数据
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
