package com.gxh.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Longwritable表示每一行的偏移量
public class WcMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
}
