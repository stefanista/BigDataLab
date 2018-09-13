package com.lendap.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

public class Map
  extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		 StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	        word.set(itr.nextToken());
	        context.write(word, one);
	      }
	
	}
}
