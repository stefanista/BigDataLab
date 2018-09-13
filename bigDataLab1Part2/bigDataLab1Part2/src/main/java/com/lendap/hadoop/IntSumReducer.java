package com.lendap.hadoop;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//This is the combiner class 

public class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		throws IOException, InterruptedException {

		//count how many words exist
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();

		}

		result.set(sum);
		context.write(key, result);

	}

}