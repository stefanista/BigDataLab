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


public class BigDataLab1 {
	
    public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
            System.err.println("Usage: BigDataLab1 <in_dir> <out_dir>");
            System.exit(2);
        }
    	Configuration conf = new Configuration();
    	
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "BigDataLab1");
        job.setJarByClass(BigDataLab1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        job.setMapperClass(Map.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(Reduce.class);
 
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.waitForCompletion(true);
    }
}