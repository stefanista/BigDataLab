package com.lendap.hadoop;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;







import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class Reduce
  extends org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,FloatWritable>  {
	 
    private FloatWritable result = new FloatWritable();
    Float average = 0f;
    Float count = 0f;
    int sum = 0;
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
	
        Text sumText = new Text(key + "              average");
        
        //counts how many of each word
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        // counts how many words total
        count += 1;
        
        //calculates the average of each word
        average = sum/count;
        result.set(average);
        context.write(sumText, result);

    }
}
