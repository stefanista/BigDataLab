package com.lendap.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

public class Map
  extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");
        String line = null;
        String[] keyArray = null;
        String[] friendArray = null;
        String[] tempArray = null;
        while(tokenizer.hasMoreTokens()){
                line = tokenizer.nextToken();
                keyArray = line.split("->");
                friendArray = keyArray[1].split(",");
                tempArray = new String[2];
                for(int i = 0; i < friendArray.length; i++){
                        tempArray[0] = friendArray[i];
                        tempArray[1] = keyArray[0];
                        
                        Arrays.sort(tempArray);
                        context.write(new Text(tempArray[0] + tempArray[1]), new Text(keyArray[1]));
                }
        }
	
	}
}
