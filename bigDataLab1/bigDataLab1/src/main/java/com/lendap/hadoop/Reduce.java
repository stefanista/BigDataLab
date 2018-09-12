package com.lendap.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;





import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class Reduce
  extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		HashMap<String, String> map = new HashMap<String, String>();
		StringBuffer sb = new StringBuffer();
		
		//values = BCD , ACDE 
		//Put in array
		
		for (Text val : values) {
			List <String> temp = Arrays.asList(val.toString().split(","));
			
			for (String friend : temp){
				if (map.containsKey(friend)){
					sb.append(friend + ",");
				}
				else {
					map.put(friend, "1");
				}
			}	
		}
		
		if (sb.lastIndexOf(",") > -1){
			sb.deleteCharAt(sb.lastIndexOf(","));
		}
		
		context.write(null, new Text("(" + key.toString() + ") -> (" + sb.toString() +")"));
		
		
	}
}
