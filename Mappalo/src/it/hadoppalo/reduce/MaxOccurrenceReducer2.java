package it.hadoppalo.reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxOccurrenceReducer2 extends
		Reducer<IntWritable, Text, Text, IntWritable> {

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		//System.out.println(key.toString() + " + + " + values);

		for (Text val : values) {
			
			context.write(val, key);
			
		}
		

	}
	
	
}
