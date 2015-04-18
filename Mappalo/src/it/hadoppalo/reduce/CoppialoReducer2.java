package it.hadoppalo.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CoppialoReducer2 extends Reducer<IntWritable, Text, Text, IntWritable> {

	private int count = 0;
	
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		count++;
		if (count < 11){
			for (Text val : values) {
				context.write(val, key);
			}
		}
		

	}

}
