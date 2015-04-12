package it.hadoppalo.reduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MaxOccurrenceReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, DoubleWritable> {

	private DoubleWritable result = new DoubleWritable();
	
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {

		int sum = 0;
		while(values.hasNext()){
			sum += values.next().get();
		}
		result.set(sum);
		output.collect(key, result);
		
	}

}
