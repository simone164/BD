package it.hadoppalo.reduce;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MaxOccurrenceReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();
	
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		int sum = 0;
		while(values.hasNext()){
			sum += values.next().get();
		}
		result.set(sum);
		
	//	Text testo = new Text(key + " " + result.toString());
		
		output.collect(key, result);
		
		
	}


}
