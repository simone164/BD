package it.cache.downloader.reduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MaxOccurrenceReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		int maxValue = Integer.MIN_VALUE;
		while (values.hasNext()) {
			maxValue = Math.max(maxValue, values.next().get());
		}
		output.collect(key, new DoubleWritable(((double) maxValue) / 10));
	}

}
