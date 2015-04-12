package it.hadoppalo.map;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MaxOccurrenceMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text cibo = new Text();

	@Override
	public void map(LongWritable writable, Text text, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {

		StringTokenizer itr = new StringTokenizer(text.toString());
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			// System.out.println(i + ": "+ token);
			String[] arrayData = token.split(",");
			String[] arrayString = Arrays.copyOfRange(arrayData, 1, arrayData.length);
			;
			for (String s : arrayString) {
				// System.out.println("i=" + i + ": " + s.toString());
				cibo.set(s);
				collector.collect(cibo, one);
			}
		}

	}

}
