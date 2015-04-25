package it.semestralo.reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SemestreReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		Text testo = new Text();
		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}
		result.set(sum);

		// Text testo = new Text(key + " " + result.toString());

		output.collect(key, result);

	}
}