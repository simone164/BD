package it.hadoppalo.map;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoppialoMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {

	private static IntWritable number = new IntWritable();
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		int count = 0;
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			String[] arrayCibo = token.split("\t");
			if (arrayCibo != null) {
				count++;

				for (String s : arrayCibo) {

					if (count == 1) {
						word = new Text(s);
					}
					if (count == 2) {
						number = new IntWritable(Integer.parseInt(s));
						context.write(number, word);

					}
				}
			}
		}
	}
}