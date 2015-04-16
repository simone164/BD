package it.hadoppalo.map;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SemestraloMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());

		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			String[] arrayData = token.split(",");
			if (arrayData[0].contains(("2015"))) {
				
				String data = arrayData[0];

				for (String s : arrayData) {
					if (s.equals(arrayData[0])) {
						// NONPRENDELADATA
					} else {
						word.set(s+","+data);
						// System.out.println(word + " + " + one.toString());
						context.write(word, one);
					}
				}
			}
		}
	}

}
