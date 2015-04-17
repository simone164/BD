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
			if (arrayData[0].contains(("2015-0-"))||
					arrayData[0].contains(("2015-1-"))||
					arrayData[0].contains(("2015-2-"))) {
				
				String dataEnorme = arrayData[0];
				String[] dataSplittata = dataEnorme.split("-");
				String data = dataSplittata[0] + "-" + dataSplittata[1];
					

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
