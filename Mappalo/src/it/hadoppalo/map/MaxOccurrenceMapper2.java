package it.hadoppalo.map;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MaxOccurrenceMapper2 extends MapReduceBase implements
		Mapper<Text , LongWritable, IntWritable, Text> {

	public void map(Text key, LongWritable value, OutputCollector<IntWritable, Text> collector, Reporter arg3) throws IOException {
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line);
        {
            int number = 999;
            String word = "empty";

            if (stringTokenizer.hasMoreTokens()) {
                String str0 = stringTokenizer.nextToken();
                word = str0.trim();
            }

            if (stringTokenizer.hasMoreElements()) {
                String str1 = stringTokenizer.nextToken();
                number = Integer.parseInt(str1.trim());
            }
            collector.collect(new IntWritable(number), new Text(word));
        }

    
	}

}
