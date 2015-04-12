package it.hadoppalo.mapreduce;

import it.hadoppalo.map.MaxOccurrenceMapper;
import it.hadoppalo.reduce.MaxOccurrenceReducer;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class MaxOccurrence {

	public static void main(String[] args) throws IOException {
		
		JobConf conf = new JobConf(MaxOccurrence.class);
		conf.setJobName("MaxOccurrence");
		FileInputFormat.addInputPath(conf, new Path("/input/esempio.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("/output/output.txt"));
		conf.setMapperClass(MaxOccurrenceMapper.class);
		conf.setReducerClass(MaxOccurrenceReducer.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		JobClient.runJob(conf);
	}

}
