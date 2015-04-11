package it.cache.downloader.mappalo.mapreduce;

import it.cache.downloader.map.MaxOccurrenceMapper;
import it.cache.downloader.reduce.MaxOccurrenceReducer;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Text;

public class MaxOccurrence {

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(MaxOccurrence.class);
		conf.setJobName("Max occurrence");
		FileInputFormat.addInputPath(conf, new Path("occurrence.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("output"));
		conf.setMapperClass(MaxOccurrenceMapper.class);
		conf.setReducerClass(MaxOccurrenceReducer.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		JobClient.runJob(conf);
	}

}
