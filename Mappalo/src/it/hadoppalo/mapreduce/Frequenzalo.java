package it.hadoppalo.mapreduce;

import it.hadoppalo.map.FrequenzaloMapper;
import it.hadoppalo.reduce.FrequenzaloReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Frequenzalo {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Path input = new Path(args[1]);
		Path output = new Path("/output");

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Frequenzalo");
		job.setJarByClass(Frequenzalo.class);
		job.setMapperClass(FrequenzaloMapper.class);
		job.setCombinerClass(FrequenzaloReducer.class);
		job.setReducerClass(FrequenzaloReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
//		boolean esit = job.waitForCompletion(true);
	}
}
