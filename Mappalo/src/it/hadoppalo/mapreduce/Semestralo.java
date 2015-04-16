package it.hadoppalo.mapreduce;

import it.hadoppalo.map.SemestraloMapper;
import it.hadoppalo.reduce.SemestraloReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Semestralo {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Path input = new Path(args[1]);
		Path output = new Path("/output");

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Semestralo");
		job.setJarByClass(Semestralo.class);
		job.setMapperClass(SemestraloMapper.class);
		job.setCombinerClass(SemestraloReducer.class);
		job.setReducerClass(SemestraloReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
		// boolean esit = job.waitForCompletion(true);

	}
}
