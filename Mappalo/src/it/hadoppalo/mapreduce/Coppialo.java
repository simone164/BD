package it.hadoppalo.mapreduce;

import it.hadoppalo.map.CoppialoMapper;
import it.hadoppalo.map.CoppialoMapper2;
import it.hadoppalo.reduce.CoppialoReducer;
import it.hadoppalo.reduce.CoppialoReducer2;
import it.hadoppalo.reduce.IntComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Coppialo {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Path input = new Path(args[1]);
		Path output = new Path(args[2]);
		Path temp = new Path("/temp");

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Coppialo");
		job.setJarByClass(Coppialo.class);
		job.setMapperClass(CoppialoMapper.class);
		job.setCombinerClass(CoppialoReducer.class);
		job.setReducerClass(CoppialoReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, temp);
		boolean esit = job.waitForCompletion(true);

		if (esit) {

			@SuppressWarnings("deprecation")
			Job job1 = new Job(conf, "Coppialo2");
			job1.setJarByClass(Coppialo.class);
			job1.setMapperClass(CoppialoMapper2.class);
			job1.setSortComparatorClass(IntComparator.class);
			job1.setReducerClass(CoppialoReducer2.class);
			job1.setMapOutputKeyClass(IntWritable.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job1, temp);
			FileOutputFormat.setOutputPath(job1, output);
			job1.waitForCompletion(true);
		} else {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(1);

		}
	}
}
