package it.hadoppalo.mapreduce;

import it.hadoppalo.map.SemestraloMapper;
import it.hadoppalo.map.SemestraloMapper2;
import it.hadoppalo.reduce.SemestraloReducer;
import it.hadoppalo.reduce.SemestraloReducer2;

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
		Path output = new Path(args[2]);
		Path temp = new Path("/temp");

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
		FileOutputFormat.setOutputPath(job, temp);
		job.waitForCompletion(true);
		boolean esit = job.waitForCompletion(true);

		if (esit) {

			@SuppressWarnings("deprecation")
			Job job1 = new Job(conf, "Semestralo2");
			job1.setJarByClass(Semestralo.class);
			job1.setMapperClass(SemestraloMapper2.class);
			//job1.setSortComparatorClass(IntComparator.class);
			job1.setReducerClass(SemestraloReducer2.class);
			job1.setMapOutputKeyClass(Text.class);
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