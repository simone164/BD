package it.hadoppalo.mapreduce;

import it.hadoppalo.map.MaxOccurrenceMapper;
import it.hadoppalo.map.MaxOccurrenceMapper2;
import it.hadoppalo.reduce.MaxOccurrenceReducer;
import it.hadoppalo.reduce.MaxOccurrenceReducer2;
import it.hadoppalo.reduce.intComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxOccurrence {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
//		if (otherArgs.length != 2) {
//			System.err.println("Usage: wordcount <in> <out>");
//			System.exit(2);
//		}
		//conf.set("mapreduce.output.key.field.separator", ",");
		
		Path input = new Path(args[1]);
		Path temp = new Path("/temp");
		Path output = new Path("/output");
		
		Job job = new Job(conf, "MaxOccurrence");
		job.setJarByClass(MaxOccurrence.class);
		job.setMapperClass(MaxOccurrenceMapper.class);
		job.setCombinerClass(MaxOccurrenceReducer.class);
		job.setReducerClass(MaxOccurrenceReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, temp);
		boolean esit = job.waitForCompletion(true);

		if (esit) {

			Job job1 = new Job(conf, "MaxOccurrence2");
			job1.setJarByClass(MaxOccurrence.class);
			job1.setMapperClass(MaxOccurrenceMapper2.class);
			job1.setSortComparatorClass(intComparator.class);
			job1.setReducerClass(MaxOccurrenceReducer2.class);
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
