package it.hadoppalo.mapreduce;

import it.hadoppalo.map.TeSeiCapitoloMapper;
import it.hadoppalo.map.TeSeiCapitoloMapper2;
import it.hadoppalo.reduce.TeSeiCapitoloReducer;
import it.hadoppalo.reduce.TeSeiCapitoloReducer2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TeSeiCapitolo {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Path input = new Path(args[1]);
		Path output = new Path(args[2]);
		Path temp = new Path("/temp");

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "TeSeiCapitolo");
		job.setJarByClass(TeSeiCapitolo.class);
		job.setMapperClass(TeSeiCapitoloMapper.class);
		job.setReducerClass(TeSeiCapitoloReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, temp);
		boolean esit = job.waitForCompletion(true);

		if (esit) {

			@SuppressWarnings("deprecation")
			Job job1 = new Job(conf, "TeSeiCapitolo2");
			job1.setJarByClass(TeSeiCapitolo.class);
			job1.setMapperClass(TeSeiCapitoloMapper2.class);
			job1.setReducerClass(TeSeiCapitoloReducer2.class);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(job1, temp);
			FileOutputFormat.setOutputPath(job1, output);
			job1.waitForCompletion(true);
		} else {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(1);

		}
	}
	
}
