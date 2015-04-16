package it.hadoppalo.semestralo;

import it.hadoppalo.map.SemestraloMapper;
import it.hadoppalo.reduce.SemestraloReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Semestralo {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// if (otherArgs.length != 2) {
		// System.err.println("Usage: wordcount <in> <out>");
		// System.exit(2);
		// }
		// conf.set("mapreduce.output.key.field.separator", ",");

		Path input = new Path(args[1]);
		Path output = new Path("/output");

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
		boolean esit = job.waitForCompletion(true);

	}
}
