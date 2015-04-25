package it.semestralo.mapreduce;

import it.semestralo.map.SemestreMapper;
import it.semestralo.reduce.SemestreReducer;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
public class SemestreSale {
	
	public static void main(String[] args) throws IOException{
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		JobConf job = new JobConf(SemestreSale.class);
		job.setJobName("SemesetreSale");
		job.setJarByClass(SemestreSale.class);
		
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(SemestreMapper.class);
		job.setCombinerClass(SemestreReducer.class);
		job.setReducerClass(SemestreReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		JobClient.runJob(job);

		
	}

}
