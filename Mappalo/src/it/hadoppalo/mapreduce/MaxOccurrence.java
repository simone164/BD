package it.hadoppalo.mapreduce;

import it.hadoppalo.map.MaxOccurrenceMapper;
import it.hadoppalo.map.MaxOccurrenceMapper2;
import it.hadoppalo.reduce.MaxOccurrenceReducer;
import it.hadoppalo.reduce.MaxOccurrenceReducer2;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MaxOccurrence {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Path input = new Path(args[1]);
		Path temp1 = new Path("temp");
		Path output = new Path(args[2]);

		JobConf conf = new JobConf(MaxOccurrence.class);
		conf.setJobName("MaxOccurrence");
		conf.setJarByClass(MaxOccurrence.class);
		FileInputFormat.setInputPaths(conf, input);
		FileOutputFormat.setOutputPath(conf, temp1);

		conf.setMapperClass(MaxOccurrenceMapper.class);
		conf.setCombinerClass(MaxOccurrenceReducer.class);
		conf.setReducerClass(MaxOccurrenceReducer.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);


//		conf.setInputFormat(TextInputFormat.class);
//		conf.setOutputFormat(TextOutputFormat.class);

		JobClient.runJob(conf);
		if (JobClient.runJob(conf).isComplete()) {
			

			JobConf conf2 = new JobConf(MaxOccurrence.class);
			conf2.setJobName("MaxOccurrence2");
			//FileInputFormat.setInputPaths(conf,  temp1);
			FileInputFormat.addInputPath(conf2, temp1);
			FileOutputFormat.setOutputPath(conf2, output);
			conf2.setJarByClass(MaxOccurrence.class);
			
			conf2.setMapperClass(MaxOccurrenceMapper2.class);
			conf2.setCombinerClass(MaxOccurrenceReducer2.class);
			conf2.setReducerClass(MaxOccurrenceReducer2.class);

			//conf2.setInputFormat(TextInputFormat.class);
			//conf2.setOutputFormat(TextOutputFormat.class);
			

			conf2.setMapOutputKeyClass(Text.class);
			conf2.setMapOutputValueClass(IntWritable.class);
			
			conf2.setOutputKeyClass(Text.class);
			conf2.setOutputValueClass(LongWritable.class);

			JobClient.runJob(conf2);

		}

	}

}
