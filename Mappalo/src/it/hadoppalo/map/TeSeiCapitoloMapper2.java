package it.hadoppalo.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TeSeiCapitoloMapper2 extends
		Mapper<LongWritable, Text, Text, Text> {

	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] components = line.split("\t");
		String element = components[0];
		String[] elComp = element.split("#");
		if (elComp.length==1)
			context.write(new Text(element), new Text(components[1]));
		else if (elComp.length>1) {
			context.write(new Text(elComp[0]), new Text(element+"#"+components[1]));
			context.write(new Text(elComp[1]), new Text(elComp[1]+"#"+elComp[0]+"#"+components[1]));
		}
	}
}
