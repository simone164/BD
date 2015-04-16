package it.hadoppalo.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SemestraloReducer2 extends Reducer<Text, Text, Text, Text> {
	
	private Text finale = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String s = "";
		
		for (Text val : values) {
			
			 s += " " + val;

		}
		
		finale = new Text(s);
		
			context.write(key, finale);


	}

}