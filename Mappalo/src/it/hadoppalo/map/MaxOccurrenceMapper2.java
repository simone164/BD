package it.hadoppalo.map;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxOccurrenceMapper2 extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	private static IntWritable number = new IntWritable();
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// your map code goes here
		StringTokenizer itr = new StringTokenizer(value.toString());
		int count = 0;
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			String[] arrayCibo = token.split("\t");
			if (arrayCibo != null) {
//				String cane = "";
//				for(String s: arrayCibo){
//					cane += "*" + s + "*";
//				}
//				System.out.println(cane);
				count++;
				
				for (String s : arrayCibo) {
					
					if (count == 1) {
						Integer valoreCount = count;
						word = new Text(s);
						System.out.println(word + " count = "
								+ valoreCount.toString());
					}
					if (count == 2) {
						Integer valoreCount = count;
						number = new IntWritable(Integer.parseInt(s));
						System.out.println(number.toString() + " count = "
								+ valoreCount.toString());
						context.write(number, word);

					}
				}
				// word.set(arrayCibo[0]);
				// number = new IntWritable(Integer.parseInt(arrayCibo[1]));
			}

			//System.out.println((number.toString() + " *** " + word));
		}
	}
	
	private static boolean isWhitespace(String s) {
	    int length = s.length();
	    if (length > 0) {
	        for (int i = 0; i < length; i++) {
	            if (!Character.isWhitespace(s.charAt(i))) {
	                return false;
	            }
	        }
	        return true;
	    }
	    return false;
	}
}

// context.write(key, new
// IntWritable(Integer.getInteger(value.toString())));

