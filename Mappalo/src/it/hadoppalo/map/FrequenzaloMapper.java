package it.hadoppalo.map;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequenzaloMapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	
		StringTokenizer st = new StringTokenizer(value.toString());
	    
	    while (st.hasMoreTokens()) {
	    	String[] dataCibi = st.nextToken().split(",");
	    	String[] cibi = Arrays.copyOfRange(dataCibi, 1, dataCibi.length);
	    	
	    	List<String> listCibi = Arrays.asList(cibi);
	    	if (listCibi.size() < 5) {
	    		Collections.sort(listCibi);
		    	
		    	String bellaDiPadella = "";
		    	
		    	for (String s : listCibi) {
		    		bellaDiPadella += s + " ";
		    	}
		    	word.set(bellaDiPadella);
				context.write(word, one);
	    	}
	    	
	    } 
	}
}
