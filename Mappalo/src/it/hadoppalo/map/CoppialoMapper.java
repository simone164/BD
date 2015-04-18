package it.hadoppalo.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoppialoMapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		
		int i, j;
        while (itr.hasMoreTokens()) {
        	String[] dataCibi = itr.nextToken().split(",");
        	String[] cibi = Arrays.copyOfRange(dataCibi, 1, dataCibi.length);
//        	System.out.println("######################");
        	
//        	for (String s : cibi) {
//        		System.out.println("********************" + s);
//        	}       	
        	
        	for (i = 0; i<cibi.length-1; i++) {
//        		System.out.println("s: " + cibi[i].toString());
        		String s1 = cibi[i].toString();
        		for(j=i+1; j<cibi.length; j++){
//        			System.out.println("#: " + cibi[j].toString());
        			String s2 = cibi[j].toString();
        			List<String> listaCibi = new ArrayList<String>();
        			listaCibi.add(s1);
        			listaCibi.add(s2);
                	Collections.sort(listaCibi);
//                	System.out.println("Primo elemento. " + listaCibi.get(0));
//                	System.out.println("Secondo elemento. " + listaCibi.get(1));
                	
                	word.set(listaCibi.get(0) + "-" + listaCibi.get(1));
					context.write(word, one);
        		}
        	}
        }
	}
}