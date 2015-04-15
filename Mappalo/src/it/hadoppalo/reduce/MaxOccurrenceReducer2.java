package it.hadoppalo.reduce;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MaxOccurrenceReducer2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> arg2, Reporter arg3) throws IOException {
        while ((values.hasNext())) {
            arg2.collect(key, values.next());
        }
    }

}