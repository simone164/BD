package it.hadoppalo.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TeSeiCapitoloMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	private final long one = 1;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String testo = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(testo, " ,");
		String precedente = null;
		String successivo = null;
		int j = 0;
		boolean first = true;
		List<String> Estrazioni = new ArrayList<String>();
		while (tokenizer.hasMoreTokens()) {
			String item = tokenizer.nextToken();
			if (first) {
				first = false;
			} else {
				Estrazioni.add(item);
				context.write(new Text(item), new LongWritable(one));
			}
		}
		for (int i = 0; i < Estrazioni.size(); i++) {
			j = i + 1;
			precedente = Estrazioni.get(i);
			while (j < Estrazioni.size()) {
				successivo = Estrazioni.get(j);
				if (precedente.compareTo(successivo) < 0)
					context.write(new Text(precedente + "#" + successivo),
							new LongWritable(one));
				else if (precedente.compareTo(successivo) > 0)
					context.write(new Text(successivo + "#" + precedente),
							new LongWritable(one));
				j++;
			}
		}

	}
}
