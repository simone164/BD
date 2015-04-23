package it.hadoppalo.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

	
	public class TeSeiCapitoloReducer2 extends Reducer<Text, Text, Text, DoubleWritable> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String, Double> mapCouple = new HashMap<String, Double>();
			double quantitySingle = -1.0;
			for(Text text : values) {
				String[] compValue = text.toString().split("#");
				if (compValue.length==1)
					quantitySingle = Double.parseDouble(compValue[0]);
				else if(compValue.length>2) {
					mapCouple.put(compValue[0]+","+compValue[1], Double.parseDouble(compValue[2]));
				}		
			}
			for(String sKey : mapCouple.keySet()) {
				double rapFreq = (mapCouple.get(sKey))/quantitySingle;
				context.write(new Text(sKey+"="), new DoubleWritable(rapFreq));
			}
		}
}
