package it.hadoppalo.map;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SemestraloMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	private Text number = new Text();
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		int count = 0;

		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();

			if (token.contains(",")) {

				String[] arrayStringa = token.split(",");
				String nome = "";
				String data = "";
				for (String cercaS : arrayStringa) {
					count++;
					if (count == 1) {
						nome = cercaS;
						word.set(nome);
					}

					if (count == 2) {

						data = cercaS;
						String meseResult = "";

						String[] nomeDataSplit = data.split("-");
						if (nomeDataSplit != null) {

							String anno = nomeDataSplit[0];

							String mese = nomeDataSplit[1];
							if (mese.equals("0")) {
								mese = "1";
							} else if (mese.equals("1")) {
								mese = "2";
							} else if (mese.equals("2")) {
								mese = "3";
							}

							meseResult = mese + "/" + anno;

							number.set(meseResult);
						}
					}

				}

			} else {
				String numberProva = number.toString();

				String totale = numberProva + ":" + token;

				number.set(totale);

				context.write(word, number);

			}

		}

	}

}
