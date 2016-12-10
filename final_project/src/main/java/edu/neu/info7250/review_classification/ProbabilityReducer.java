package edu.neu.info7250.review_classification;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProbabilityReducer extends Reducer<CompositeKeyWritable, NullWritable, Text, NullWritable> {
	public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {

		boolean isFirstOne = true;
		String probabilityString = null;
		Double pos_pro = null;
		Double neg_pro = null;

		try {
			for (NullWritable value : values) {
				if (isFirstOne) {
					isFirstOne = false;
					probabilityString = key.getCount_or_rid();
					String[] proArr = probabilityString.split("@@@@@@");
					pos_pro = Double.parseDouble(proArr[0].split(":")[1]);
					neg_pro = Double.parseDouble(proArr[1].split(":")[1]);
				} else {
					if (pos_pro == null || neg_pro == null)
						return;
					context.write(
							new Text(new StringBuilder().append(key.getWord()).append(",").append(pos_pro).append(",")
									.append(neg_pro).append(",").append(key.getCount_or_rid()).toString()),
							NullWritable.get());
				}
			}
		} catch (Exception e) {
			System.out.println("-------------Something wrong in the probability reducer----------");
			e.printStackTrace();
		}
	}
}
