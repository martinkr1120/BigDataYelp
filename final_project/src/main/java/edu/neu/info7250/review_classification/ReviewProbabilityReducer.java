package edu.neu.info7250.review_classification;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewProbabilityReducer extends Reducer<Text, Text, Text, NullWritable> {
	public void reduce(Text key, Iterable<Text> values, Context context) {
		Configuration conf = context.getConfiguration();
		double pos_product = conf.getFloat("pos_pro_review", -1);
		double neg_product = conf.getFloat("neg_pro_review", -1);

		if (pos_product == -1 || neg_product == -1)
			return;

		for (Text value_text : values) {
			String value = value_text.toString();
			String[] arr = value.split(",");
			double pos_pro = Double.parseDouble(arr[0]);
			double neg_pro = Double.parseDouble(arr[1]);

			pos_product *= pos_pro;
			neg_product *= neg_pro;
		}

		try {
			if (pos_product > neg_product) {
				context.write(new Text("positive::" + key), NullWritable.get());
			}else{
				context.write(new Text("negative::" + key), NullWritable.get());
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
