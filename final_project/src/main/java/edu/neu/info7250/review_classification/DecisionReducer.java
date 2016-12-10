package edu.neu.info7250.review_classification;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import net.sf.json.JSONObject;

public class DecisionReducer extends Reducer<Text, Text, Text, Text> {
	private MultipleOutputs<Text, Text> out;

	public void setup(Context context) {
		out = new MultipleOutputs<Text, Text>(context);
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean isPositive = false;
		String filename = null;
		String baseDir = null;

		LinkedList<String> list = new LinkedList<String>();
		for (Text value_text : values) {
			String value = value_text.toString().trim();
			if (value.equals("positive")) {
				isPositive = true;
				continue;
			} else if (value.equals("negative")) {
				continue;
			}
			list.add(value);
		}

		if (isPositive) {
			filename = "positive";
			baseDir = "positive_review";
		} else {
			filename = "negative";
			baseDir = "negaive_review";
		}

		for (String item : list) {
			JSONObject obj = JSONObject.fromObject(item);
			String bid = obj.getString("business_id");
//			String rid = obj.getString("review_id");
//			out.write(filename + "Index", bid + "\t" + rid, NullWritable.get(), baseDir + "_index");
			out.write(filename, bid + "\t" + item, NullWritable.get(), baseDir);
			Counter counter = null;
			if (isPositive) {
				counter = context.getCounter("Counter_Group", "result_positive");
				counter.increment(1);
			} else {
				counter = context.getCounter("Counter_Group", "result_negative");
				counter.increment(1);
			}
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		out.close();
	}
}
