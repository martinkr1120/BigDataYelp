package edu.neu.info7250.review_classification;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import net.sf.json.JSONObject;

public class DecisionMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		if (line.startsWith("positive::") || line.startsWith("negative::")) {
			String[] arr = line.split("::");
			context.write(new Text(arr[1]), new Text(arr[0]));
		} else {
			JSONObject review = JSONObject.fromObject(line);
//			String bid = review.getString("business_id");
//			String uid = review.getString("user_id");
//			context.write(new Text(bid + "@@@" + uid), value);
			String rid = review.getString("review_id");
			context.write(new Text(rid), value);
		}
	}
}
