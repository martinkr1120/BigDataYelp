package edu.neu.info7250.review_classification;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import net.sf.json.JSONObject;

public class CountMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		JSONObject review_json = JSONObject.fromObject(line);

		try {
			String review_text = review_json.getString("text");
//			String business_id = review_json.getString("business_id");
//			String user_id = review_json.getString("user_id");
			String review_id = review_json.getString("review_id");
			review_text = review_text.replaceAll("([a-zA-z]+://[^\\s]*)|(\\n\\s*\\r)", " ")
					.replaceAll("[^A-Za-z]+", " ");

			int star = review_json.getInt("stars");
			if (star >= 3) {
				// context.write(new Text("pos@@@"), new Text(business_id
				// + "@@@" + user_id + "@@@" + review_text));
				context.write(new Text("pos@@@"), new Text("something"));
				String[] words = review_text.split("\\s+");

				for (String word : words) {
					if (word.length() > 2) {
						Counter count_pos_word = context.getCounter("Counter_Group", "num_pos_word");
						count_pos_word.increment(1);
						context.write(new Text(word.toLowerCase()),
//								new Text("pos" + "@@@" + business_id + "@@@" + user_id));
								new Text("pos" + "@@@" + review_id));
					}
				}
			} else {
				// context.write(new Text("neg@@@"), new Text(business_id
				// + "@@@" + user_id + "@@@" + review_text));
				context.write(new Text("neg@@@"), new Text("something"));
				String[] words = review_text.split("\\s+");
				for (String word : words) {
					if (word.length() > 2) {
						Counter count_neg_word = context.getCounter("Counter_Group", "num_neg_word");
						count_neg_word.increment(1);
						context.write(new Text(word.toLowerCase()),
//								new Text("neg" + "@@@" + business_id + "@@@" + user_id));
								new Text("neg" + "@@@" + review_id));
					}
				}
			}
//			context.write(new Text("both-pos-neg*****"), new Text("something"));
		} catch (Exception e) {
			System.out.println("something wrong with json parser");
			e.printStackTrace();
		}

	}
}
