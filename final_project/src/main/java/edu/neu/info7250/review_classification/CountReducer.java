package edu.neu.info7250.review_classification;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String key_str = key.toString();
		int count = 0;

		if (key_str.equals("pos@@@") || key_str.equals("neg@@@")) {
			for (Text value : values) {
				count++;
			}

			if (key_str.equals("pos@@@")) {
				Counter con = context.getCounter("Counter_Group", "num_pos_review");
				con.increment(count);
			} else {
				Counter con = context.getCounter("Counter_Group", "num_neg_review");
				con.increment(count);
			}

		} else {
			int num_pos_in_review = 0;
			int num_neg_in_review = 0;
			boolean pos_first_occur = true;
			boolean neg_first_occur = true;
			//StringBuilder sb = new StringBuilder();

			for (Text value_text : values) {
				String value = value_text.toString();
				String[] arr = value.split("@@@");
				// sb.append(",").append(arr[1]).append("@@@").append(arr[2]);
//				sb.append(",").append(arr[1]);

				if (arr[0].equals("pos")) {
					if (pos_first_occur) {
						Counter count_unique_pos_word = context.getCounter("Counter_Group", "num_unique_pos_word");
						count_unique_pos_word.increment(1);
						pos_first_occur = false;
					}
					num_pos_in_review++;
				} else {
					if (neg_first_occur) {
						Counter count_unique_neg_word = context.getCounter("Counter_Group", "num_unique_neg_word");
						count_unique_neg_word.increment(1);
						neg_first_occur = false;
					}
					num_neg_in_review++;
				}
				
				context.write(new Text(key_str + "," + arr[1]), null);
			}
			
			context.write(new Text(key_str + ",pos_count:" + num_pos_in_review + ",neg_count:" + num_neg_in_review), null);
			
			
//			StringBuilder sbb = new StringBuilder(key_str);
//			sbb.append(",").append("pos_count:").append(num_pos_in_review).append(",").append("neg_count:")
//					.append(num_neg_in_review).append(sb);

//			context.write(new Text(sbb.toString()), null);
		}

	}
}
