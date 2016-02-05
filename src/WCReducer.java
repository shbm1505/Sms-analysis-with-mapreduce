import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends
      Reducer<Text, Text, Text, Text> {
	
	private IntWritable result = new IntWritable();
      public void reduce(Text key, Text values, Context context) throws IOException
      {
    	  Text key2=new Text();
    	  Text t2=new Text();
    	  key2=key;
    	  t2=values;
    	  try {
			context.write(key2, t2);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
          
      }
}