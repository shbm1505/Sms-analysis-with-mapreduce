import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends
      Reducer<Text, IntWritable, Text, IntWritable>
{
	private IntWritable result = new IntWritable();
      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException
      {
    	  
    	  
            int sum = 0;
            /*iterates through all the values available with a key and add them together and give the
            final result as the key and sum of its values*/
            for (IntWritable val : values) {
            	 sum += val.get();
            	}
            	result.set(sum);
            	try {
					context.write(key, result);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            	}
            	}






