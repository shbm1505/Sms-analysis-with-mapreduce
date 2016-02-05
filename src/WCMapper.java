import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, Text>
{
      //hadoop supported data types
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
     
      //map method that performs the tokenizer job and framing the initial key value pairs
      //after all lines are converted into key-value pairs, reducer is called.
      public void map(LongWritable key, Text value, Context context) throws IOException
      {
            //taking one line at a time from input file and tokenizing the same
    	  String[] tokens = value.toString().split("|");
    	  String x=tokens[11];
            String line = value.toString();
     //       StringTokenizer tokenizer = new StringTokenizer(line);
  		try {
			context.write(new Text("hey"), new Text(x));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
            //iterating through all the words available in that line and forming the key value pair
            
       }
}