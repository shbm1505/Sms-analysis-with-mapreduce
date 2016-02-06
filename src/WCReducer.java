import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class WCReducer extends
      Reducer<Text, Text, Text, Text> {
	
	private IntWritable result = new IntWritable();
      public void reduce(Text key, Text values, Context context) throws IOException
      {
    	  Text smskey=new Text();
    	  Text smsinfo=new Text();
    	  smskey=key;
    	  smsinfo=values;
    	  String arr[] = smsinfo.toString().split("\t");
    	  
    	  String pn=arr[0];
    	  String t=arr[1];
    	  String attrib=arr[2];
    	  String va=arr[3];
    	  String pr=arr[4];
    	  
    	  Configuration config = HBaseConfiguration.create();

          
          HTable hTable = new HTable(config, "smslogs");

                    
          System.out.println("data inserted");
          Put put = new Put(toBytes(smskey));
          put.add(toBytes("phoneno"), toBytes(pn));
          put.add(toBytes("time"), toBytes(t));
          put.add(toBytes("attribute"), toBytes(attrib));
          put.add(toBytes("value"), toBytes(val));
          context.write(null, put);
          
          hTable.put(p);
          // closing HTable
          hTable.close();
    	  
    	  try {
			context.write(null,put);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
          
      }
}