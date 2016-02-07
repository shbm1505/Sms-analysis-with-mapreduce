
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCDriver {

   public static void main(String[] args) throws Exception {
    //  Configuration conf = new Configuration();
	   
	  Configuration config = HBaseConfiguration.create();
      Job job = Job.getInstance(config, "WordCounter");
      job.setJarByClass(WCDriver.class);
      job.setMapperClass(WCMapper.class);
      job.setReducerClass(WCReducer.class);
      
      Scan scan = new Scan();
      scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
      scan.setCacheBlocks(false);

      // TODO: specify output types
  //    job.setOutputKeyClass(Text.class);
  //    job.setOutputValueClass(Text.class);

      // TODO: specify input and output DIRECTORIES (not files)
      FileInputFormat.setInputPaths(job, new Path(args[0]));
   //   FileOutputFormat.setOutputPath(job, new Path(args[1]));
      TableMapReduceUtil.initTableReducerJob(
    		   smslogs,
    		   WCReducer.class,
    		   job);
      

      if (!job.waitForCompletion(true))
         return;
   }
}




