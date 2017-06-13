# assignment12.1

Counter Driver code:

package com.counter;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class Counter extends Configured implements Tool {
@Override
public int run(String[] args) throws Exception {
String input, output;
if(args.length == 2) {
 input = args[0];
 output = args[1];
} 
private static string getInput(Scanner in)
    {
Scanner in = new Scanner(System.in);
System.out.println("Enter a string here: ");
input = in.nextLine();
  if(input.length() > 0)
        {
            getInput(input);    //Get jobid
        else
        {
            System.out.println("Enter a string here: ");
            input = in.nextLine();
        }

        return input;
}
JobConf conf = new JobConf(getConf(), ComplaintCounter.class);
conf.setJobName(this.getClass().getName());
FileInputFormat.setInputPaths(conf, new Path(input));
FileOutputFormat.setOutputPath(conf, new Path(output));
conf.setMapperClass(CounterMapper.class);
conf.setMapOutputKeyClass(Text.class);
conf.setMapOutputValueClass(IntWritable.class);
conf.setOutputKeyClass(Text.class);
conf.setOutputValueClass(IntWritable.class);
conf.setNumReduceTasks(0);
RunningJob job = JobClient.runJob(conf);
return 0;
}
public static void main(String[] args) throws Exception {
 int exitCode = ToolRunner.run(new Counter(), args);
 System.exit(exitCode);
}
}


Mapper code:

package com.evoke.bigdata.mr.complaint;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
 
public class ComplaintCounterMapper extends MapReduceBase implements
 Mapper<LongWritable, Text, Text, IntWritable> {
public void map(LongWritable key, Text value,
OutputCollector<Text, IntWritable> output, Reporter reporter)
throws IOException {
String[] fields = value.toString().split(",");
if (fields.length > 1) {
 String jobid = fields[1].toLowerCase();
 }
 output.collect(new Text(jobid), new IntWritable(1));
 }
 }
}
