package wordOccurrence;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;

public class wordOccaurrenceJob extends Configured implements Tool {

    public int run(final String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = Job.getInstance(conf, "computing");
        job.setJarByClass((Class)wordOccaurrenceJob.class);
        job.setMapperClass((Class) wordReducer.class);
        job.setReducerClass((Class) wordMapper.class);
        job.setMapOutputKeyClass((Class)Text.class);
        job.setMapOutputValueClass((Class)IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        final boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        if (args.length != 2) {
            System.exit(-1);
        }
        final wordOccaurrenceJob wordJob = new wordOccaurrenceJob();
        System.exit(ToolRunner.run((Tool)wordJob, args));
    }
}
