package tp1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;

public class WordCount {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        System.out.println("Hello world!");
        Configuration conf = new Configuration();
        String[] ourArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf,"wordCount job");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenCounterMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath( job , new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        if (job.waitForCompletion(true))
            System.exit(0);
        System.exit(-1);


    }
}
