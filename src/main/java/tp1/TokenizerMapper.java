package tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TokenizerMapper extends Mapper<Object , Text , Text , IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for(String word : words){
            Text text = new Text(word);
            context.write(text, one);
        }
    }
}
