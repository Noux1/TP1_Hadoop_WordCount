package tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer extends Reducer<Text , IntWritable , Text , Text> {

    public void reduce(Text key , Iterable<IntWritable> values , Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values){
            sum = sum + value.get();
        }
        context.write(key ,new Text(sum+"occurrences "));

    }

}
