package wordOccurrence;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class wordReducer extends Reducer<Text, IntWritable, Text, Text> {

    public void reduce(final Text key, final Iterable<IntWritable> values, final Reducer.Context context) throws IOException, InterruptedException {
        int count = 0;
        for (final IntWritable i : values) {
            ++count;
            context.write((Object)key, (Object)new Text(count + " occurrences"));
        }
    }
}
