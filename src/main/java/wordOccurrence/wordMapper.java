package wordOccurrence;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class wordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable ONE;

    protected void map(final Object key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
        final StringTokenizer tok = new StringTokenizer(value.toString(), " ");
        while (tok.hasMoreTokens()) {
            final Text word = new Text(tok.nextToken());
            context.write((Object)word, (Object) wordMapper.ONE);
        }
    }

    static {
        ONE = new IntWritable(1);
    }
}
