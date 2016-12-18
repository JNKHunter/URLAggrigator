import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jhunter on 12/17/16.
 */
public class UrlMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, one);
    }
}
