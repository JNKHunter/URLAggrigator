import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jhunter on 12/17/16.
 */
public class UrlMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);


    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        context.write(new Text(key), one);
    }
}
