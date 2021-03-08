package basictest6.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Bean, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String post = "POST";
        String get = "GET";
        String http = "HTTP/1.0";
        Bean key1 = new Bean();
        int i;
        int j = line.indexOf(http);
        if (line.contains(post)) {
            i = line.lastIndexOf(post);
            key1.setType(post);
        } else {
            i = line.lastIndexOf(get);
            key1.setType(get);
        }
        String url = line.substring(i, j).trim();
        key1.setUrl(url);
        context.write(key1, new IntWritable(1));
    }
}
