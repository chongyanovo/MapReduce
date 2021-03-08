package basictest6.task3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String post = "POST";
        String get = "GET";
        String http = "HTTP/1.0";
        Bean bean = new Bean();
        int i;
        int j = line.indexOf(http);
        if (line.contains(post)) {
            i = line.lastIndexOf(post);
        } else {
            i = line.lastIndexOf(get);
        }
        String url = line.substring(i, j).trim();
        int k = line.lastIndexOf(" ");
        Double time = Double.valueOf(line.substring(k));
        context.write(new Text(url), new DoubleWritable(time));
    }
}
