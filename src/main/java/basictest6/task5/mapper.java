package basictest6.task5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String http = "HTTP/1.0\"";
        int i = line.lastIndexOf(http);
        int j = line.lastIndexOf(" ");
        String str = line.substring(i, j).trim();
        String flag = "404";
        if (str.equals(flag)) {
            context.write(value, NullWritable.get());
        }
    }
}
