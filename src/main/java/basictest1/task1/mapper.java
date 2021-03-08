package basictest1.task1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("Map", "In");
        counter.increment(1L);
        String[] line = value.toString().split("\t");
        context.write(new Text(line[0]), new DoubleWritable(Double.parseDouble(line[2])));
    }
}
