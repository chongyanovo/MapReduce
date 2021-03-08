package basictest4.task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Text, Bean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        Bean dataBean33 = new Bean();
        dataBean33.setSex(line[2]);
        dataBean33.setName(line[3]);
        context.write(new Text(line[0]), dataBean33);
    }
}
