package basictest4.task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Text, Bean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        Bean dataBean22 = new Bean();
        dataBean22.setArea(line[0]);
        dataBean22.setName(line[3]);
        dataBean22.setAge(Integer.parseInt(line[1]));
        dataBean22.setSex(line[2]);
        context.write(new Text(line[2]), dataBean22);
    }
}
