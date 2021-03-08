package basictest4.task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Bean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        Bean bean1 = new Bean();
        bean1.setArea(line[0]);
        bean1.setName(line[3]);
        bean1.setAge(Integer.parseInt(line[1]));
        bean1.setSex(line[2]);
        context.write(bean1, NullWritable.get());
    }
}
