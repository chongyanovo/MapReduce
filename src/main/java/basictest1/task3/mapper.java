package basictest1.task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Bean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        Bean bean = new Bean();
        bean.setHot(Integer.parseInt(line[2]));
        bean.setLocation(line[3]);
        context.write(bean, new Text(line[1]));
    }
}
