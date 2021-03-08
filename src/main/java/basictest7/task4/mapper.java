package basictest7.task4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Bean, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split("\t");
        String year = line[5].trim().substring(0, 4);
        Bean bean = new Bean();
        bean.setCompare(line[7]);
        bean.setYear(year);
        context.write(bean, new IntWritable(1));
    }
}
