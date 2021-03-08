package basictest2.task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Text, Bean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        Bean bean3 = new Bean();
        bean3.setClazz(line[2]);
        bean3.setNum(Integer.parseInt(line[4]));
        context.write(new Text(line[1]), bean3);
    }
}
