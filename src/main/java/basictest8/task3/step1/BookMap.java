package basictest8.task3.step1;

import basictest8.task3.ValueBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class BookMap extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        String str = line[1] + "," + line[2] + "," + line[3];
        ValueBean valueBean = new ValueBean();
        valueBean.setValue(str);
        valueBean.setFlag("first");
        context.write(new Text(line[0]), valueBean);
    }
}
