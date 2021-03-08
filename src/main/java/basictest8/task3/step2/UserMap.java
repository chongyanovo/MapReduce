package basictest8.task3.step2;

import basictest8.task3.ValueBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class UserMap extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        ValueBean valueBean = new ValueBean();
        valueBean.setValue(line[1]);
        valueBean.setFlag("second");
        context.write(new Text(line[0]), valueBean);
    }
}
