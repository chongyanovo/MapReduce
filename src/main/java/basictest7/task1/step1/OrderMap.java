package basictest7.task1.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class OrderMap extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        String str = line[0];
        ValueBean valueBean = new ValueBean();
        valueBean.setValue(line[1] + "\t" + line[2] + "\t" + line[3] + "\t" + line[4] + "\t" + line[5]);
        valueBean.setFlag(1);
        context.write(new Text(str), valueBean);
    }
}
