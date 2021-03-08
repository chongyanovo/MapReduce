package basictest8.task3.step2;

import basictest8.task3.ValueBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        int i = line.length;
        if (line.length == 7) {
            String str = line[0] + "," + line[2] + "," + line[3] + "," + line[4] + "," + line[5] + "," + line[6];
            ValueBean valueBean = new ValueBean();
            valueBean.setValue(str);
            valueBean.setFlag("first");
            context.write(new Text(line[1]), valueBean);
        }
    }
}
