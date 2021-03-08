package basictest7.task1.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class Map extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split("\t");
        String str = line[0] + "\t" + line[1] + "\t" + line[2];
        ValueBean valueBean = new ValueBean();
        valueBean.setValue(line[3] + "\t" + line[4] + "\t" + line[5] +
                "\t" + line[6] + "\t" + line[7] + "\t" + line[8] + "\t" + line[9]);
        valueBean.setFlag(0);
        context.write(new Text(str), valueBean);
    }
}
