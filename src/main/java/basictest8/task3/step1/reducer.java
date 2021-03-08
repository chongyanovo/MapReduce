package basictest8.task3.step1;

import basictest8.task3.ValueBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class reducer extends Reducer<Text, ValueBean, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<ValueBean> values, Context context) throws IOException, InterruptedException {
        String first = "";
        String second = "";
        for (ValueBean i : values) {
            if (i.getFlag().equals("first")) {
                first = i.toString();
            } else {
                second = i.toString();
            }
        }
        String[] split = first.split(",");
        String split1 = split[0];
        String split2 = split[1] + "," + split[2];
        String str = split1 + "," + second + "," + split2;
        context.write(new Text(str), NullWritable.get());
    }
}
