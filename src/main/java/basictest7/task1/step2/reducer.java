package basictest7.task1.step2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class reducer extends Reducer<Text, ValueBean, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<ValueBean> values, Context context) throws IOException, InterruptedException {
        String str = "";
        String first = "";
        String second = "";
        for (ValueBean i : values) {
            if (i.getFlag() == 0) {
                first = i.toString();
            }
            if (i.getFlag() == 1) {
                second = i.toString();
            }
        }
        str = key.toString() + "\t" + first + "\t" + second;
        context.write(new Text(str), NullWritable.get());
    }
}
