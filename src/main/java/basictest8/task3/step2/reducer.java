package basictest8.task3.step2;

import basictest8.task3.ValueBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

class reducer extends Reducer<Text, ValueBean, Text, NullWritable> {


    @Override
    protected void reduce(Text key, Iterable<ValueBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> list = new ArrayList<>();
        String first = "";
        String second = "";
        for (ValueBean i : values) {
            if (i.getFlag().equals("second")) {
                second = i.toString();
            } else {
                first = i.toString();
                list.add(first);
            }
        }
        for (String j : list) {
            String[] split = first.split(",");
            String split1 = split[0];
            String split2 = split[1] + "," + split[2] + "," + split[3] + "," + split[4] + "," + split[5];
            String str = split1 + "," + second + "," + split2;
            context.write(new Text(str), NullWritable.get());
        }
    }
}
