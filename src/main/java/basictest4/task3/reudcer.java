package basictest4.task3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

class reudcer extends Reducer<Text, Bean, Text, NullWritable> {
    ArrayList<String> list = new ArrayList<String>();

    @Override
    protected void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
        int countMan = 0;
        int countWoman = 0;
        for (Bean i : values) {
            String sex = i.getSex();
            if (sex.equals("男")) {
                countMan++;
            } else {
                countWoman++;
            }
        }
        list.add(key.toString() + "\t" + "男" + "\t" + String.valueOf(countMan));
        list.add(key.toString() + "\t" + "女" + "\t" + String.valueOf(countWoman));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String i : list) {
            context.write(new Text(i), NullWritable.get());
        }
    }
}
