package basictest2.task3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

class reducer extends Reducer<Text, Bean, Text, NullWritable> {
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    int Num = 0;

    @Override
    protected void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
        Num++;
        int clazzNum = 0;
        HashSet set = new HashSet();
        for (Bean i : values) {
            if (!set.contains(i.getClazz())) {
                clazzNum++;
                set.add(i.getClazz());
            }
        }
        map.put(key.toString(), clazzNum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String str = "人工智能学院有 " + String.valueOf(Num) + " 个专业";
        context.write(new Text(str), NullWritable.get());
        for (String i : map.keySet()) {
            String str1 = i + "专业有 " + String.valueOf(map.get(i)) + " 个班";
            context.write(new Text(str1), NullWritable.get());
        }
    }
}
