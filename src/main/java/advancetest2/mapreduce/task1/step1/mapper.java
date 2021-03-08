package advancetest2.mapreduce.task1.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

//1）将“缺货”改为0，将小数点的数据按四舍五入的方式改为整数，比如“15.48”改为15。
class mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private String splitter = "";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        splitter = ",";
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(splitter);
        String flag = "缺货";
        HashMap<Integer, String> map = new HashMap<>();
        map.put(3, lines[3]);
        map.put(5, lines[5]);
        for (Integer i : map.keySet()) {
            if (lines[i].equals(flag)) {
                map.put(i, "0");
            } else {
                float j = Float.parseFloat(map.get(i));
                map.put(i, "" + Math.round(j));
            }
        }
        for (Integer j : map.keySet()) {
            lines[j] = map.get(j);
        }
        String str = "";
        for (String k : lines) {
            str = str + "," + k;
        }
        str = str.substring(1);
        context.write(new Text(str), NullWritable.get());
    }
}
