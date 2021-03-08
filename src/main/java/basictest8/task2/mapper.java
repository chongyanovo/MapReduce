package basictest8.task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

class mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    int count = 0;
    ArrayList<String> list = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("计数器", "删除的非法值数量:");
        String[] line = value.toString().trim().split(",");
        String comment = line[2];
        if (comment.contains("级")) {
            comment = comment.replace("级", "");
        }
        int i = Integer.parseInt(comment);
        if (i > 0 && i < 6) {
            comment += "级";
            String str = line[0] + "," + line[1] + "," + comment + "," + line[3] + "," + line[4];
            list.add(str);
        } else {
            count++;
            counter.increment(1L);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //context.write(new Text("删除了" + count + "个非法值"), NullWritable.get());
        for (String i : list) {
            context.write(new Text(i), NullWritable.get());
        }
    }
}
