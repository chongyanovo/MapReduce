package basictest3.task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
1）采集时间格式问题：2020-2-25 7:44:39应该为2020-02-25 07:44:39。
2）城区格式问题：上城应该为上城区，滨江应该为滨江区。
3）温度和甲醛采集值问题：20应该为20%，3.5应该为3.5%。
4）设备异常情况：如张大彪家温度采集器对房屋2的采集值为none，表示该温度采集器工作异常，则该条纪录应该删除。
 */
public class mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Counter counter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        counter = context.getCounter("计数器", "设备异常情况:");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        String area = lines[2];
        String value1 = lines[7];
        String value2 = lines[8];
        String value3 = lines[9];
        String flag = "none";
        String str = "";
        if (!(value1.equals(flag) || value2.equals(flag) || value3.equals(flag))) {
            if (!area.contains("区")) {
                lines[2] = area + "区";
            }
            if (!value1.equals("%")) {
                lines[7] = value1 + "%";
            }
            if (!value2.equals("%")) {
                lines[8] = value2 + "%";
            }
            if (!value3.equals("%")) {
                lines[9] = value3 + "%";
            }

            for (String i : lines) {
                str = str + "," + i;
            }
            context.write(new Text(str.substring(1)), NullWritable.get());
        } else {
            counter.increment(1L);
        }
    }
}
