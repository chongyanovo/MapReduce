package basictest3.task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
任务二、甲醛的浓度超过2.0%表示该房间甲醛超标，使用mapreduce对规范化的数据进行处理，获得超标房间对应的户主信息，形成如下效果：
省份     城市    城区      小区名称   户主姓名    设备id   设备类型   房屋1采集值  房屋2采集值  房屋3采集值     超标告警
江苏省   南京    栖霞区    阳光小区    张三        0102     甲醛       1.4%             1.5%          1.7%          未超标
江苏省   南京    栖霞区    阳光小区    李四        0112     甲醛       2.5%             0.5%          1.4%          已超标
浙江省   杭州    上城区     罗马小区    张大彪      0201     甲醛       2.4%             3.5%          5.7%         已超标
江苏省   杭州    滨江区     湖畔小区    李艳        0302     甲醛       2.2%             2.5%          2.4%
 */
public class mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        String type = lines[6];
        String flag = "甲醛";
        Float value1 = Float.valueOf(lines[7].substring(0, lines[7].length()));
        Float value2 = Float.valueOf(lines[8].substring(0, lines[8].length()));
        Float value3 = Float.valueOf(lines[9].substring(0, lines[9].length()));
        String isFlag = "";
        if (type.equals(flag)) {
            if (value1 > 2.0) {
                isFlag = "已超标";
            } else {
                isFlag = "未超标";
            }
            if (value2 > 2.0) {
                isFlag = "已超标";
            } else {
                isFlag = "未超标";
            }
            if (value3 > 2.0) {
                isFlag = "已超标";
            } else {
                isFlag = "未超标";
            }
            String str = "";
            for (int i = 0; i < lines.length - 1; i++) {
                str = str + "," + lines[i];
                if (i == lines.length - 1) {
                    str = str + "," + isFlag;
                }
            }
            str = str.substring(1);
            context.write(new Text(str), NullWritable.get());
        }
    }
}
