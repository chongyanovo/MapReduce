package UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class FormatUrL extends UDF {
    public static String evaluate(String url) {
        String[] split = url.split(" ");
        return split[0] + ":" + split[1] + "+" + split[2];
    }

//    public static void main(String[] args) {
//        System.out.println(evaluate("get /service/2.htm HTTP/ 1.0"));
//    }
}
