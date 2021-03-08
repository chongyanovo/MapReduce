package UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class formatDate extends UDF {
    public static String evaluate(String data) {
        return data.substring(1, data.length() - 1).replaceAll("-", "/");
    }

//    public static void main(String[] args) {
//        String test = "[2021-02-04 09:17:23]";
//        String str = evaluate(test);
//        System.out.println(str);
//    }
}
