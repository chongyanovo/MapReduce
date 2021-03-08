package UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class getResponseTime extends UDF {
    public static String evaluate(String time) {
        if (time.contains("ms")) {
            String str = time.substring(0, time.length() - 2);
            return "" + Double.parseDouble(str) + "ms";
        } else {
            Double i;
            if (time.contains("s")) {
                i = Double.parseDouble(time.substring(0, time.length() - 1));
            } else {
                i = Double.parseDouble(time);
            }
            return "" + i * 1000 + "ms";
        }
    }

//    public static void main(String[] args) {
//        System.out.println(evaluate("0.1"));
//        System.out.println(evaluate("3s"));
//        System.out.println(evaluate("30ms"));
//    }
}
