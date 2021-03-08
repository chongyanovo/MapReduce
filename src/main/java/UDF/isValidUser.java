package UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class isValidUser extends UDF {
    public static int evaluate(String id) {
//        if (id.contains("@") && id.contains(".com")) {
//            return 1;
//        } else {
//            return 0;
//        }

        String check = "^([a-z0-9A-Z]+[-|\\.]?)+[a-z0-9A-Z]@([a-z0-9A-Z]+(-[a-z0-9A-Z]+)?\\.)+[a-zA-Z]{2,}$";
        Pattern regex = Pattern.compile(check);
        Matcher matcher = regex.matcher(id);
        boolean isMatched = matcher.matches();
        //System.out.println(isMatched);

        return isMatched ? 1 : 0;
    }

//    public static void main(String[] args) {
//        String str = "node@sina.edu";
//        String str1 = "chahu";
//
//        System.out.println(evaluate(str1));
//    }
}
