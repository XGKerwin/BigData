import java.util.HashMap;

/**
 * @author guanxin
 * @Date 2022-12-18 14:29
 * @Email aiykerwin@sina.com
 */
public class Test {


    public static void main(String[] args) {
        HashMap<String, Integer> stringIntegerHashMap = new HashMap<>();
        Integer put = stringIntegerHashMap.put("1", 0);
        Integer put1 = stringIntegerHashMap.put("1", 0);
        Integer put2 = stringIntegerHashMap.put("1", 0);
        Integer put3 = stringIntegerHashMap.put("2", 1);
        System.out.println(put);
        System.out.println(put1);
        System.out.println(put2);
        System.out.println(put3);

    }
}
