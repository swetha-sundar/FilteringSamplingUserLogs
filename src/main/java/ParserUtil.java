import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 10/10/14
 * Time: 9:39 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * ParserUtil class : Takes in a log entry and parses every parameter pair
 * Stores the field and value in a hashmap
 *
 */
public class ParserUtil {

    private static Map<String, String> parameterValueMap;
    private static final String EMPTY_STRING = "";

    public static Map<String, String> parse(String line) {
        parameterValueMap = new HashMap<String, String>();
        String[] fields = line.split("\\|\t\\|");
        for (String field : fields) {
            String[] pair = field.split(":", 2);

            if (pair.length == 2) {
                parameterValueMap.put(pair[0],pair[1]);
            }
            else if (pair.length == 1) {
                parameterValueMap.put(pair[0],EMPTY_STRING);
            }
        }
        return parameterValueMap;
    }


}
