import sessions.Impression;

import java.util.Comparator;

/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 10/13/14
 * Time: 12:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class ListComparatorUtil implements Comparator<Impression> {

    @Override
    public int compare(Impression imp1, Impression imp2) {
        //Sorting by the increasing timestamp
        if (imp1.getTimestamp() < imp2.getTimestamp()) {
            return -1;
        }
        else if (imp1.getTimestamp() > imp2.getTimestamp()) {
            return 1;
        }
        else {
            return 0;
        }
    }
}
