import sessions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 10/28/14
 * Time: 12:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class ImpressionUtils {

    //Fields in the Impressions
    protected static final String[] fields = {
            "uid", "apikey", "uagent", "res", "activex",
            "type", "action", "action_name", "id",
            "timestamp", "ab", "vertical", "start_index", 
            "total", "domain", "lat", "lon", "address",
            "city", "zip", "state", "phone_type", "listingzip"};

    //A series of helper functions that finds what enum value should be stored in the session/impression object
    public static ActiveX findActiveXValue(String value) {
        for (ActiveX activeX : ActiveX.values()) {
            if (value.equalsIgnoreCase(activeX.toString())) {
                return activeX;
            }
        }
        return ActiveX.NOT_SUPPORTED;
    }

    public static ImpressionType findImpressionType(String value) {
        String[] vdpValues = {"phone", "email", "landing"};
        String THANK_YOU = "thankyou";
        if (Arrays.asList(vdpValues).contains(value.toLowerCase())) {
            return ImpressionType.VDP;
        }
        else if (value.equalsIgnoreCase(ImpressionType.ACTION.toString())) {
            return ImpressionType.ACTION;
        }
        else if (value.equalsIgnoreCase(THANK_YOU)) {
            return ImpressionType.THANK_YOU;
        }
        return ImpressionType.SRP;
    }

    public static Action findActionType(String value) {
        for (Action action : Action.values()) {
            if (value.equalsIgnoreCase(action.toString())) {
                return action;
            }
        }
        return Action.PAGE_VIEW;
    }

    public static ActionName findActionName(String value) {
        if (value.isEmpty()) {
            return ActionName.NONE;
        }
        else {
            for (ActionName aname : ActionName.values()) {
                if (value.equalsIgnoreCase(aname.toString())) {
                    return aname;
                }
            }
        }
        return ActionName.UNKNOWN;
    }

    public static List<Long> stringToLong(String[] values) {
        List<Long> ids = new ArrayList<Long>();
        for (String val : values) {
            ids.add(Long.parseLong(val));
        }
        return ids;
    }

    public static Vertical findVertical(String value) {
        for (Vertical vertical : Vertical.values()) {
            if (value.equalsIgnoreCase(vertical.toString())) {
                return vertical;
            }
        }
        return Vertical.OTHER;
    }

    public static PhoneType findPhoneType(String value) {
        for (PhoneType phoneType : PhoneType.values()) {
            if (value.equalsIgnoreCase(phoneType.toString())) {
                return phoneType;
            }
        }
        return PhoneType.NONE;
    }

    public static String[] getFields() {
        return fields;
    }

    //a helper function used to set the value to the corresponding field in the session & impression objects
    public static void setField(Session.Builder builder, Impression.Builder impBuilder,
                                int index, Map<String,String> values) {

        switch (index) {
            case 0:
                if (values.containsKey(fields[0]))
                    builder.setUserId(values.get(fields[0]));
                break;
            case 1:
                if (values.containsKey(fields[1]))
                    builder.setApiKey(values.get(fields[1]));
                break;
            case 2:
                if (values.containsKey(fields[2]))
                    builder.setUserAgent(values.get(fields[2]));
                break;
            case 3:
                if (values.containsKey(fields[3]))
                    builder.setResolution(values.get(fields[3]));
                break;
            case 4:
                if (values.containsKey(fields[4]))
                    builder.setActivex(findActiveXValue(values.get(fields[4])));
                break;
            case 5:
                if (values.containsKey(fields[5]))
                    impBuilder.setImpressionType(findImpressionType(values.get(fields[5])));
                break;
            case 6:
                if (values.containsKey(fields[6]))
                    impBuilder.setAction(findActionType(values.get(fields[6])));
                else
                    impBuilder.setAction(Action.PAGE_VIEW);
                break;
            case 7:
                if (values.containsKey(fields[7]))
                    impBuilder.setActionName(findActionName(values.get(fields[7])));
                else
                    impBuilder.setActionName(ActionName.NONE);
                break;
            case 8:
                //Multiple ids are split and stored in a list
                try {
                    String[] ids = values.get(fields[8]).split(",");
                    if (ids.length != 0) {
                        impBuilder.setId(stringToLong(ids));
                    }
                    else {
                        //If the list if null (if there are no ids, then a new null list is created and set
                        List<Long> nullIds = new ArrayList<Long>();
                        impBuilder.setId(nullIds);
                    }
                } catch (NullPointerException e) {
                    List<Long> nullIds = new ArrayList<Long>();
                    impBuilder.setId(nullIds);
                }
                break;
            case 9:
                if (values.containsKey(fields[9]))
                    impBuilder.setTimestamp(Long.parseLong(values.get(fields[9])));
                break;
            case 10:
                if (values.containsKey(fields[10]))
                    impBuilder.setAb(values.get(fields[10]));
                break;
            case 11:
                if (values.containsKey(fields[11]))
                    impBuilder.setVertical(findVertical(values.get(fields[11])));
                else
                    impBuilder.setVertical(Vertical.CARS);
                break;
            case 12:
                try {
                    impBuilder.setStartIndex(Integer.parseInt(values.get(fields[12])));
                } catch (NumberFormatException e) {
                    impBuilder.setStartIndex(-1);
                }
                break;
            case 13:
                try {
                    impBuilder.setTotal(Integer.parseInt(values.get(fields[13])));
                } catch (NumberFormatException e) {
                    impBuilder.setTotal(-1);
                }
                break;
            case 14:
                if (values.containsKey(fields[14]))
                    impBuilder.setDomain(values.get(fields[14]));
                break;
            case 15:
                if (values.containsKey(fields[15])) {
                    try {
                        //If the field contains a non-numeric value, it is caught in the exception and
                        //a default 0.0 value is set to latitude
                        impBuilder.setLat(Double.parseDouble(values.get(fields[15])));
                    } catch (NumberFormatException e) {
                        System.err.println("Non-numeric value set for latitude:");
                        impBuilder.setLat(0.0);
                    }
                }
                break;
            case 16:
                if (values.containsKey(fields[16])) {
                    try{
                        //If the field contains a non-numeric value, it is caught in the exception and
                        //a default 0.0 value is set to longitude
                        impBuilder.setLon(Double.parseDouble(values.get(fields[16])));
                    }catch (NumberFormatException e) {
                        System.err.println("Non-numeric value set for longitude:");
                        impBuilder.setLon(0.0);
                    }
                }
                break;
            case 17:
                if (values.containsKey(fields[17]))
                    impBuilder.setAddress(values.get(fields[17]));
                break;
            case 18:
                if (values.containsKey(fields[18]))
                    impBuilder.setCity(values.get(fields[18]));
                break;
            case 19:
                String zip = "";
                if (values.containsKey(fields[19])) {
                    zip = values.get(fields[19]);
                    impBuilder.setZip(zip);
                }
                break;
            case 20:
                if (values.containsKey(fields[20]))
                    impBuilder.setState(values.get(fields[20]));
                break;
            case 21:
                if (values.containsKey(fields[21]))
                    impBuilder.setPhoneType(findPhoneType(values.get(fields[21])));
                else
                    impBuilder.setPhoneType(PhoneType.NONE);
                break;
            case 22:
                String listingzip = "";
                if (values.containsKey(fields[22])) {
                    listingzip = values.get(fields[22]);
                    impBuilder.setZip(listingzip);
                }
                break;
        }
    }
}
