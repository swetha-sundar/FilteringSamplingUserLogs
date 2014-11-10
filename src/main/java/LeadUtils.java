import org.apache.avro.AvroTypeException;
import sessions.*;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 10/28/14
 * Time: 12:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class LeadUtils {

//A series of helper functions that finds what enum value should be stored in the session/lead object
    protected static final String[] fields = {
        "userid", "apikey", "lead_id",
        "type", "bidtype", "advertiser", "campaign_id",
        "recordid", "lead_amount", "revenue", "test", "ab", "customer_zip","zip"};

    public static LeadType findLeadType(String value) {
        for (LeadType type : LeadType.values()) {
            if (value.equalsIgnoreCase(type.toString())) {
                return type;
            }
        }
        return LeadType.BAD;
    }

    public static BidType findBidType(String value) {
        for (BidType bidType : BidType.values()) {
            if (value.equalsIgnoreCase(bidType.toString())) {
                return bidType;
            }
        }
        return BidType.OTHER;
    }

    public static String[] getType2Fields() {
        return fields;
    }

    //a helper function used to set the value to the corresponding field in the session & lead objects
    public static void setField(Session.Builder builder, Lead.Builder leadBuilder,
                                int index, Map<String, String> value) {
        switch (index) {
            case 0:
                builder.setUserId(value.get(fields[0]));
                break;
            case 1:
                builder.setApiKey(value.get(fields[1]));
                break;
            case 2:
                try {
                    leadBuilder.setLeadId(Long.parseLong(value.get(fields[2])));
                } catch (NumberFormatException e) {
                    leadBuilder.setLeadId(0L);
                }
                break;
            case 3:
                try {
                    leadBuilder.setType(LeadUtils.findLeadType(value.get(fields[3])));
                } catch (AvroTypeException e) {
                    leadBuilder.setType(LeadType.BAD);
                }
                break;
            case 4:
                leadBuilder.setBidType(LeadUtils.findBidType(value.get(fields[4])));
                break;
            case 5:
                leadBuilder.setAdvertiser(value.get(fields[5]));
                break;
            case 6:
                leadBuilder.setCampaignId(value.get(fields[6]));
                break;
            case 7:
                try {
                    leadBuilder.setId(Long.parseLong(value.get(fields[7])));
                } catch (NumberFormatException e) {
                    leadBuilder.setId(0L);
                }
                break;
            case 8:
                try {
                    leadBuilder.setAmount(Float.parseFloat(value.get(fields[8])));
                } catch (NumberFormatException e) {
                    leadBuilder.setAmount(0);
                }
                break;
            case 9:
                try {
                    leadBuilder.setRevenue(Float.parseFloat(value.get(fields[9])));
                } catch (NumberFormatException e) {
                    leadBuilder.setRevenue(0);
                }
                break;
            case 10:
                if (value.containsKey(fields[10])) {
                    leadBuilder.setTest(Boolean.parseBoolean(value.get(fields[10])));
                }
                break;
            case 11:
                leadBuilder.setAb(value.get(fields[11]));
                break;
            case 12:
                leadBuilder.setCustomerZip(fields[12]);
                break;
            case 13:
                leadBuilder.setVehicleZip(fields[13]);
                break;
        }
    }

    //Find the vdp index
    public static int findTheVdpIndex(Lead lead, List<Impression> impressions) {
        int index = -1;
        Long id = lead.getId();
        for (int i = 0; i < impressions.size(); i++) {
            Impression imp = impressions.get(i);
            List<Long> ids = imp.getId();
            if (ids.contains(id) && imp.getImpressionType().equals(ImpressionType.VDP)) {
                index = i;
            }
        }
        return index;
    }
}

