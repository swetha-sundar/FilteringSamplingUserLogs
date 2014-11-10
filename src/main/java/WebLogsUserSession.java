import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import sessions.*;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 10/10/14
 * Time: 9:40 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 *  WebLogUserSession : generates a user session from a list of impressions and leads (log entries).
 *  Joins the two different log entries into one session
 *  Performs Replicated join : Associates every zip field with its corresponding DMA
 *  Writes out multiple output files depending upon the user's behavior in the site
 */
public class WebLogsUserSession extends Configured implements Tool {

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */

    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: WebLogsUserSession <input path1> <input path2> <output path> <percentage>");
            return -1;
        }

        //Configuration object that has all the configuration details
        Configuration conf = getConf();

        Job job = new Job(conf, "WebLogsUserSession");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WebLogsUserSession.class);

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the input format
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        //Specify the Map class, the mapper's output key & value
        job.setMapperClass(ImpressionMapClass.class);
        job.setMapperClass(LeadMapClass.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);

        //Specify the output key and value class type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Grab the input file and output directory from the command line.
        String inputPath1 = appArgs[0];
        String inputPath2 = appArgs[1];

        //Specify the input files and associating the corresponding mapper
        MultipleInputs.addInputPath(job, new Path(inputPath1), KeyValueTextInputFormat.class, ImpressionMapClass.class);
        MultipleInputs.addInputPath(job, new Path(inputPath2), KeyValueTextInputFormat.class, LeadMapClass.class);

        //Specify the output folder
        FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

        //Specify the configuration for multiple outputs
        MultipleOutputs.addNamedOutput(job, "userType", TextOutputFormat.class, Text.class, Text.class);

        MultipleOutputs.setCountersEnabled(job,true);
        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

   /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WebLogsUserSession(), args);
        System.exit(res);
    }

    /**
     * Map class: reads a log entry and converts every impression entry into an user session object
     * Input: a log entry
     * Output: Key = user_id concatenated with the api_key
     *         Value = Avro UserSessionObject with one impression
     */

    public static class ImpressionMapClass extends Mapper<Text, Text, Text, AvroValue<Session>> {

        AvroValue<Session> userSessionObject;
        List<Impression> impressions;
        Impression.Builder impBuilder;
        Session.Builder builder;
        String[] type1Fields;

        //Setup method called once which gets the cached files and then uses a Hashmap
        //to store zip to dma mappings

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{

            String logEntry = value.toString();

            // A hashmap datastructure is used to store the field (key) and its values
            // The line is passed to the ParserUtil class which parses it and loads the map
            Map<String, String> valueMap = new HashMap<String, String>();
            valueMap = ParserUtil.parse(logEntry);

            impressions = new ArrayList<Impression>();
            impBuilder = Impression.newBuilder();
            builder = Session.newBuilder();
            type1Fields = ImpressionUtils.getFields();

            //Iterate through the list of fields and store every field's value in its corresponding
            //avro session object's member variable

            for (int i = 0; i < type1Fields.length; i++) {
                ImpressionUtils.setField(builder, impBuilder, i, valueMap);
            }

            //Build the avro session object
            impressions.add(impBuilder.build());
            builder.setImpressions(impressions);
            builder.setLeads(new ArrayList<Lead>());
            userSessionObject = new AvroValue(builder.build());

            //Generate the key by concatenating the user_id and the api_key
            String userId = userSessionObject.datum().getUserId().toString();
            String apiKey = userSessionObject.datum().getApiKey().toString();
            String finalKey = userId + ":" + apiKey;


            //emit the key and the session object
            context.write(new Text(finalKey), userSessionObject);
        }
    }

    /**
     * Map class: reads a log entry and converts every lead entry into an user session object
     * Input: a log entry
     * Output: Key = user_id concatenated with the api_key
     *         Value = Avro UserSessionObject with one lead
     */

    public static class LeadMapClass extends Mapper<Text, Text, Text, AvroValue<Session>> {

        AvroValue<Session> userSessionObject;
        List<Lead> leads;
        Lead.Builder leadBuilder;
        Session.Builder builder;
        String[] type2Fields;

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{

            String logEntry = value.toString();
            type2Fields = LeadUtils.getType2Fields();

            // A hashmap datastructure is used to store the field (key) and its values
            // The line is passed to the ParserUtil class which parses it and loads the map

            Map<String, String> valueMap = new HashMap<String, String>();
            valueMap = ParserUtil.parse(logEntry);

            leads = new ArrayList<Lead>();
            leadBuilder = Lead.newBuilder();
            builder = Session.newBuilder();

            //Iterate through the list of fields and store every field's value in its corresponding
            //avro session object's member variable

            for (int i = 0; i < type2Fields.length; i++) {
                    LeadUtils.setField(builder, leadBuilder, i, valueMap);
            }

            //Build the avro session object
            leads.add(leadBuilder.build());
            builder.setActivex(ActiveX.NOT_SUPPORTED);
            builder.setLeads(leads);
            builder.setImpressions(new ArrayList<Impression>());
            userSessionObject = new AvroValue(builder.build());

            //Generate the key by concatenating the user_id and the api_key
            String userId = userSessionObject.datum().getUserId().toString();
            String apiKey = userSessionObject.datum().getApiKey().toString();
            String finalKey = userId + ":" + apiKey;
            key = new Text(finalKey);

            //emit the key and the session object
            context.write(key, userSessionObject);

        }
    }

    /**
     * The Reduce class for WebLogUserSession generator.  Extends class Reducer, provided by Hadoop.
     * It aggregates multiple values for a single key from different mappers
     */
    public static class ReduceClass
            extends Reducer<Text, AvroValue<Session>,
            Text, Text> {

        Session.Builder builder;

        //Creating an instance of multiple outputs to write out the
        //different categories of users.
        private MultipleOutputs multipleOutputs;

        //Different categories
        private String SUBMITTER = "submitter";
        private String BOUNCER = "bouncer";
        private String BROWSER = "browser";
        private String SEARCHER = "searcher";

        @Override
        public void setup(Context context) {
            multipleOutputs = new MultipleOutputs(context);
        }

        @Override
        public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
                throws IOException, InterruptedException {

            String category = BOUNCER;
            boolean isSearcher = false;
            //Creating two lists that will hold the entire collection of impressions and leads for one session key
            List<Impression> impCollection = new ArrayList<Impression>();
            List<Lead> leadsCollection = new ArrayList<Lead>();

            //Boolean that decides if the common attributes of the session have been set or not
            boolean hasSessionDetailsBeenSet= false;

            //User Id and the Api Key
            String user_id = key.toString().split(":")[0];
            String api_key = key.toString().split(":")[1];

            builder = Session.newBuilder();

            //Iterate through the values to combine the impressions and leads into one session
            for (AvroValue<Session> value : values) {

                List<Impression> impList = new ArrayList<Impression>();
                List<Lead> leadList = new ArrayList<Lead>();

                impList = value.datum().getImpressions();
                leadList = value.datum().getLeads();

                if (!hasSessionDetailsBeenSet) {
                    hasSessionDetailsBeenSet = setSessionDetails(value);
                }

                for (Impression imp : impList) {
                    impCollection.add(Impression.newBuilder(imp).build());
                }

                for (Lead lead : leadList) {
                    leadsCollection.add(Lead.newBuilder(lead).build());
                }
            }

            //Sort the impressions by time
            Collections.sort(impCollection,new ListComparatorUtil());

            for (Impression impression : impCollection) {
                if (impression.getImpressionType().equals(ImpressionType.SRP)) {
                    isSearcher = true;
                }
                else {
                    isSearcher = false;
                    break;
                }

            }
            //build the avro session object
            builder.setUserId(user_id);
            builder.setApiKey(api_key);

            builder.setImpressions(impCollection);

            //Find the vdp_index of the impression for the lead
            for (int i = 0; i < leadsCollection.size(); i++) {
                Lead lead = leadsCollection.get(i);
                int vdp_index = LeadUtils.findTheVdpIndex(lead, impCollection);
                lead.setVdpIndex(vdp_index);
                leadsCollection.set(i, lead);
            }

            //store the values in the avro object
            builder.setLeads(leadsCollection);

            //Identify the different categories of the user behavior and set the category field
            if (builder.hasLeads() && builder.getLeads().size() != 0)
                category = SUBMITTER;
            else if (builder.hasImpressions() && builder.getImpressions().size() == 1)
                category = BOUNCER;
            else if (isSearcher)
                category = SEARCHER;
            else
                category = BROWSER;

            Text outputKey = key;
            Text outputValue = new Text(builder.build().toString());
            //Emit the key and the generated user session avro value
            multipleOutputs.write("userType", outputKey, outputValue, category);
        }

        //Sets the session level values once
        private boolean setSessionDetails(AvroValue<Session> value) {
            try {
                builder.setUserAgent(value.datum().getUserAgent());
                builder.setResolution(value.datum().getResolution());
                builder.setActivex(value.datum().getActivex());
            } catch (Exception e) {
                return false;
            }
            return true;
        }

        //Closes the multiple output instance
        @Override
        public void cleanup(Context context) throws InterruptedException, IOException{
            multipleOutputs.close();
        }
    }
}
