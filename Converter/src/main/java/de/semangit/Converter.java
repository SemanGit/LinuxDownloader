package de.semangit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.opencsv.CSVReader;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Converter implements Runnable {

    private static AtomicInteger numTriples;

    private static boolean useBlankNodes = true;
    private String workOnFile;
    private String path;
    private static boolean debug = false;
    private static boolean noUserTexts = false;
    private static int sampling = 0;
    private static ArrayList<Float> samplingPercentages = new ArrayList<>();

    //a parameter to better examine the output of the program. Does not create a combined.ttl file, but keeps all output in the original .tll files
    private static boolean mergeOutput = true;

    private static Map<String, BufferedWriter> graphWriters = new HashMap<>();
    private static String rdfPath;

    private static Random rnd = new Random();


    //private static final String PREFIX_Semangit = "<http://www.semangit.de/ontology/>";
    private static final String TAG_Semangit = "semangit:"; //TODO: Speed option: Instead of concatenating "semangit:" to all IDs at runtime, do this once at the beginning for all identifiers.
    private static final String TAG_Userprefix = "ghuser_";
    private static final String TAG_Repoprefix = "ghrepo_";
    private static final String TAG_Commitprefix = "ghcom_";
    private static final String TAG_Commentprefix = "ghcomment_";
    private static final String TAG_Issueprefix = "ghissue_";
    private static final String TAG_Pullrequestprefix = "ghpr_";
    private static final String TAG_Repolabelprefix = "ghlb_";
    private static final String TAG_Langprefix = "ghlang_";
    private static final String TAG_Followprefix = "ghfollow_";
    private static final String TAG_Issue_Eventprefix = "ghissueevent_";
    private static final String TAG_Orga_Join_Eventprefix = "ghorgajoinevent_";
    private static final String TAG_Repo_Join_Eventprefix = "ghrepojoinevent_";
    private static final String TAG_Issue_Commentprefix = "ghissuecomment_";
    private static final String TAG_Pullrequest_Commentprefix = "ghprcomment_";
    private static final String TAG_Repolangprefix = "ghrepolang_";
    private static final String TAG_Pullrequest_Actionprefix = "ghpraction_";
    private static final String TAG_Watcherprefix = "ghwatcher_";

    private static int errorCtr = 0;

    private static boolean prefixing = true; //default: use prefixing
    private static int mode = 0; //default: 0 = base64, 1 = base32, 2 = base16, 3 = base10

    private static final Map<String, String> prefixTable = new HashMap<>();
    private static void initPrefixTable()
    {
        //ProjectCommits
        prefixTable.put(TAG_Semangit + TAG_Repoprefix, ""); //most common prefix gets empty prefix in output
        prefixTable.put(TAG_Semangit + "repository_has_commit", "a"); //TODO: wrong way around?!
        prefixTable.put(TAG_Semangit + TAG_Commitprefix, "b");


        //CommitParents
        prefixTable.put(TAG_Semangit + "commit_has_parent", "c");

        //Followers
        prefixTable.put(TAG_Semangit + "github_commit", "d");
        prefixTable.put(TAG_Semangit + "commit_sha", "e");
        prefixTable.put(TAG_Semangit + "commit_author", "f");
        prefixTable.put(TAG_Semangit + "commit_committed_by", "g");
        prefixTable.put(TAG_Semangit + "commit_created_at", "h");
        prefixTable.put(TAG_Semangit + "github_follow_event", "i");
        prefixTable.put(TAG_Semangit + "github_following_since", "j");
        prefixTable.put(TAG_Semangit + "github_user_or_project", "k");
        prefixTable.put(TAG_Semangit + "github_follower", "l");
        prefixTable.put(TAG_Semangit  + TAG_Userprefix, "m");
        prefixTable.put(TAG_Semangit + "github_follows", "n");

        //Issue events
        prefixTable.put(TAG_Semangit + "github_issue_event", "o");
        prefixTable.put(TAG_Semangit + "github_issue_event_created_at", "p");
        prefixTable.put(TAG_Semangit + "github_issue_event_action_specific_sha", "cd");
        prefixTable.put(TAG_Semangit + "github_issue_event_action", "r");
        prefixTable.put(TAG_Semangit + "github_issue_event_actor", "s");
        prefixTable.put(TAG_Semangit + "github_issue_event_for", "t");

        //Issue Labels
        prefixTable.put(TAG_Semangit + TAG_Issueprefix, "u");
        prefixTable.put(TAG_Semangit + TAG_Repolabelprefix, "v");
        prefixTable.put(TAG_Semangit + "github_issue_label_used_by", "w");

        //Issues
        prefixTable.put(TAG_Semangit + "github_issue", "x");
        prefixTable.put(TAG_Semangit + "github_issue_project", "y");
        prefixTable.put(TAG_Semangit + "github_issue_reporter", "z");
        prefixTable.put(TAG_Semangit + "github_issue_assignee", "A");
        prefixTable.put(TAG_Semangit + "github_issue_pull_request", "B");
        prefixTable.put(TAG_Semangit + "github_issue_created_at", "C");
        prefixTable.put(TAG_Semangit + TAG_Pullrequestprefix, "D");


        //Organization Members
        prefixTable.put(TAG_Semangit + "github_organization_join_event", "E");
        prefixTable.put(TAG_Semangit + "github_organization_joined_at", "F");
        prefixTable.put(TAG_Semangit + "github_organization_joined_by", "G");
        prefixTable.put(TAG_Semangit + "github_organization_is_joined", "H");

        //Project Members
        prefixTable.put(TAG_Semangit + "github_project_join_event", "I");
        prefixTable.put(TAG_Semangit + "github_project_join_event_created_at", "J");
        prefixTable.put(TAG_Semangit + "github_project_joining_user", "K");
        prefixTable.put(TAG_Semangit + "github_project_joined", "L");

        //Projects
        prefixTable.put(TAG_Semangit + "github_project", "M");
        prefixTable.put(TAG_Semangit + "repository_url", "N");
        prefixTable.put(TAG_Semangit + "github_has_owner", "O");
        prefixTable.put(TAG_Semangit + "github_project_name", "P");
        prefixTable.put(TAG_Semangit + "github_project_description", "Q");
        prefixTable.put(TAG_Semangit + "repository_language", "R");
        prefixTable.put(TAG_Semangit + "github_forked_from", "S");
        prefixTable.put(TAG_Semangit + "github_project_deleted", "T");
        prefixTable.put(TAG_Semangit + "repository_created_at", "U");

        //Pull Request Commits
        prefixTable.put(TAG_Semangit + "pull_request_has_commit", "V");

        //Pull Request History
        prefixTable.put(TAG_Semangit + "github_pull_request_action", "W");
        prefixTable.put(TAG_Semangit + "github_pull_request_action_created_at", "X");
        prefixTable.put(TAG_Semangit + "github_pull_request_action_id", "Y");
        prefixTable.put(TAG_Semangit + "github_pull_request_action_type", "Z");
        prefixTable.put(TAG_Semangit + "github_pull_request_actor", "aa");
        prefixTable.put(TAG_Semangit + "github_pull_request_action_pull_request", "ab");

        //Pull Requests
        prefixTable.put(TAG_Semangit + "github_pull_request", "ac");
        prefixTable.put(TAG_Semangit + "pull_request_base_project", "ad");
        prefixTable.put(TAG_Semangit + "pull_request_head_project", "ae");
        prefixTable.put(TAG_Semangit + "pull_request_base_commit", "af");
        prefixTable.put(TAG_Semangit + "pull_request_head_commit", "ag");
        prefixTable.put(TAG_Semangit + "github_pull_request_id", "ah");
        prefixTable.put(TAG_Semangit + "github_pull_request_intra_branch", "ai");

        //Repo Labels
        prefixTable.put(TAG_Semangit + "github_repo_label", "aj");
        prefixTable.put(TAG_Semangit + "github_repo_label_project", "ak");
        prefixTable.put(TAG_Semangit + "github_repo_label_name", "al");

        //User
        prefixTable.put(TAG_Semangit + "github_user", "am");
        prefixTable.put(TAG_Semangit + "github_login", "an");
        prefixTable.put(TAG_Semangit + "github_name", "ao");
        prefixTable.put(TAG_Semangit + "github_company", "ap");
        prefixTable.put(TAG_Semangit + "github_user_location", "aq");
        prefixTable.put(TAG_Semangit + "user_email", "ar");
        prefixTable.put(TAG_Semangit + "github_user_created_at", "as");
        prefixTable.put(TAG_Semangit + "github_user_is_org", "at");
        prefixTable.put(TAG_Semangit + "github_user_deleted", "au");
        prefixTable.put(TAG_Semangit + "github_user_fake", "av");

        //Watchers == Followers

        //Comments
        prefixTable.put(TAG_Semangit + "comment", "aD");
        prefixTable.put(TAG_Semangit + TAG_Commentprefix + "commit_", "aw");
        prefixTable.put(TAG_Semangit + "comment_for", "ax");
        prefixTable.put(TAG_Semangit + "comment_author", "ay");
        prefixTable.put(TAG_Semangit + "comment_body", "az");
        prefixTable.put(TAG_Semangit + "comment_line", "aA");
        prefixTable.put(TAG_Semangit + "comment_pos", "aB");
        prefixTable.put(TAG_Semangit + "comment_created_at", "aC");


        //languages
        prefixTable.put(TAG_Semangit + TAG_Langprefix, "aD");
        prefixTable.put(TAG_Semangit + "github_project_language", "aE");
        prefixTable.put(TAG_Semangit + "github_project_language_bytes", "aF");
        prefixTable.put(TAG_Semangit + "github_project_language_timestamp", "aG");
        prefixTable.put(TAG_Semangit + "github_project_language_repo", "aH");
        prefixTable.put(TAG_Semangit + "github_project_language_is", "aI");
        prefixTable.put(TAG_Semangit + "programming_language", "aO");
        prefixTable.put(TAG_Semangit + "programming_language_name", "aP");



        //update for users
        prefixTable.put(TAG_Semangit + "github_user_lat", "aJ");
        prefixTable.put(TAG_Semangit + "github_user_lng", "aK");
        prefixTable.put(TAG_Semangit + "github_user_country_code", "aL");
        prefixTable.put(TAG_Semangit + "github_user_state", "aM");
        prefixTable.put(TAG_Semangit + "github_user_city", "aN");


        prefixTable.put(TAG_Semangit + "commit_repository", "q");

        prefixTable.put(TAG_Semangit + TAG_Followprefix, "ce");
        prefixTable.put(TAG_Semangit + TAG_Issue_Eventprefix, "cf");
        prefixTable.put(TAG_Semangit + TAG_Orga_Join_Eventprefix, "cg");
        prefixTable.put(TAG_Semangit + TAG_Repo_Join_Eventprefix, "ch");
        prefixTable.put(TAG_Semangit + TAG_Issue_Commentprefix, "ci");
        prefixTable.put(TAG_Semangit + TAG_Pullrequest_Commentprefix, "cj");
        prefixTable.put(TAG_Semangit + TAG_Repolangprefix, "ck");
        prefixTable.put(TAG_Semangit + TAG_Pullrequest_Actionprefix, "cl");
        prefixTable.put(TAG_Semangit + TAG_Watcherprefix, "cm");
        //tag "CD" used farther up

    }

    private static Map<String, Integer> prefixCtr = new HashMap<>();

    private static String getPrefix(String s)
    {
        if(prefixing) {
            if (prefixTable.get(s) == null) {
                System.out.println("Prefix for " + s + " missing.");
            }
            int ctr = 0;
            try {
                if (prefixCtr.containsKey(s)) {
                    ctr = prefixCtr.get(s);
                }
            }
            catch (NullPointerException e)
            {

                //System.out.println("For some random reason, no prefix counter for " + s + " could be retrieved.");
                //e.printStackTrace();
            }
            prefixCtr.put(s, ++ctr);
            return prefixTable.get(s) + ":";
        }
        else {
            return s;
        }
    }

    private static class TableSchema
    {
        private ArrayList<Integer> integerColumns = new ArrayList<>();
        private ArrayList<Boolean> nullableColumns = new ArrayList<>();
        private int totalColumns = 0;
        int integrityChecksPos = 0;
        int integrityChecksNeg = 0;
        int nullabilityFails = 0;
        private TableSchema(){}
    }

    private static String formatDateTime(String date)
    {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'hh-mm-ss");
            return formatter.format( new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(date));
        }
        catch (Exception e)
        {
            System.out.println("Illegal format for date: " + date);
        }
        return date;
    }

    private static HashMap<String, TableSchema> schemata = new HashMap<>();

    /**
     * This function should be executed exactly once before any other parsing function.
     * It gathers some structural information from the schema.sql file so that we can tell "good" and "malformed" entities apart
     *
     * @param path String value to store the path to the schema.sql file
     */

    private static void parseSQLSchema(String path) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(path + "schema.sql"));
            String line;
            String tableName = "";
            int colCtr = 0;
            TableSchema scm = null;

            while ((line = br.readLine()) != null) {
                if (line.contains("CREATE TABLE IF NOT EXISTS"))
                {
                    tableName = line.substring(40, line.length() - 3);
                    //System.out.println("Found table with name " + tableName + " in schema.");
                    scm = new TableSchema();
                    colCtr = 0;
                }
                if(scm == null)
                {
                    continue;
                }
                if((line.contains("CONSTRAINT") || line.contains("DEFAULT CHARACTER SET") || line.contains("PRIMARY KEY") || line.contains("FOREIGN")  || line.contains("ENGINE") )&& !tableName.equals("") && !schemata.containsKey(tableName))
                {
                    scm.totalColumns = colCtr;
                    if(!tableName.equals("schema_info"))
                    {
                        schemata.put(tableName, scm);
                    }
                    //System.out.println("Added a schema for " + tableName + " with " + scm.totalColumns + " columns.");
                    colCtr = 0;
                }
                if(line.contains(" INT(")) //the space before INT is important to avoid also counting booleans (i.e. TINYINT(1))
                {
                    if(!line.contains("CONSTRAINT")) {
                        scm.integerColumns.add(colCtr);
                    }
                }
                //if(line.contains("INT") || line.contains("TINYINT") || line.contains("VARCHAR") || line.contains("TIMESTAMP") || line.contains("MEDIUMTEXT") || line.contains("DECIMAL") || line.contains("CHAR"))
                //VARCHAR covered by CHAR, TINYINT by INT
                if(line.contains("INT") || line.contains("TIMESTAMP") || line.contains("MEDIUMTEXT") || line.contains("DECIMAL") || line.contains("CHAR"))
                {
                    if(line.contains("NOT NULL"))
                    {
                        scm.nullableColumns.add(false);
                    }
                    else
                    {
                        scm.nullableColumns.add(true);
                    }
                    colCtr++;
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }


    /**
     * This function should be called once for every line that is being parsed to make sure the entity is "good" and not "malformed", leading to crashes etc.
     * @param schema The schema of the CSV table that is being parsed, as computed by the parseSQLSchema function above
     * @param line The String Array of the current line as generated by CSV parser
     * @return true, if line seems to be broken, false otherwise.
     */
    private static boolean brokenLineCheck(TableSchema schema, String[] line)
    {
        if(line.length != schema.totalColumns)
        {
            schema.integrityChecksNeg++;
            return true;
        }
        if(!schema.integerColumns.isEmpty())
        {
            for(Integer i : schema.integerColumns)
            {
                //Test if line may be empty and check for null!
                if(schema.nullableColumns.get(i))
                {
                    //null values allowed.
                    if(line[i].equals("") || line[i].equals("N"))
                    {
                        continue;
                    }
                }
                try{
                    Integer.parseInt(line[i]);
                }
                catch (Exception e)
                {
                    schema.integrityChecksNeg++;
                    return true;
                }
            }
        }
        int currentCol = 0;
        for(boolean b : schema.nullableColumns)
        {
            if(!b) //column must not be null
            {
                if(line[currentCol].equals("") || line[currentCol].equals("N"))
                {
                    schema.integrityChecksNeg++;
                    schema.nullabilityFails++;
                    return true;
                }
            }
            currentCol++;
        }
        schema.integrityChecksPos++;
        return false;
    }

    private static String b64(String input)
    {
        if(mode == 0) {
            String alphabet64 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"; //can only find 63 ASCII characters that work except for ":", but not sure if that breaks the syntax
            // base64 on ID only
            // for forward/backward conversion, see https://stackoverflow.com/a/26172045/9743294
            StringBuilder sb = new StringBuilder();
            try {
                String rightOfColon = input.substring(input.indexOf(":") + 1);
                String leftOfColon = input.substring(0, input.indexOf(":") + 1);

                int in = Integer.parseInt(rightOfColon);
                int j = (int) Math.ceil(Math.log(in) / Math.log(alphabet64.length()));
                for (int i = 0; i < j; i++) {
                    sb.append(alphabet64.charAt(in % alphabet64.length()));
                    in /= alphabet64.length();
                }
                return leftOfColon + sb.reverse().toString();
            } catch (Exception e) {
                errorCtr++;
                e.printStackTrace();
                return input;
            }
        }
        //base32 attempt on the ID only (not prefix)
        else if(mode == 1) {
            try {
                String rightOfComma = input.substring(input.lastIndexOf(":") + 1);
                String leftOfComma = input.substring(0, input.lastIndexOf(":") + 1);
                return leftOfComma + Integer.toString(Integer.parseInt(rightOfComma), 32);
            } catch (Exception e) {
                errorCtr++;
                e.printStackTrace();
                return input;
            }
        }

        //base36 attempt on the ID only (not prefix)
        /*String rightOfComma = input.substring(input.lastIndexOf(":") + 1);
        String leftOfComma = input.substring(0,input.lastIndexOf(":") + 1);
        return leftOfComma + Integer.toString(Integer.parseInt(rightOfComma), 36);
        */

        else if (mode == 2) {
            //base16 attempt on the ID only (not prefix)
            try {
                String rightOfComma = input.substring(input.lastIndexOf(":") + 1);
                String leftOfComma = input.substring(0, input.lastIndexOf(":") + 1);
                return leftOfComma + Integer.toString(Integer.parseInt(rightOfComma), 16);
            } catch (Exception e) {
                errorCtr++;
                e.printStackTrace();
                return input;
            }
        }
        else {
            //no conversion
            return input;
        }
    }
    private static void printTriples(String currentTriple, ArrayList<BufferedWriter> writers)
    {
        try {
            //experimental: count triples. To be replaced by some database query
            if(debug) {
                int ctr = 0;
                ctr += currentTriple.length() - currentTriple.replace(".\n", "\n").length(); //occurrences of terminating dots
                ctr += currentTriple.length() - currentTriple.replace(",\n", "\n").length(); //occurrences of terminating commas
                ctr += currentTriple.length() - currentTriple.replace(";\n", "\n").length(); //occurrences of terminating semicolons

                numTriples.addAndGet(ctr);
            }
            switch (sampling)
            {
                case 0: writers.get(0).write(currentTriple);break; //no sampling
                //cases 1 and 2 (head/tail sampling) removed. doing those via bash instead for better performance
                case 3:           //random sampling with given percentage(s)
                    if(samplingPercentages.size() == 1) //only one sample to be generated
                    {
                        if(rnd.nextFloat() < samplingPercentages.get(0)){
                            writers.get(0).write(currentTriple);
                        }
                        break;
                    }
                    else //multiple samples. Got one file handler per percentage
                    {
                        for(int i = 0; i < samplingPercentages.size(); i++)
                        {
                            if(rnd.nextFloat() < samplingPercentages.get(i))
                            {
                                writers.get(i).write(currentTriple);
                            }
                        }
                    }
                case 4: case 5: //connected & bfs
                //get last 2 digits of identifier, if its not a blank node (i.e. has no identifier)
                if(currentTriple.charAt(2) == '[' || currentTriple.charAt(0) == '[' || currentTriple.charAt(1) == '[')
                {
                    return;
                }
                int index = currentTriple.indexOf(" "); //end of identifier
                String identifier = currentTriple.substring(index - 2, index);
                if(identifier.contains(":"))
                {
                    if(identifier.charAt(0) == ':')
                    {
                        identifier = identifier.replace(':', '0');
                    }
                    else
                    {
                        identifier = "00";
                    }
                }
                if(!graphWriters.containsKey(identifier)) {
                    File file = new File(rdfPath + identifier.charAt(0) + "/" + identifier.charAt(1) + ".ttl");
                    if (!file.exists()) {
                        File parentDirec = new File(rdfPath + identifier.charAt(0) + "/");
                        if (!parentDirec.exists()) {
                            //noinspection ResultOfMethodCallIgnored
                            parentDirec.mkdirs();
                        }
                    }
                    BufferedWriter wr = new BufferedWriter(new FileWriter(file));
                    graphWriters.put(identifier, wr);
                    wr.write(currentTriple); //avoid null pointer exception
                    return;
                }
                //TODO: sometimes causes random nullpointerexceptions at start
                graphWriters.get(identifier).write(currentTriple);
                /*if(w == null)
                {
                    w = new BufferedWriter(new FileWriter(rdfPath + identifier.charAt(0) + "/" + identifier.charAt(1)), 32768);
                    graphWriters.put(identifier, w);
                }
                w.write(currentTriple);*/
                //TODO: Any kind of closing/reopening? Could create a FileWriter with append flag set to true
                break;
            }
        }
        catch (java.io.IOException e)
        {
            e.printStackTrace();
            errorCtr++;
        }

    }

    private static void parseCommitParents(String path) {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "commit_parents.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/commit_parents.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/commit_parents.ttl"), 32768));
                    }
                }
            }
            String[] nextLine;
            String[] curLine;


            curLine = reader.readNext();
            boolean abbreviated = false;

            TableSchema schema = schemata.get("commit_parents");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }

                if (!abbreviated) {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + curLine[0]) ).append( " " ).append( getPrefix(TAG_Semangit + "commit_has_parent") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + curLine[1]));
                } else {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + curLine[1])); //only specifying next object. subject/predicate are abbreviated
                }
                if (curLine[0].equals(nextLine[0])) {
                    currentTriple.append(",\n"); //abbreviating subject and predicate for next line
                    abbreviated = true;
                } else {
                    currentTriple.append(".\n"); //cannot use turtle abbreviation here
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                    abbreviated = false;
                }
                curLine = nextLine;
            }
            //handle last line of file
            if(curLine.length == 2){
                if (!abbreviated) {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + curLine[0]) ).append( " " ).append( getPrefix(TAG_Semangit + "commit_has_parent") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + curLine[1]) ).append( ".");
                } else {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + curLine[1]) ).append( "."); //only specifying next object. subject/predicate are abbreviated
                }
                currentTriple.append("\n");
                printTriples(currentTriple.toString(), writers);
            }
            for(BufferedWriter w : writers) {
                w.close();
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void parseCommits(String path) {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "commits.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/commits.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/commits.ttl"), 32768));
                    }
                }
            }
            String[] nextLine;

            TableSchema schema = schemata.get("commits");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }

                String commitURI = b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + nextLine[0]);
                currentTriple.append(  commitURI ).append( " a " ).append( getPrefix(TAG_Semangit + "github_commit") ).append( ";\n");
                currentTriple.append(getPrefix(TAG_Semangit + "commit_sha") ).append( " \"" ).append( nextLine[1] ).append( "\";\n");
                currentTriple.append(getPrefix(TAG_Semangit + "commit_author") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[2]) ).append( ";\n");
                currentTriple.append(getPrefix(TAG_Semangit + "commit_committed_by") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[3]) ).append( ";\n");
                if(!nextLine[4].equals("N")) {
                    currentTriple.append(getPrefix(TAG_Semangit + "commit_repository") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + nextLine[4]) ).append( ";\n");
                }
                currentTriple.append(getPrefix(TAG_Semangit + "commit_created_at") ).append( " \"" ).append( formatDateTime(nextLine[5]) ).append( "\"^^xsd:dateTime.\n");
                printTriples(currentTriple.toString(), writers);
                currentTriple.setLength(0); //clear
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void parseFollowers(String path)
    {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "followers.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/followers.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/followers.ttl"), 32768));
                    }
                }
            }
            int idCtr = 0;

            String[] nextLine;

            TableSchema schema = schemata.get("followers");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }

                if(useBlankNodes)
                {
                    currentTriple.append("[ a " ).append( getPrefix(TAG_Semangit + "github_follow_event") ).append( ";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_following_since") ).append( " \"" ).append( nextLine[2] ).append( "\";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_user_or_project") ).append( " false ] " ).append( getPrefix(TAG_Semangit + "github_follower") ).append( " " ).append( b64(getPrefix(TAG_Semangit  + TAG_Userprefix) + nextLine[1]) ).append( ";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_follows") ).append( " " ).append( b64(getPrefix(TAG_Semangit  + TAG_Userprefix) + nextLine[0]) ).append( ".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);

                }
                else
                {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Followprefix) + idCtr++)).append(" a " ).append( getPrefix(TAG_Semangit + "github_follow_event") ).append( ";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_following_since") ).append( " \"" ).append( nextLine[2] ).append( "\";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_user_or_project") ).append( " false;\n " ).append( getPrefix(TAG_Semangit + "github_follower") ).append( " " ).append( b64(getPrefix(TAG_Semangit  + TAG_Userprefix) + nextLine[1]) ).append( ";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_follows") ).append( " " ).append( b64(getPrefix(TAG_Semangit  + TAG_Userprefix) + nextLine[0]) ).append( ".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);

                }
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }



    private static void parseIssueEvents(String path) {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "issue_events.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/issue_events.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/issue_events.ttl"), 32768));
                    }
                }
            }
            String[] nextLine;

            TableSchema schema = schemata.get("issue_events");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }

                if(useBlankNodes) {
                    //event id, issue id, actor id, action, action specific sha, created at
                    currentTriple.append("[ a ").append(getPrefix(TAG_Semangit + "github_issue_event")).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_issue_event_created_at")).append(" \"").append(formatDateTime(nextLine[5])).append("\"^^xsd:dateTime;");
                    currentTriple.append("\n");
                    if (!nextLine[4].equals("N")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "github_issue_event_action_specific_sha")).append(" \"").append(nextLine[4]).append("\";");
                        currentTriple.append("\n");
                    }
                    currentTriple.append(getPrefix(TAG_Semangit + "github_issue_event_action")).append(" \"").append(nextLine[3]).append("\" ] ");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_issue_event_actor")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[2])).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_issue_event_for")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Issueprefix) + nextLine[1])).append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
                else
                {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Issue_Eventprefix) + nextLine[0])).append(" a ").append(getPrefix(TAG_Semangit + "github_issue_event")).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_issue_event_created_at")).append(" \"").append(formatDateTime(nextLine[5])).append("\"^^xsd:dateTime;");
                    currentTriple.append("\n");
                    if (!nextLine[4].equals("N")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "github_issue_event_action_specific_sha")).append(" \"").append(nextLine[4]).append("\";");
                        currentTriple.append("\n");
                    }
                    currentTriple.append(getPrefix(TAG_Semangit + "github_issue_event_action")).append(" \"").append(nextLine[3]).append("\";\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_issue_event_actor")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[2])).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_issue_event_for")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Issueprefix) + nextLine[1])).append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }



    private static void parseIssueLabels(String path) {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "issue_labels.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/issue_labels.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/issue_labels.ttl"), 32768));
                    }
                }
            }

            String[] nextLine;

            String[] curLine = reader.readNext();
            boolean abbreviated = false;

            TableSchema schema = schemata.get("issue_labels");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }


                if(abbreviated)
                {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Issueprefix) + curLine[1])); //only print object
                }
                else
                {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Repolabelprefix) + curLine[0]) ).append( " " ).append( getPrefix(TAG_Semangit + "github_issue_label_used_by") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Issueprefix) + curLine[1])); //print entire triple
                }
                if(curLine[0].equals(nextLine[0]))
                {
                    abbreviated = true;
                    currentTriple.append(",");
                    currentTriple.append("\n");
                }
                else {
                    abbreviated = false;
                    currentTriple.append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }


                curLine = nextLine;
            }
            if(abbreviated)
            {
                currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Issueprefix) + curLine[1])); //only print object
            }
            else
            {
                currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Repolabelprefix) + curLine[0]) ).append( " " ).append( getPrefix(TAG_Semangit + "github_issue_label_used_by") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Issueprefix) + curLine[1])); //print entire triple
            }
            currentTriple.append(".");
            currentTriple.append("\n");
            printTriples(currentTriple.toString(), writers);
            for(BufferedWriter w : writers) {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void parseIssues(String path) {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "issues.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/issues.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/issues.ttl"), 32768));
                    }
                }
            }

            String[] nextLine;

            String[] curLine = reader.readNext();

            TableSchema schema = schemata.get("issues");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }

                //check if line is "duplicate", i.e. only sql id is not identical
                if(nextLine[7].equals(curLine[7]) && nextLine[6].equals(curLine[6]) && nextLine[5].equals(curLine[5]) &&nextLine[4].equals(curLine[4]) && nextLine[3].equals(curLine[3]) && nextLine[2].equals(curLine[2]) && nextLine[1].equals(curLine[1]))
                {
                    curLine = nextLine;
                    continue;
                }
                String issueURL = b64(getPrefix(TAG_Semangit + TAG_Issueprefix) + curLine[7]);
                currentTriple.append( issueURL ).append( " a " ).append( getPrefix(TAG_Semangit + "github_issue") ).append( ";");
                currentTriple.append("\n");
                currentTriple.append(getPrefix(TAG_Semangit + "github_issue_project") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + curLine[1]) ).append( ";");
                currentTriple.append("\n");
                if(!curLine[2].equals("N"))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_issue_reporter") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Userprefix) + curLine[2]) ).append( ";");
                    currentTriple.append("\n");
                }
                if(!curLine[3].equals("N"))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_issue_assignee") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Userprefix) + curLine[3]) ).append( ";");
                    currentTriple.append("\n");
                }
                if(!curLine[5].equals("N"))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_issue_pull_request") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Pullrequestprefix) + curLine[5]) ).append( ";");
                    currentTriple.append("\n");
                }
                currentTriple.append(getPrefix(TAG_Semangit + "github_issue_created_at") ).append( " \"" ).append( formatDateTime(curLine[6]) ).append( "\"^^xsd:dateTime.");
                currentTriple.append("\n");
                printTriples(currentTriple.toString(), writers);
                currentTriple.setLength(0);
                curLine = nextLine;

            }
            //Handle last line
            String issueURL = b64(getPrefix(TAG_Semangit + TAG_Issueprefix) + curLine[7]);
            currentTriple.append( issueURL ).append( " a " ).append( getPrefix(TAG_Semangit + "github_issue") ).append( ";");
            currentTriple.append("\n");
            currentTriple.append(getPrefix(TAG_Semangit + "github_issue_project") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + curLine[1]) ).append( ";");
            currentTriple.append("\n");
            if(!curLine[2].equals("N"))
            {
                currentTriple.append(getPrefix(TAG_Semangit + "github_issue_reporter") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Userprefix) + curLine[2]) ).append( ";");
                currentTriple.append("\n");
            }
            if(!curLine[3].equals("N"))
            {
                currentTriple.append(getPrefix(TAG_Semangit + "github_issue_assignee") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Userprefix) + curLine[3]) ).append( ";");
                currentTriple.append("\n");
            }
            if(!curLine[5].equals("N"))
            {
                currentTriple.append(getPrefix(TAG_Semangit + "github_issue_pull_request") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Pullrequestprefix) + curLine[5]) ).append( ";");
                currentTriple.append("\n");
            }
            currentTriple.append(getPrefix(TAG_Semangit + "github_issue_created_at") ).append( " \"" ).append( curLine[6] ).append( "\".");
            currentTriple.append("\n");
            printTriples(currentTriple.toString(), writers);
            currentTriple.setLength(0);
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);

        }
    }

    private static void parseOrganizationMembers(String path)
    {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "organization_members.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/organization_members.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/organization_members.ttl"), 32768));
                    }
                }
            }
            String[] nextLine;

            int idCtr = 0;

            TableSchema schema = schemata.get("organization_members");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }

                if(useBlankNodes) {
                    currentTriple.append("[ a ").append(getPrefix(TAG_Semangit + "github_organization_join_event")).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_organization_joined_at")).append(" \"").append(formatDateTime(nextLine[2])).append("\"^^xsd:dateTime ] ").append(getPrefix(TAG_Semangit + "github_organization_joined_by")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[1])).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_organization_is_joined")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[0])).append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
                else
                {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Orga_Join_Eventprefix) + idCtr++)).append(" a ").append(getPrefix(TAG_Semangit + "github_organization_join_event")).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_organization_joined_at")).append(" \"").append(formatDateTime(nextLine[2])).append("\"^^xsd:dateTime;\n").append(getPrefix(TAG_Semangit + "github_organization_joined_by")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[1])).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_organization_is_joined")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[0])).append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }



    private static void parseProjectCommits(String path)
    {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "project_commits.csv"));
            StringBuilder sb = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/project_commits.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/project_commits.ttl"), 32768));
                    }
                }
            }

            String[] nextLine;

            String[] curLine = reader.readNext();
            boolean abbreviated = false;

            TableSchema schema = schemata.get("project_commits");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }


                if(abbreviated) //abbreviated in previous step. Only need to print object now
                {
                    sb.append(b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + curLine[1])); //one commit for multiple repositories (branching / merging)
                }
                else //no abbreviation occurred. Full subject predicate object triple printed
                {
                    sb.append(b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + curLine[0]) ).append( " " ) .append( getPrefix(TAG_Semangit + "repository_has_commit") ).append( " " ) .append( b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + curLine[1]));
                }

                abbreviated = (curLine[0].equals(nextLine[0]));
                curLine = nextLine;
                if(abbreviated)
                {
                    sb.append(",\n");
                }
                else {
                    sb.append(".\n");
                    printTriples(sb.toString(), writers);
                    sb.setLength(0);
                }
            }

            //handle last line
            if(abbreviated) //abbreviated in previous step. Only need to print object now
            {
                sb.append(b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + curLine[1]) ); //one commit for multiple repositories (branching / merging)
            }
            else //no abbreviation occurred. Full subject predicate object triple printed
            {
                sb.append(b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + curLine[0]) ).append( " " ).append( getPrefix(TAG_Semangit + "repository_has_commit") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + curLine[1]) );
            }
            sb.append(".\n");
            printTriples(sb.toString(), writers);
            //System.out.println("Is this broken? " + sb.toString());

            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }




    private static void parseProjectMembers(String path)
    {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "project_members.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/project_members.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/project_members.ttl"), 32768));
                    }
                }
            }
            int idCtr = 0;

            String[] nextLine;

            TableSchema schema = schemata.get("project_members");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }


                if(useBlankNodes) {
                    currentTriple.append("[ a ").append(getPrefix(TAG_Semangit + "github_project_join_event")).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_join_event_created_at")).append(" \"").append(formatDateTime(nextLine[2])).append("\"^^xsd:dateTime ] ");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_joining_user")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[1])).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_joined")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + nextLine[0])).append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
                else {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Repo_Join_Eventprefix) + idCtr++)).append(" a ").append(getPrefix(TAG_Semangit + "github_project_join_event")).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_join_event_created_at")).append(" \"").append(formatDateTime(nextLine[2])).append("\"^^xsd:dateTime;\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_joining_user")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[1])).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_joined")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + nextLine[0])).append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }




    private static void parseProjects(String path)
    {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "projects.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/projects.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/projects.ttl"), 32768));
                    }
                }
            }

            String[] nextLine;

            TableSchema schema = schemata.get("projects");
            while ((nextLine = reader.readNext()) != null) {
                if (brokenLineCheck(schema, nextLine)) {
                    schema.integrityChecksNeg--; //re-testing
                    //changes have been made to the structure without documenting it in the schema... so here goes a check for that
                    if (brokenLineCheck(schema, Arrays.copyOf(nextLine, nextLine.length - 1))) {
                        //System.out.println("Still broken...");
                        continue;
                    }
                }


                for (int i = 0; i < nextLine.length; i++) {
                    nextLine[i] = groovy.json.StringEscapeUtils.escapeJava(nextLine[i]);
                }
                currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + nextLine[0]) ).append( " a " ).append( getPrefix(TAG_Semangit + "github_project") ).append( ";");
                currentTriple.append("\n");
                currentTriple.append(getPrefix(TAG_Semangit + "repository_url") ).append( " \"" ).append( nextLine[1] ).append( "\";");
                currentTriple.append("\n");
                currentTriple.append(getPrefix(TAG_Semangit + "github_has_owner") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[2]) ).append( ";");
                currentTriple.append("\n");
                currentTriple.append(getPrefix(TAG_Semangit + "github_project_name") ).append( " \"" ).append( nextLine[3] ).append( "\";");
                currentTriple.append("\n");
                if (!nextLine[4].equals("")) {

                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_description") ).append( " \"" ).append( nextLine[4] ).append( "\";");
                    currentTriple.append("\n");
                }
                if (!nextLine[5].equals("N")) {
                    currentTriple.append(getPrefix(TAG_Semangit + "repository_language") ).append( " \"" ).append( nextLine[5] ).append( "\";");
                    currentTriple.append("\n");
                }
                if (!nextLine[7].equals("N")) {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_forked_from") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + nextLine[7]) ).append( ";");
                    currentTriple.append("\n");
                }
                if (nextLine[8].equals("1")) {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_deleted") ).append( " true;");
                    currentTriple.append("\n");
                } else {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_deleted") ).append( " false;");
                    currentTriple.append("\n");
                }
                //Not taking "last update" into account, as we can easily compute that on a graph database. TODO: reconsider?
                currentTriple.append(getPrefix(TAG_Semangit + "repository_created_at") ).append( " \"" ).append( formatDateTime(nextLine[6]) ).append( "\"^^xsd:dateTime.");
                currentTriple.append("\n");
                printTriples(currentTriple.toString(), writers);
                currentTriple.setLength(0);
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void parsePullRequestCommits(String path)
    {
        try
        {
            CSVReader reader = new CSVReader(new FileReader(path + "pull_request_commits.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/pull_request_commits.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/pull_request_commits.ttl"), 32768));
                    }
                }
            }

            String[] nextLine;

            String[] curLine = reader.readNext();
            boolean abbreviated = false;

            TableSchema schema = schemata.get("pull_request_commits");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }


                if(abbreviated)
                {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + curLine[1]));
                }
                else
                {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Pullrequestprefix) + curLine[0]) ).append( " " ).append( getPrefix(TAG_Semangit + "pull_request_has_commit") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + curLine[1]));
                }
                if(curLine[0].equals(nextLine[0]))
                {
                    abbreviated = true;
                    currentTriple.append(",");
                    currentTriple.append("\n");
                }
                else
                {
                    abbreviated = false;
                    currentTriple.append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
                curLine = nextLine;
            }
            //handle last line of file
            if(abbreviated)
            {
                currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + curLine[1]) ).append( ".");
            }
            else
            {
                currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Pullrequestprefix) + curLine[0]) ).append( " " ).append( getPrefix(TAG_Semangit + "pull_request_has_commit") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + curLine[1]) ).append( ".\n");
            }
            printTriples(currentTriple.toString(), writers);
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void parsePullRequestHistory(String path) {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "pull_request_history.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/pull_request_history.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/pull_request_history.ttl"), 32768));
                    }
                }
            }

            int idCtr = 0;

            String[] nextLine;

            TableSchema schema = schemata.get("pull_request_history");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }

                if(useBlankNodes) {
                    currentTriple.append("[ a ").append(getPrefix(TAG_Semangit + "github_pull_request_action")).append(";");
                    currentTriple.append("\n");
                    //id, PR id, created at, action, actor
                    currentTriple.append(getPrefix(TAG_Semangit + "github_pull_request_action_created_at")).append(" \"").append(formatDateTime(nextLine[2])).append("\"^^xsd:dateTime;");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_pull_request_action_id")).append(" ").append(nextLine[0]).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_pull_request_action_type")).append(" \"").append(nextLine[3]).append("\" ] ");
                    if (!nextLine[4].equals("N")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "github_pull_request_actor")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[4])).append(";");
                        currentTriple.append("\n");
                    }
                    if (!nextLine[1].equals("N")) {
                        //TODO: Need some else part here to not end up with broken triples
                        currentTriple.append(getPrefix(TAG_Semangit + "github_pull_request_action_pull_request")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Pullrequestprefix) + nextLine[1])).append(".");
                        currentTriple.append("\n");
                        printTriples(currentTriple.toString(), writers);
                        currentTriple.setLength(0);
                    }
                }
                else {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Pullrequest_Actionprefix) + idCtr++)).append(" a ").append(getPrefix(TAG_Semangit + "github_pull_request_action")).append(";");
                    currentTriple.append("\n");
                    //id, PR id, created at, action, actor
                    currentTriple.append(getPrefix(TAG_Semangit + "github_pull_request_action_created_at")).append(" \"").append(formatDateTime(nextLine[2])).append("\"^^xsd:dateTime;");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_pull_request_action_id")).append(" ").append(nextLine[0]).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_pull_request_action_type")).append(" \"").append(nextLine[3]).append("\";\n");
                    if (!nextLine[4].equals("N")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "github_pull_request_actor")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[4])).append(";");
                        currentTriple.append("\n");
                    }
                    if (!nextLine[1].equals("N")) {
                        //TODO: Need some else part here to not end up with broken triples
                        currentTriple.append(getPrefix(TAG_Semangit + "github_pull_request_action_pull_request")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Pullrequestprefix) + nextLine[1])).append(".");
                        currentTriple.append("\n");
                        printTriples(currentTriple.toString(), writers);
                        currentTriple.setLength(0);
                    }
                }
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }




    private static void parsePullRequests(String path) {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "pull_requests.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/pull_requests.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/pull_requests.ttl"), 32768));
                    }
                }
            }

            String[] nextLine;

            TableSchema schema = schemata.get("pull_requests");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }

                currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Pullrequestprefix) + nextLine[0]) ).append( " a " ).append( getPrefix(TAG_Semangit + "github_pull_request") ).append( ";");
                currentTriple.append("\n");
                if(!nextLine[2].equals("N")) {
                    currentTriple.append(getPrefix(TAG_Semangit + "pull_request_base_project") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + nextLine[2]) ).append( ";");
                    currentTriple.append("\n");
                }
                if(!nextLine[1].equals("N")) {
                    currentTriple.append(getPrefix(TAG_Semangit + "pull_request_head_project") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + nextLine[1]) ).append( ";");
                    currentTriple.append("\n");
                }
                if(!nextLine[4].equals("N")) {
                    currentTriple.append(getPrefix(TAG_Semangit + "pull_request_base_commit") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + nextLine[4]) ).append( ";");
                    currentTriple.append("\n");
                }
                if(!nextLine[3].equals("N")) {
                    currentTriple.append(getPrefix(TAG_Semangit + "pull_request_head_commit") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + nextLine[3]) ).append( ";");
                    currentTriple.append("\n");
                }
                if(!nextLine[5].equals("N")) {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_pull_request_id") ).append( " " ).append( nextLine[5] ).append( ";");
                    currentTriple.append("\n");
                }
                currentTriple.append(getPrefix(TAG_Semangit + "github_pull_request_intra_branch") ).append( " ");
                if(nextLine[6].equals("0"))
                {
                    currentTriple.append("false");
                }
                else{
                    currentTriple.append("true");
                }
                currentTriple.append(".");
                currentTriple.append("\n");
                printTriples(currentTriple.toString(), writers);
                currentTriple.setLength(0);
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void parseRepoLabels(String path)
    {
        try
        {
            CSVReader reader = new CSVReader(new FileReader(path + "repo_labels.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/repo_labels.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/repo_labels.ttl"), 32768));
                    }
                }
            }

            String[] nextLine;

            TableSchema schema = schemata.get("repo_labels");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }

                for (int i = 0; i < nextLine.length; i++) {
                    nextLine[i] = groovy.json.StringEscapeUtils.escapeJava(nextLine[i]);
                }
                currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Repolabelprefix) + nextLine[0]) ).append( " a " ).append( getPrefix(TAG_Semangit + "github_repo_label") ).append( ";");
                currentTriple.append("\n");
                currentTriple.append(getPrefix(TAG_Semangit + "github_repo_label_project") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + nextLine[1]) ).append( ";");
                currentTriple.append("\n");
                currentTriple.append(getPrefix(TAG_Semangit + "github_repo_label_name") ).append( " \"" ).append( nextLine[2] ).append( "\".");
                currentTriple.append("\n");
                printTriples(currentTriple.toString(), writers);
                currentTriple.setLength(0);
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void parseRepoMilestones(String path)
    {
        try
        {
            CSVReader reader = new CSVReader(new FileReader(path + "repo_milestones.csv"));
            //StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/repo_milestones.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/repo_milestones.ttl"), 32768));
                    }
                }
            }

            String[] nextLine;

            TableSchema schema = schemata.get("repo_milestones");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }

                for (int i = 0; i < nextLine.length; i++) {
                    nextLine[i] = groovy.json.StringEscapeUtils.escapeJava(nextLine[i]);
                }
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }




    private static void parseUsers(String path)
    {
        try
        {
            CSVReader reader = new CSVReader(new FileReader(path + "users.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/users.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/users.ttl"), 32768));
                    }
                }
            }

            String[] nextLine;

            TableSchema schema = schemata.get("users");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }

                for (int i = 0; i < nextLine.length; i++) {
                    nextLine[i] = groovy.json.StringEscapeUtils.escapeJava(nextLine[i]);
                }

                //System.out.println(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[0]));
                String userURI = b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[0]);
                currentTriple.append(userURI ).append( " a " ).append( getPrefix(TAG_Semangit + "github_user") ).append( ";");
                currentTriple.append("\n");
                if(!nextLine[1].equals("N"))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_login") ).append( " \"" ).append( nextLine[1] ).append( "\";");
                    currentTriple.append("\n");
                }
                /*if(!nextLine[2].equals("N"))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_name") + " \"" + nextLine[2] + "\";");
                    currentTriple.append("\n");
                }*/
                if(!nextLine[2].equals("N"))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_company") ).append( " \"" ).append( nextLine[2] ).append( "\";");
                    currentTriple.append("\n");
                }
                if(!nextLine[3].equals("N"))
                    currentTriple.append(getPrefix(TAG_Semangit + "github_user_created_at") ).append( " \"" ).append( formatDateTime(nextLine[3]) ).append( "\"^^xsd:dateTime;");
                currentTriple.append("\n");

                currentTriple.append(getPrefix(TAG_Semangit + "github_user_is_org") ).append( " ");
                if(nextLine[4].equals("USR"))
                {
                    currentTriple.append("false;");
                    currentTriple.append("\n");
                }
                else
                {
                    currentTriple.append("true;");
                    currentTriple.append("\n");
                }

                currentTriple.append(getPrefix(TAG_Semangit + "github_user_deleted") ).append( " ");
                if(nextLine[5].equals("0"))
                {
                    currentTriple.append("false;");
                    currentTriple.append("\n");
                }
                else
                {
                    currentTriple.append("true;");
                    currentTriple.append("\n");
                }

                if(!nextLine[7].equals("N") && !nextLine[7].equals(""))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_user_lng") ).append( " \"" ).append( nextLine[7] ).append( "\";");
                    currentTriple.append("\n");
                }
                if(!nextLine[8].equals("N") && !nextLine[8].equals(""))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_user_lat") ).append( " \"" ).append( nextLine[8] ).append( "\";");
                    currentTriple.append("\n");
                }

                if(!nextLine[9].equals("N") && !nextLine[9].equals(""))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_user_country_code") ).append( " \"" ).append( nextLine[9] ).append( "\";");
                    currentTriple.append("\n");
                }

                if(!nextLine[10].equals("N") && !nextLine[10].equals(""))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_user_state") ).append( " \"" ).append( nextLine[10] ).append( "\";");
                    currentTriple.append("\n");
                }

                if(!nextLine[11].equals("N") && !nextLine[11].equals(""))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_user_city") ).append( " \"" ).append( nextLine[11] ).append( "\";");
                    currentTriple.append("\n");
                }

                if(!nextLine[12].equals("N") && !nextLine[12].equals(""))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "github_user_location") ).append( " \"" ).append( nextLine[12] ).append( "\";");
                    currentTriple.append("\n");
                }

                currentTriple.append(getPrefix(TAG_Semangit + "github_user_fake") ).append( " ");
                if(nextLine[6].equals("0"))
                {
                    currentTriple.append("false.");
                    currentTriple.append("\n");
                }
                else
                {
                    currentTriple.append("true.");
                    currentTriple.append("\n");
                }
                printTriples(currentTriple.toString(), writers);
                currentTriple.setLength(0);

            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            System.out.println("Warning! Cannot parse users. Maybe working on an old dump? Skipping...");
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }

    //watchers

    private static void parseWatchers(String path)
    {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "watchers.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/watchers.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/watchers.ttl"), 32768));
                    }
                }
            }

            int idCtr = 0;

            String[] nextLine;

            TableSchema schema = schemata.get("watchers");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }

                if(useBlankNodes) {
                    currentTriple.append("[ a ").append(getPrefix(TAG_Semangit + "github_follow_event")).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_following_since")).append(" \"").append(formatDateTime(nextLine[2])).append("\"^^xsd:dateTime;");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_user_or_project")).append(" true ] ").append(getPrefix(TAG_Semangit + "github_follower")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[1])).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_follows")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + nextLine[0])).append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
                else
                {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Watcherprefix) + idCtr++)).append(" a ").append(getPrefix(TAG_Semangit + "github_follow_event")).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_following_since")).append(" \"").append(formatDateTime(nextLine[2])).append("\"^^xsd:dateTime;");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_user_or_project")).append(" true;\n").append(getPrefix(TAG_Semangit + "github_follower")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[1])).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_follows")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + nextLine[0])).append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }




    //project_languages

    private static void parseProjectLanguages(String path)
    {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "project_languages.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/project_languages.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/project_languages.ttl"), 32768));
                    }
                }
            }

            String[] nextLine;
            Map<String, Integer> languages = new HashMap<>();
            int langCtr = 0;
            int idCtr = 0;
            int currentLang;

            TableSchema schema = schemata.get("project_languages");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }


                if(languages.containsKey(nextLine[1])) {
                    currentLang = languages.get(nextLine[1]);
                }
                else
                {
                    languages.put(nextLine[1], langCtr++);
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Langprefix) + langCtr) ).append( " a " ).append( getPrefix(TAG_Semangit + "programming_language") ).append( ";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "programming_language_name") ).append( " \"" ).append( nextLine[1] ).append( "\".");
                    currentTriple.append("\n");
                    currentLang = langCtr;
                }
                if(useBlankNodes) {
                    currentTriple.append("[ a ").append(getPrefix(TAG_Semangit + "github_project_language")).append(";");
                    currentTriple.append("\n");
                    //bytes, timestamp, then close brackets and do remaining two links
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_language_bytes")).append(" ").append(nextLine[2]).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_language_timestamp")).append(" \"").append(formatDateTime(nextLine[3])).append("\"^^xsd:dateTime ] ").append(getPrefix(TAG_Semangit + "github_project_language_repo")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + nextLine[0])).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_language_is")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Langprefix) + currentLang)).append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
                else
                {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Repolangprefix) + idCtr++)).append(" a ").append(getPrefix(TAG_Semangit + "github_project_language")).append(";");
                    currentTriple.append("\n");
                    //bytes, timestamp, then close brackets and do remaining two links
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_language_bytes")).append(" ").append(nextLine[2]).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_language_timestamp")).append(" \"").append(formatDateTime(nextLine[3])).append("\"^^xsd:dateTime;\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_language_repo")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Repoprefix) + nextLine[0])).append(";");
                    currentTriple.append("\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "github_project_language_is")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Langprefix) + currentLang)).append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (FileNotFoundException e)
        {
            System.out.println("Warning, seem to be working on an old dump. Some files could not be read from the SQL file.");
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }


    //parseProjectTopics added as .csv file, but undocumented! Skipping this tiny file




    /**
     * Comment section. Below are all functions related to comments.
     * commit_comments
     * issue_comments
     * pull_request_comments
     */

    private static void parseCommitComments(String path) {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "commit_comments.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/commit_comments.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/commit_comments.ttl"), 32768));
                    }
                }
            }

            String[] nextLine;
            int consecutiveFailedChecks = 0;
            ArrayList<String[]> failedComments = new ArrayList<>();
            TableSchema schema = schemata.get("commit_comments");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    consecutiveFailedChecks++;
                    failedComments.add(nextLine);
                    continue;
                }

                if(consecutiveFailedChecks > 10)
                {
                    System.out.println("Info: " + consecutiveFailedChecks + " consecutive comment lines failed the integrity check in " + Thread.currentThread().getStackTrace()[1] + ". The lines are printed below, starting with the last one that didn't fail.");
                    for(String[] s : failedComments)
                    {
                        for(String t : s) {
                            System.out.print(t + ">|<");
                        }
                        System.out.println();
                    }
                }

                failedComments.clear();
                failedComments.add(nextLine); //last working line
                consecutiveFailedChecks = 0;

                for (int i = 0; i < nextLine.length; i++) {
                    nextLine[i] = groovy.json.StringEscapeUtils.escapeJava(nextLine[i]);
                }
                currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Commentprefix + "commit_") + nextLine[0]) ).append( " a " ).append( getPrefix(TAG_Semangit + "comment") ).append( ";");
                currentTriple.append("\n");
                if(!nextLine[1].equals("N") && !nextLine[1].equals("")){
                    currentTriple.append(getPrefix(TAG_Semangit + "comment_for") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + nextLine[1]) ).append( ";"); //comment for a commit
                    currentTriple.append("\n");
                }
                if(!nextLine[2].equals("N") && !nextLine[2].equals("")) {
                    currentTriple.append(getPrefix(TAG_Semangit + "comment_author") ).append( " " ).append( b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[2]) ).append( ";");
                    currentTriple.append("\n");
                }
                if(!noUserTexts) {
                    if (!nextLine[3].equals("N")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "comment_body")).append(" \"").append(nextLine[3]).append("\";");
                        currentTriple.append("\n");
                    }
                }

                if(!nextLine[4].equals("N"))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "comment_line") ).append( " " ).append( nextLine[4] ).append( ";");
                    currentTriple.append("\n");
                }

                if(!nextLine[5].equals("N"))
                {
                    currentTriple.append(getPrefix(TAG_Semangit + "comment_pos") ).append( " " ).append( nextLine[5] ).append( ";");
                    currentTriple.append("\n");
                }

                currentTriple.append(getPrefix(TAG_Semangit + "comment_created_at") ).append( " \"" ).append( formatDateTime(nextLine[7]) ).append( "\"^^xsd:dateTime.");
                currentTriple.append("\n");
                printTriples(currentTriple.toString(), writers);
                currentTriple.setLength(0);
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void parseIssueComments(String path) {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "issue_comments.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/issue_comments.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/issue_comments.ttl"), 32768));
                    }
                }
            }

            int idCtr = 0;
            String[] nextLine;

            TableSchema schema = schemata.get("issue_comments");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    continue;
                }


                if(useBlankNodes) {
                    //TODO: Let's verify the integrity of the RDF output of this
                    currentTriple.append("[").append(getPrefix(TAG_Semangit + "comment_for")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Issueprefix) + nextLine[0])).append(";"); //comment for an issue
                    currentTriple.append("\n");

                    currentTriple.append(getPrefix(TAG_Semangit + "comment_created_at")).append(" \"").append(nextLine[3]).append("\";"); //TODO: ^^xsd_date stuff
                    currentTriple.append("\n");

                    if (!nextLine[1].equals("") && !nextLine[1].equals("N")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "comment_author")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[1])).append("] a ").append(getPrefix(TAG_Semangit + "comment")).append(".");
                        currentTriple.append("\n");
                    } else {
                        System.out.println("Warning! Invalid user found in parseIssueComments. Using MAX_INT as userID.");
                        currentTriple.append(getPrefix(TAG_Semangit + "comment_author")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + Integer.MAX_VALUE)).append("] a ").append(getPrefix(TAG_Semangit + "comment")).append("."); //TODO Double check this line
                        currentTriple.append("\n");
                    }
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
                else
                {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Issue_Commentprefix) + idCtr++)).append(" a ").append(getPrefix(TAG_Semangit + "comment")).append(";\n");
                    currentTriple.append(getPrefix(TAG_Semangit + "comment_for")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Issueprefix) + nextLine[0])).append(";"); //comment for an issue
                    currentTriple.append("\n");

                    currentTriple.append(getPrefix(TAG_Semangit + "comment_created_at")).append(" \"").append(nextLine[3]).append("\";"); //TODO: ^^xsd_date stuff
                    currentTriple.append("\n");

                    currentTriple.append(getPrefix(TAG_Semangit + "comment_author")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[1])).append(".");
                    currentTriple.append("\n");
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }



    private static void parsePullRequestComments(String path) {
        try {
            CSVReader reader = new CSVReader(new FileReader(path + "pull_request_comments.csv"));
            StringBuilder currentTriple = new StringBuilder();
            ArrayList<BufferedWriter> writers = new ArrayList<>();
            if(sampling < 4) {
                if (samplingPercentages.size() < 2) {
                    writers.add(new BufferedWriter(new FileWriter(path + "rdf/pull_request_comments.ttl"), 32768));
                } else {
                    for (float f : samplingPercentages) {
                        writers.add(new BufferedWriter(new FileWriter(path + "rdf/" + f + "/pull_request_comments.ttl"), 32768));
                    }
                }
            }

            int idCtr = 0;

            String[] nextLine;

            int consecutiveFailedChecks = 0;
            ArrayList<String[]> failedComments = new ArrayList<>();

            TableSchema schema = schemata.get("pull_request_comments");
            while ((nextLine = reader.readNext()) != null) {
                if(brokenLineCheck(schema, nextLine))
                {
                    consecutiveFailedChecks++;
                    failedComments.add(nextLine);
                    continue;
                }

                if(consecutiveFailedChecks > 10)
                {
                    System.out.println("Info: " + consecutiveFailedChecks + " consecutive comment lines failed the integrity check in " + Thread.currentThread().getStackTrace()[1] + ". The lines are printed below, starting with the last one that didn't fail.");
                    for(String[] s : failedComments)
                    {
                        for(String t : s) {
                            System.out.print(t + ">|<");
                        }
                        System.out.println();
                    }
                }

                failedComments.clear();
                failedComments.add(nextLine);
                consecutiveFailedChecks = 0;

                for (int i = 0; i < nextLine.length; i++) {
                    nextLine[i] = groovy.json.StringEscapeUtils.escapeJava(nextLine[i]);
                }

                if(useBlankNodes) {
                    //TODO: Let's verify the integrity of the RDF output of this
                    if (!nextLine[0].equals("")) {
                        currentTriple.append("[").append(getPrefix(TAG_Semangit + "comment_for")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Pullrequestprefix) + nextLine[0])); //comment for a pull request
                        if (!nextLine[5].equals("") && !nextLine[5].equals("N")) {
                            currentTriple.append(",\n").append(getPrefix(TAG_Semangit + "comment_for")).append(" ");
                        }
                        else {
                            currentTriple.append(";\n");
                        }
                    }
                    if (!nextLine[5].equals("") && !nextLine[5].equals("N")) {
                        currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + nextLine[5])).append(";");
                        currentTriple.append("\n");
                    }
                    if (!nextLine[6].equals("") && !nextLine[6].equals("N")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "comment_created_at")).append(" \"").append(nextLine[6]).append("\";"); //TODO ^^xsd:date stuff
                        currentTriple.append("\n");
                    }
                    if (!nextLine[3].equals("") && !nextLine[3].equals("N")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "comment_pos")).append(" ").append(nextLine[3]).append(";");
                        currentTriple.append("\n");
                    }
                    if (!noUserTexts) {
                        if (!nextLine[4].equals("") && !nextLine[4].equals("N")) {

                            currentTriple.append(getPrefix(TAG_Semangit + "comment_body")).append(" \"").append(nextLine[4]).append("\";");
                            currentTriple.append("\n");
                        }
                    }
                    if (!nextLine[1].equals("") && !nextLine[1].equals("N")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "comment_author")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[1])).append("] a ").append(getPrefix(TAG_Semangit + "comment")).append(".");
                        currentTriple.append("\n");
                    } else {
                        System.out.println("Warning! Invalid user found in parsePullRequestComments. Using MAX_INT as userID.");
                        currentTriple.append(getPrefix(TAG_Semangit + "comment_author")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + Integer.MAX_VALUE)).append("] a ").append(getPrefix(TAG_Semangit + "comment")).append("."); //TODO: Also check this line
                        currentTriple.append("\n");
                    }
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);
                }
                else
                {
                    currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Pullrequest_Commentprefix) + idCtr++)).append(" a ").append(getPrefix(TAG_Semangit + "comment")).append(";\n");
                    if (!nextLine[0].equals("")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "comment_for")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Pullrequestprefix) + nextLine[0])); //comment for a pull request
                        if (!nextLine[5].equals("") && !nextLine[5].equals("N")) {
                            currentTriple.append(",\n").append(getPrefix(TAG_Semangit + "comment_for")).append(" ");
                        }
                        else {
                            currentTriple.append(";\n");
                        }
                    }
                    if (!nextLine[5].equals("") && !nextLine[5].equals("N")) {
                        currentTriple.append(b64(getPrefix(TAG_Semangit + TAG_Commitprefix) + nextLine[5])).append(";");
                        currentTriple.append("\n");
                    }
                    if (!nextLine[6].equals("") && !nextLine[6].equals("N")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "comment_created_at")).append(" \"").append(nextLine[6]).append("\";"); //TODO ^^xsd:date stuff
                        currentTriple.append("\n");
                    }
                    if (!nextLine[3].equals("") && !nextLine[3].equals("N")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "comment_pos")).append(" ").append(nextLine[3]).append(";");
                        currentTriple.append("\n");
                    }
                    if (!noUserTexts) {
                        if (!nextLine[4].equals("") && !nextLine[4].equals("N")) {

                            currentTriple.append(getPrefix(TAG_Semangit + "comment_body")).append(" \"").append(nextLine[4]).append("\";");
                            currentTriple.append("\n");
                        }
                    }
                    if (!nextLine[1].equals("") && !nextLine[1].equals("N")) {
                        currentTriple.append(getPrefix(TAG_Semangit + "comment_author")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + nextLine[1])).append(" .");
                        currentTriple.append("\n");
                    } else {
                        System.out.println("Warning! Invalid user found in parsePullRequestComments. Using MAX_INT as userID.");
                        currentTriple.append(getPrefix(TAG_Semangit + "comment_author")).append(" ").append(b64(getPrefix(TAG_Semangit + TAG_Userprefix) + Integer.MAX_VALUE)).append("] a ").append(getPrefix(TAG_Semangit + "comment")).append("."); //TODO: Also check this line
                        currentTriple.append("\n");
                    }
                    printTriples(currentTriple.toString(), writers);
                    currentTriple.setLength(0);

                }
            }
            for(BufferedWriter w : writers)
            {
                w.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }


/*
    private static ArrayList<String> extractConnectedEntities(ArrayList<String> inputTriple)
    {
        ArrayList<String> output = new ArrayList<>();
        for (String s : inputTriple)
        {
            if(s.contains("\""))
            {
                continue;
            }
            //get position of last colon in a row - see if its a resource, i.e. not something like boolean or number. Strings cannot occur here anymore
            int indexColon = s.lastIndexOf(":"); //only interested in objects! Let's check if it is an object or a predicate
            if(s.charAt(indexColon + 1) == ' ') //it's a predicate
            {
                continue;
            }
            int indexSpaceBeforeColon = s.lastIndexOf(' ', indexColon);
            int indexTerminating = s.indexOf(" ", indexColon);
            if(indexTerminating < 0)
            {
                indexTerminating = s.indexOf(".", indexColon);
            }
            if(indexTerminating < 0)
            {
                indexTerminating = s.indexOf(";", indexColon);
            }
            if(indexTerminating < 0)
            {
                indexTerminating = s.indexOf(",", indexColon);
            }
            if(indexTerminating < 0)
            {
                //System.out.println("No entity found in line: " + s);
                continue;
            }
            if(indexTerminating == indexColon + 1)
            {
                //System.out.println("This line is not a reference to another entity: " + s);
                continue;
            }
            String URI = s.substring(indexSpaceBeforeColon + 1, indexTerminating);
            output.add(URI);
        }
        return output;
    }
*/

    /*
    private static String entityToIdentifier(String entity)
    {
        entity = entity.substring(entity.length() -2);
        if(entity.contains(":"))
        {
            if(entity.charAt(0) == ':')
            {
                entity = entity.replace(':', '0');
            }
            else
            {
                entity = "00";
            }
        }
        return entity;
    }
    */

/*    private static void connectedSubgraph(String directory)
    {

        //random file as entry point:
        Random rand = new Random();
        int randomFile = rand.nextInt(63*63);
        String filePath;
        String alphabet64 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_";
        StringBuilder sb = new StringBuilder();
        try {
            int j = (int) Math.ceil(Math.log(randomFile) / Math.log(alphabet64.length()));
            for (int i = 0; i < j; i++) {
                sb.append(alphabet64.charAt(randomFile % alphabet64.length()));
                randomFile /= alphabet64.length();
            }
            filePath = sb.reverse().toString();
            filePath = "rdf/" + filePath.charAt(0) + "/" + filePath.charAt(1) + ".ttl";
        } catch (Exception e) {
            errorCtr++;
            e.printStackTrace();
            return;
        }
        System.out.println("Using random entry point: " + filePath);


        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(directory + "/rdf/subgraph.ttl"), 32768);

            //initialization step. Work on first triple
            BufferedReader br = new BufferedReader(new FileReader(directory + filePath));

            LinkedList<String> toVisit = new LinkedList<>();
            ArrayList<String> currentTriple = new ArrayList<>();
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                currentTriple.add(currentLine);
                if (currentLine.endsWith(".")) {
                    if(extractConnectedEntities(currentTriple).size() > 20) //try to avoid bad entry point
                    {
                        break;
                    }
                    currentTriple.clear();
                }
            }
            //extract other URIs to include in our subgraph
            ArrayList<String> connectedTriples = extractConnectedEntities(currentTriple);
            for(String s : connectedTriples)
            {
                //s = entityToIdentifier(s);
                if(!toVisit.contains(s))
                {
                    toVisit.add(s); //may be slow! HashMap might be better solution
                }
            }


            //Add some "Found good entry point" check
            if(!toVisit.isEmpty()){
                br.close();
            }
            else { //restart with new random entry point
                connectedSubgraph(directory);
                return;
            }

            for(String s : currentTriple)
            {
                writer.write(s);
                writer.newLine();
            }

            //iteration step
            int depth = 0;
            while (!toVisit.isEmpty() && depth < 5) //replace with parameter
            {
                //count which file is most prominent
                HashMap<String, ArrayList<String>> counters = new HashMap<>();
                for(String s : toVisit)
                {
                    //System.out.println("Depth: " + depth + ". toVisit contains: " + s);
                    //we're only interested in last two letters, as they act as hashing value - i.e. the file in which they are stored
                    String id = entityToIdentifier(s);
                    if(!counters.containsKey(id))
                    {
                        ArrayList<String> e = new ArrayList<>();
                        e.add(id);
                        counters.put(id, e);
                    }
                    else //increment by one
                    {
                        //not sure if this actually updates, or if i need to put again
                        counters.get(id).add(s);
                    }
                }

                //we collected all "To be visited" entities and grouped them by the files in which they are stored
                //next up: decide which file to work on next.
                final Set<Map.Entry<String, ArrayList<String>>> entries = counters.entrySet();
                String bestFileIdentifier = "";
                int bestIdentifierCtr = 0;
                //ArrayList<String> bestIdentifiers = new ArrayList<>();
                for(Map.Entry<String, ArrayList<String>> entry : entries)
                {
                    if(entry.getValue().size() > bestIdentifierCtr)
                    {
                        bestIdentifierCtr = entry.getValue().size();
                        bestFileIdentifier = entry.getKey();
                        //bestIdentifiers = entry.getValue();
                        //System.out.println("Setting best file identifier to " + bestFileIdentifier);
                    }
                }
                if(bestIdentifierCtr > 0) {
                    //build regex test string against which to test lines in the file we will search next
                    String testString = "(?:";
                    ArrayList<String> toBeRemoved = new ArrayList<>(); //saves which elements need to be removed from linkedList. Avoids ConcurrentModificationException
                    for (String s : toVisit) {
                        if(s.endsWith(bestFileIdentifier)) {
                            //System.out.println("Removing " + s + " from toVisit.");
                            //we will "visit" all entries stored in this file in the order they are stored. So it's not really a BFS
                            toBeRemoved.add(s);

                            testString += s + "|";
                        }
                    }
                    for(String s : toBeRemoved)
                    {
                        toVisit.remove(s);
                    }
                    testString = testString.substring(0, testString.length() - 1); //remove trailing pipe
                    testString += ").*";
                    //System.out.println("Our regex test string is: " + testString);
                    //open file bestFileIdentifier and extract identifiers from adjacent entities of those in bestIdentifiers.
                    try {
                        depth++;
                        BufferedReader reader = new BufferedReader(new FileReader(directory + "rdf/" + bestFileIdentifier.charAt(0) + "/" + bestFileIdentifier.charAt(1) + ".ttl"));
                        //System.out.println("Now working on file " + directory + "rdf/" + bestFileIdentifier.charAt(0) + "/" + bestFileIdentifier.charAt(1) + ".ttl");
                        //if line starts with any of the identifiers in bestIdentifiers (and then succeeds with a space)...
                        String nextLine;
                        boolean inspectSubject = true;
                        boolean recordingMatchingTriple = false;
                        ArrayList<String> recordedTriple = new ArrayList<>();
                        while((nextLine = reader.readLine()) != null)
                        {
                            if(inspectSubject || recordingMatchingTriple) {
                                if (nextLine.matches(testString)) {
                                    //System.out.println("Found matching entity! " + nextLine);
                                    recordingMatchingTriple = true;
                                    recordedTriple.add(nextLine);
                                    //record this triple and send it to extractor
                                    writer.write(nextLine);
                                    writer.newLine();
                                }
                            }

                            //only inspect next line for entity match, if it starts with a new triple
                            if(nextLine.charAt(nextLine.length() - 1) == '.')
                            {
                                inspectSubject = true;

                                //check if we reached the end of a triple we have been searching for
                                if(recordingMatchingTriple)
                                {
                                    //update toVisit
                                    ArrayList<String> newTriples = extractConnectedEntities(recordedTriple);
                                    for(String s : newTriples)
                                    {
                                        if(!toVisit.contains(s))
                                        {
                                            toVisit.add(s);
                                        }
                                    }
                                    recordedTriple.clear();
                                }
                                recordingMatchingTriple = false;
                            }
                            else
                            {
                                inspectSubject = false;
                            }
                            //System.out.println("inspectSubject = " + inspectSubject + ", ends with: " + nextLine.charAt(nextLine.length() - 1));
                        }
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            }
            writer.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }
*/
    //TODO: This does not belong inside the combined.ttl file, but into a separate void.ttl file. Then we can also add meta info such as number of triples etc.
    private static void printVoID(BufferedWriter w)
    {
        try {
            w.write("@prefix void: <http://rdfs.org/ns/void#> .");
            w.newLine();
            w.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .");
            w.newLine();
            w.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .");
            w.newLine();
            w.write("@prefix owl: <http://www.w3.org/2002/07/owl#> .");
            w.newLine();
            w.write("@prefix dcterms: <http://purl.org/dc/terms/> .");
            w.newLine();
            w.write("@prefix foaf: <http://xmlns.com/foaf/0.1/> .");
            w.newLine();
            //w.write("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .");
            //w.newLine();
            w.write("@prefix wv: <http://vocab.org/waiver/terms/norms> .");
            w.newLine();
            //w.write("@prefix sd: <http://www.w3.org/ns/sparql-service-description#> .");
            //w.newLine();

            w.write("@prefix meta: <#>");
            w.newLine();

            w.write("<> a void:DatasetDescription;");
            w.newLine();
            w.write("dcterms:title \"A VoID description of the SemanGit dataset\";");
            w.newLine();
            w.write("dcterms:creator meta:Dennis;");
            w.newLine();
            w.write("dcterms:creator meta:Matthias;");
            w.newLine();
            w.write("dcterms:creator meta:Damien;");
            w.newLine();
            w.write("foaf:primaryTopic meta:SemanGit .");
            w.newLine();


            w.write("meta:SemanGit a void:Dataset;");
            w.newLine();
            w.write("foaf:homepage <http://semangit.de/>;");
            w.newLine();
            w.write("foaf:page <http://semangit.de/downloads/>;"); //TODO
            w.newLine();
            w.write("dcterms:title \"SemanGit\";");
            w.newLine();
            w.write("dcterms:description \"RDF data extracted from GitHub. More git repository providers to be added.\";");
            w.newLine();
            w.write("dcterms:contributor meta:Dennis;");
            w.newLine();
            w.write("dcterms:contributor meta:Matthias;");
            w.newLine();
            w.write("dcterms:contributor meta:Damien;");
            w.newLine();
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
            Date date = new Date();
            w.write("dcterms:modified \"" + dateFormat.format(date) + "\"^^xsd:date;");
            w.newLine();
            //w.write("dcterms:publisher <>;");
            //w.newLine();

            /*
            w.write("dcterms:license <>;");
            w.newLine();
            w.write("wv:norms <>;");
            w.newLine();
            w.write("wv:waiver <>;");
            w.newLine();
            */

            w.write("dcterms:subject <http://dbpedia.org/resource/Computer_Science>;"); //todo add more
            w.newLine();
            w.write("void:feature <http://www.w3.org/ns/formats/Turtle>;");
            w.newLine();

            //w.write("void:sparqlEndpoint <http://semangit.de/sparql>;");
            //w.newLine();

            //w.write("void:dataDump <http://semangit.de/latest/>;"); //todo
            //w.newLine();

            w.write("void:exampleResource <>;");
            w.newLine();
            w.write("void:exampleResource <>;");
            w.newLine();
            w.write("void:exampleResource <>;");
            w.newLine();

            w.write("void:uriSpace \"http://semangit.de/\";");
            w.newLine();

            //void:vocabulary -- give URIs of all vocabularies used?!

            /*
            w.write("void:triples ;");
            w.newLine();
            w.write("void:entities ;");
            w.newLine();
            w.write("void:classes ;");
            w.newLine();
            w.write("void:properties ;");
            w.newLine();
            w.write("void:distinctSubjects ;");
            w.newLine();
            w.write("void:distinctObjects ;");
            w.newLine();
            */

            w.write(" .");
            w.newLine();

            w.write("meta:Dennis a foaf:Person;");
            w.newLine();
            w.write("rdfs:label \"Dennis Oliver Kubitza\";");
            w.newLine();
            w.write("foaf:mbox <mailto:dennis.kubitza@iais.fraunhofer.de> .");
            w.newLine();

            w.write("meta:Matthias a foaf:Person;");
            w.newLine();
            w.write("rdfs:label \"Matthias Böckmann\";");
            w.newLine();
            w.write("foaf:mbox <mailto:matthias.boeckmann@iais.fraunhofer.de> .");
            w.newLine();

            w.write("meta:Damien a foaf:Person;");
            w.newLine();
            w.write("rdfs:label \"Damien Graux\";");
            w.newLine();
            w.write("foaf:mbox <mailto:damien.graux@iais.fraunhofer.de> .");
            w.newLine();


        }
        catch (IOException e)
        {
            e.printStackTrace();
            errorCtr++;
        }
        //not closing w, as we still need it afterwards
    }


    private static void appendFileToOutput(String directory, String fileName)
    {
        if(samplingPercentages.size() < 2) {
            String outPath = directory.concat("combined.ttl");
            File index = new File(outPath);
            if (!index.exists()) {
                try {
                    BufferedWriter writer = new BufferedWriter(new FileWriter(outPath), 32768);
                    final Set<Map.Entry<String, String>> entries = prefixTable.entrySet();
                    printVoID(writer);
                    writer.write("@prefix semangit: <http://semangit.de/ontology/>.");
                    for (Map.Entry<String, String> entry : entries) {
                        writer.write("@prefix " + entry.getValue() + ": <http://semangit.de/ontology/" + entry.getKey() + "#>.");
                        writer.newLine();
                    }
                    writer.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            try (BufferedReader br = new BufferedReader(new FileReader(directory.concat(fileName)))) {
                BufferedWriter output = new BufferedWriter(new FileWriter(outPath, true));
                for (String line; (line = br.readLine()) != null; ) {
                    output.append(line);
                    output.newLine();
                }
                output.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        else {
            for(float f : samplingPercentages)
            {
                String outPath = directory.concat(f + "/combined.ttl");
                File index = new File(outPath);
                if (!index.exists()) {
                    try {
                        BufferedWriter writer = new BufferedWriter(new FileWriter(outPath), 32768);
                        final Set<Map.Entry<String, String>> entries = prefixTable.entrySet();
                        writer.write("@prefix semangit: <http://semangit.de/ontology/>.");
                        printVoID(writer);
                        for (Map.Entry<String, String> entry : entries) {
                            writer.write("@prefix " + entry.getValue() + ": <http://semangit.de/ontology/" + entry.getKey() + "#>.");
                            writer.newLine();
                        }
                        writer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
                try (BufferedReader br = new BufferedReader(new FileReader(directory.concat(f + "/" + fileName)))) {
                    BufferedWriter output = new BufferedWriter(new FileWriter(outPath, true));
                    for (String line; (line = br.readLine()) != null; ) {
                        output.append(line);
                        output.newLine();
                    }
                    output.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    private Converter(String workOnFile, String path)
    {
        this.workOnFile = workOnFile;
        this.path = path;
    }

    public void run()
    {
        if(this.workOnFile == null)
        {
            throw(new RuntimeException("You need to define a file to work on!"));
        }
        switch ( workOnFile )
        {
            case "commit_comments": parseCommitComments(this.path);break;
            case "commit_parents": parseCommitParents(this.path);break;
            case "commits": parseCommits(this.path);break;
            case "followers": parseFollowers(this.path);break;
            case "issue_comments": parseIssueComments(this.path);break;
            case "issue_events": parseIssueEvents(this.path);break;
            case "issue_labels": parseIssueLabels(this.path);break;
            case "issues": parseIssues(this.path);break;
            case "organization_members": parseOrganizationMembers(this.path);break;
            case "project_commits": parseProjectCommits(this.path);break;
            case "project_languages": parseProjectLanguages(this.path);break;
            case "project_members": parseProjectMembers(this.path);break;
            case "projects": parseProjects(this.path);break;
            case "pull_request_comments": parsePullRequestComments(this.path);break;
            case "pull_request_commits": parsePullRequestCommits(this.path);break;
            case "pull_request_history": parsePullRequestHistory(this.path);break;
            case "pull_requests": parsePullRequests(this.path);break;
            case "users": parseUsers(this.path);break;
            case "repo_labels": parseRepoLabels(this.path);break;
            case "repo_milestones":parseRepoMilestones(this.path);break;
            case "watchers":parseWatchers(this.path);break;
            default: throw new RuntimeException("Unknown file name specified! Which file to parse?!");
        }
        //System.out.println("Finished working on " + this.workOnFile);
    }

    public static void main(String[] args)
    {
        try (Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withClusterName("SemanGit Cluster").build())
        {
            Session session = cluster.connect();
            ResultSet rs = session.execute("select release_version from system.local");
            Row row = rs.one();
            System.out.println(row.getString("release_version"));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        if(args.length > 1)
        {
            boolean percentageGiven = false;
            boolean samplingGiven = false;
            //mode, prefixing
            for(String s: args)
            {
                s = s.toLowerCase();
                if(s.equals("-noprefix"))
                {
                    System.out.println("Prefixing disabled! Large output expected...");
                    prefixing = false;
                }
                else if(s.contains("-base="))
                {
                    String rightOfEql = s.substring(s.lastIndexOf("=") + 1);
                    int in = Integer.parseInt(rightOfEql);
                    if(in == 64)
                    {
                        System.out.println("Using Base64URL-like representation for integers.");
                        mode = 0;
                    }
                    else if(in == 32)
                    {
                        System.out.println("Using Base32 representation for integers.");
                        mode = 1;
                    }
                    else if(in == 16)
                    {
                        System.out.println("Using Base16 representation for integers.");
                        mode = 2;
                    }
                    else if(in == 10)
                    {
                        System.out.println("Using Base10 representation for integers.");
                        mode = 3;
                    }
                    else
                    {
                        System.out.println("Unknown base passed as argument. Using Base64URL-like representation for integers.");
                        mode = 0;
                    }
                }
                else if(s.contains("-debug"))
                {
                    debug = true;
                    System.out.println("Debug mode enabled.");
                }
                else if(s.contains("-sampling="))
                {
                    String rightOfEql = s.substring(s.lastIndexOf("=") + 1);
                    samplingGiven = true;
                    switch (rightOfEql)
                    {
//                        case "head": sampling = 1;break;
//                        case "tail": sampling = 2;break;
                        case "random": sampling = 3;break;
//                        case "connected": sampling = 4;mergeOutput=false;noUserTexts=true;break;
//                        case "bfs": sampling = 5;mergeOutput=false;noUserTexts=true;break;
                        default: sampling = 0; samplingGiven = false;
                            System.out.println("Invalid sampling method. Only available sampling method is random sampling. Using no sampling.");
                    }
                    if(sampling != 0 && sampling != 3)
                    {
                        System.out.println("The selected sampling method is not available via the parser anymore. Head and Tail sampling are done in bash, graph sampling with a different java class.");
                        System.exit(1);
                    }
                }
                else if(s.contains("-percentage=")) //can be used multiple times so we generate multiple samples in one go
                {
                    String rightOfEql = s.substring(s.lastIndexOf("=") + 1);

                    try{
                        samplingPercentages.add(Float.parseFloat(rightOfEql));
                        if(samplingPercentages.get(samplingPercentages.size() - 1) > 0 && samplingPercentages.get(samplingPercentages.size() -1) <= 1)
                        {
                            percentageGiven = true;
                        }
                        else {
                            System.out.println("Percentage needs to be (0, 1]");
                            System.exit(1);
                        }
                    }
                    catch (NumberFormatException e){
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
                else if(s.contains("-nomerging"))
                {
                    mergeOutput = false;
                }
                else if(s.contains("-nostrings"))
                {
                    noUserTexts = true;
                }
                else if(s.contains("-noblank"))
                {
                    useBlankNodes = false;
                    //TODO to be documented
                }
                else
                {
                    if(!s.equals(args[0].toLowerCase()))
                    {
                        System.out.println("Unknown parameter: " + s);
                        System.out.println("Usage: java -jar converter_main.jar <path/to/input/directory> [-base=64|32|16|10] [-noprefix] [-sampling=random] [-percentage=X] [-nomerging] [-nostrings]");
                        System.out.println("Note that percentage parameter is obligatory, if sampling method is set to random. Otherwise it will be ignored.");
                    }
                }
            }
            if(samplingGiven && !percentageGiven)
            {
                System.out.println("Please specify the sampling percentage with -percentage=X");
                System.exit(1);
            }
            if(samplingPercentages.size() > 1)
            {
                //multiple samples to be generated. need to organize in sub-folders
                for(float f : samplingPercentages)
                {
                    File index = new File(args[0] + "rdf/" + f);
                    if (!index.exists() && !index.mkdirs()) {
                        System.out.println("Unable to create " + args[0] + "rdf/" + f + "/ directory. Exiting.");
                        System.exit(1);
                    }
                }
            }
            if(!prefixing && (mode != 3))
            {
                System.out.println("No Prefixing option is only available for base10. Setting base to 10.");
                mode = 3;
            }
        }
        try {
            numTriples = new AtomicInteger(0);
            File index = new File(args[0] + "rdf");
            if (!index.exists() && !index.mkdirs()) {
                System.out.println("Unable to create " + args[0] + "rdf/ directory. Exiting.");
                System.exit(1);
            }
            rdfPath = args[0] + "rdf/";
            initPrefixTable();
            parseSQLSchema(args[0]);

            System.out.println();

            ArrayList<Thread> processes = new ArrayList<>();
            processes.add(new Thread(new Converter("project_commits", args[0])));
            processes.add(new Thread(new Converter("commit_comments", args[0])));
            processes.add(new Thread(new Converter("commit_parents", args[0])));
            processes.add(new Thread(new Converter("commits", args[0])));
            processes.add(new Thread(new Converter("followers", args[0])));
            processes.add(new Thread(new Converter("issue_comments", args[0])));
            processes.add(new Thread(new Converter("issue_events", args[0])));
            processes.add(new Thread(new Converter("issue_labels", args[0])));
            processes.add(new Thread(new Converter("issues", args[0])));
            processes.add(new Thread(new Converter("organization_members", args[0])));
            processes.add(new Thread(new Converter("project_members", args[0])));
            processes.add(new Thread(new Converter("project_languages", args[0])));
            processes.add(new Thread(new Converter("projects", args[0])));
            processes.add(new Thread(new Converter("pull_request_comments", args[0])));
            processes.add(new Thread(new Converter("pull_request_commits", args[0])));
            processes.add(new Thread(new Converter("pull_request_history", args[0])));
            processes.add(new Thread(new Converter("pull_requests", args[0])));
            processes.add(new Thread(new Converter("users", args[0])));
            processes.add(new Thread(new Converter("repo_labels", args[0])));
            processes.add(new Thread(new Converter("repo_milestones", args[0])));
            processes.add(new Thread(new Converter("watchers", args[0])));
            for (Thread t : processes) {
                try {
                    t.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    errorCtr++;
                }
            }
            for (Thread t : processes) {
                try {
                    t.join();
                } catch (Exception e) {
                    e.printStackTrace();
                    errorCtr++;
                }
            }

            String correctPath = args[0].concat("rdf/");
            if (mergeOutput) {
                appendFileToOutput(correctPath, "project_commits.ttl");
                appendFileToOutput(correctPath, "commit_comments.ttl");
                appendFileToOutput(correctPath, "commits.ttl");
                appendFileToOutput(correctPath, "commit_parents.ttl");
                appendFileToOutput(correctPath, "issue_comments.ttl");
                appendFileToOutput(correctPath, "pull_request_comments.ttl");
                appendFileToOutput(correctPath, "issue_events.ttl");
                appendFileToOutput(correctPath, "issues.ttl");
                appendFileToOutput(correctPath, "project_members.ttl");
                appendFileToOutput(correctPath, "project_languages.ttl");
                appendFileToOutput(correctPath, "projects.ttl");
                appendFileToOutput(correctPath, "pull_request_history.ttl");
                appendFileToOutput(correctPath, "pull_request_commits.ttl");
                appendFileToOutput(correctPath, "pull_requests.ttl");
                appendFileToOutput(correctPath, "repo_labels.ttl");
                appendFileToOutput(correctPath, "repo_milestones.ttl");
                appendFileToOutput(correctPath, "issue_labels.ttl");
                appendFileToOutput(correctPath, "watchers.ttl");
                appendFileToOutput(correctPath, "organization_members.ttl");
                appendFileToOutput(correctPath, "followers.ttl");
                appendFileToOutput(correctPath, "users.ttl");

                if (samplingPercentages.size() > 1) {
                    for (float f : samplingPercentages) {
                        File index2 = new File(args[0] + "/rdf/" + f);

                        if (index2.exists()) {
                            String[] entries = index2.list();
                            if (entries != null) {
                                for (String s : entries) {
                                    File currentFile = new File(index2.getPath(), s);
                                    if (s.equals("combined.ttl")) {
                                        continue;
                                    }
                                    if (!currentFile.delete()) {
                                        System.out.println("Failed to delete existing file: " + index2.getPath() + s);
                                        System.exit(1);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    if (index.exists()) {
                        String[] entries = index.list();
                        if (entries != null) {
                            for (String s : entries) {
                                File currentFile = new File(index.getPath(), s);
                                if (s.equals("combined.ttl")) {
                                    continue;
                                }
                                if (!currentFile.delete()) {
                                    System.out.println("Failed to delete existing file: " + index.getPath() + s);
                                    System.exit(1);
                                }
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        if(errorCtr != 0)
        {
            System.out.println("A total of " + errorCtr + " errors occurred.");
            System.exit(2);
        }

        //Statistics: Printing prefixing statistics to optimize space usage of future runs
        //System.out.println("All processing executed successfully. Now printing statistics.");
        final Set<Map.Entry<String, TableSchema>> schemaEntries = schemata.entrySet();
        for(Map.Entry<String, TableSchema> t : schemaEntries)
        {
            if(t.getValue().integrityChecksNeg * 10 > t.getValue().integrityChecksPos + t.getValue().integrityChecksNeg) //
            {
                System.out.println("WARNING! Schema for: " + t.getKey() + ". Pos: " + t.getValue().integrityChecksPos + ". Neg: " + t.getValue().integrityChecksNeg + ". Nullability: " + t.getValue().nullabilityFails);
            }
        }
//      System.out.println("Integrity Checks done: " + integrityCheckNeg + integrityCheckPos + ". Pos: " + integrityCheckPos + ", Neg: " + integrityCheckNeg);
        if(debug) {
            System.out.println("Line rejection percentage (integrity check) per table: ");
            for(Map.Entry<String, TableSchema> t : schemaEntries)
            {
                if(t.getValue().integrityChecksNeg + t.getValue().integrityChecksPos == 0)
                {
                    System.out.println(t.getKey() + ": No data found.");
                }
                else
                {
                    System.out.println(t.getKey() + ": " + ((double)(t.getValue().integrityChecksNeg) / (t.getValue().integrityChecksPos + t.getValue().integrityChecksNeg)));
                }
            }
            System.out.println("Now printing statistics for prefix usage for further optimization.");
            final Set<Map.Entry<String, Integer>> prefixCtrs = prefixCtr.entrySet();
            for (Map.Entry<String, Integer> entry : prefixCtrs) {
                System.out.println("URI: " + entry.getKey() + " -- Used Prefix: " + prefixTable.get(entry.getKey()) + " -- Counter: " + entry.getValue());
            }
            System.out.println("Num triples: " + numTriples.get());
        }
        System.exit(0);

    }
}
