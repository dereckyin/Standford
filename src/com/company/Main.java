package com.company;
import java.io.*;
import java.sql.*;
import java.util.*;

import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.PreparedStatement;
import org.json.*;

public class Main {

    private static final String basedir = System.getProperty("SegDemo", "data");
    private static CRFClassifier<CoreLabel> segmenter = null;

    private static Connection conReader = null;
    private static Statement stReader = null;

    private static Connection conWriter = null;
    private static Statement stWriter = null;

    private static String strSkipWord = ", . ; + - | / \\ ' \" : ? < > [ ] { } ! @ # $ % ^ & * ( ) ~ ` _ － ‐ ， （ ）";
    private static String[] SkipWord;

    private static String ReaderUrl = null;
    private static String ReaderUser = null;
    private static String ReaderPwd = null;

    private static String WriterUrl = null;
    private static String WriteUser = null;
    private static String WriterPwd = null;

    private static String SelectSql = null;

    private static String WriteMsSql = null;

    public static void main(String[] args) {


        loadParams();

        System.out.println(SelectSql);

        if(SelectSql.isEmpty())
            System.exit(0);


        SkipWord = strSkipWord.split(" ");

        initCoreLabelCRFClassifier();

        connectPostgrel();

        long startTime = System.nanoTime();

        selectPostgrel();

        closePostgrel();

        long endTime = System.nanoTime();

        long duration = (endTime - startTime);

        System.out.println("Excution Time: " + duration/1000000000 + " seconds");
    }

    protected static void ProcessData(String pid, String pn, String mfs, String catalog, String description, String param)
    {
        Map<String, String> scoreMap = null;


        // 清除雜訊
        if(description != null && !description.isEmpty())
            description.replaceAll("[\"\']", "");
        if(param != null && !param.isEmpty())
            param.replaceAll("[\"\']", "");

        // 料號
        //scoreMap = segmentData(pn);

        // 料號需有完整紀錄
        //if(!scoreMap.containsKey(pn))
        //{
        InsertPostgrel(pn.toUpperCase(),
                Integer.parseInt(pid),
                1,
                0, pn, mfs, catalog, pn);
        //}

        //InsertAllWord(pid, 0, pn, mfs, catalog, scoreMap);

        // mfs
        scoreMap = segmentData(mfs);
        InsertAllWord(pid, 1, pn, mfs, catalog, scoreMap);

        // catalog
        scoreMap = segmentDataCatalog(catalog);
        InsertAllWord(pid, 2, pn, mfs, catalog, scoreMap);

        // description
        scoreMap = segmentDataDDesc(description);
        InsertAllWord(pid, 3, pn, mfs, catalog, scoreMap);

        // param
        scoreMap = segmentDataParam(param);
        InsertAllWord(pid, 4, pn, mfs, catalog, scoreMap);
    }

    protected static void InsertAllWord(String pid, int kind, String pn, String mfs, String catalog, Map<String, String> scoreMap) {
        Iterator it = scoreMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            //System.out.println(pair.getKey() + " = " + pair.getValue());

            String [] token = pair.getValue().toString().split(",");

            float token1 = 0.0f;

            try{
                token1 = Float.parseFloat(token[0]);
            }
            catch (NumberFormatException e)
            {

            }

            if(token != null && token.length == 2) {

                InsertPostgrel(pair.getKey().toString(),
                        Integer.parseInt(pid),
                        token1,
                        kind, pn, mfs, catalog, token[1]);
            }

            it.remove(); // avoids a ConcurrentModificationException
        }
    }

    protected static boolean SkipWord(String strIn)
    {
        boolean bHave = false;

        for(String str : SkipWord)
        {
            if (strIn.trim().equalsIgnoreCase(str))
                bHave = true;
        }

        return bHave;
    }


    protected static int CountStringOccurrences(String text, String pattern)
    {
        // Loop through all instances of the string 'text'.
        int count = 0;
        int i = 0;
        while ((i = text.indexOf(pattern, i)) != -1)
        {
            i += pattern.length();
            count++;
        }
        return count;
    }

    public static void loadParams() {
        Properties props = new Properties();
        InputStream is = null;

        // First try loading from the current directory
        try {
            File f = new File("config.properties");
            is = new FileInputStream( f );
        }
        catch ( Exception e ) { is = null; }

        try {
            if ( is == null ) {
                // Try loading from classpath
                is = Main.class.getClassLoader().getResourceAsStream("config.properties");
            }

            // Try loading properties from the file (if found)
            props.load( is );
        }
        catch ( Exception e ) { }

        ReaderUrl = props.getProperty("ReaderUrl", "");
        ReaderUser = props.getProperty("ReaderUser", "");
        ReaderPwd  = props.getProperty("ReaderPwd", "");

        WriterUrl = props.getProperty("WriterUrl", "");
        WriteUser = props.getProperty("WriteUser", "");
        WriterPwd  = props.getProperty("WriterPwd", "");

        SelectSql = props.getProperty("SelectSql", "");

        WriteMsSql = props.getProperty("WriteMsSql", "0");
    }

    protected static Map<String, String> segmentDataCatalog(String strData)
    {
        String [] strFullword = null;

        List<String> sList = new ArrayList<String>();
        List<String> sFullword = new ArrayList<String>();

        Map<String, String> scoreMap = new HashMap<String, String>();

        String val = null;

        val = strData;


        if (val != null) {

            val = val.toUpperCase();

            val = val.trim();

            val = val.replace("，", " ");

            val = val.replace(">", " ");

            strFullword = val.split(" ");

            if(strFullword != null) {
                for (String stoken : strFullword) {



                    stoken = stoken.replace(" ", "");

                    if (SkipWord(stoken) || stoken.length() == 0)
                        continue;


                    if (stoken.trim() == "")
                        continue;

                    //InsertPostgrel(stoken, Integer.parseInt(pid), 1, 4, pn, mfs, catalog, val);
                    sList.add(stoken);
                    sFullword.add(val);
                }
            }

        }


        for(int i=0; i<sList.size(); i++)
        {
            float weight = 0.0f;

            weight = (float)similarity(sList.get(i), sFullword.get(i));


            if(scoreMap.containsKey(sList.get(i).toUpperCase()))
            {

                String sValue = scoreMap.get(sList.get(i).toUpperCase());
                String [] token = sValue.split(",");

                double score = Double.parseDouble(token[0]);

                // 取最大值
                if(score < weight)
                    score = weight;

                String s = Double.toString(score);

                if(s.length() > 4)
                    s = s.substring(0, 4);

                s += "," + sFullword.get(i);

                scoreMap.put(sList.get(i).toUpperCase(), s);

            }
            else {
                String s = Float.toString(weight) + "," + sFullword.get(i);
                scoreMap.put(sList.get(i).toUpperCase(), s);
            }
        }

        return scoreMap;

    }

    protected static Map<String, String> segmentDataDDesc(String strData)
    {
        String [] strFullword = null;

        List<String> sList = new ArrayList<String>();
        List<String> sFullword = new ArrayList<String>();

        Map<String, String> scoreMap = new HashMap<String, String>();

        String val = null;

        val = strData;


        if (val != null) {

            val = val.toUpperCase();

            val = val.trim();

            val = val.replace("，", " ");


            strFullword = val.split(" ");

            if(strFullword != null) {
                for (String stoken : strFullword) {


                    stoken = stoken.replace(" ", "");

                    if (SkipWord(stoken) || stoken.length() == 0)
                        continue;

                    if (stoken.trim() == "")
                        continue;

                    //InsertPostgrel(stoken, Integer.parseInt(pid), 1, 4, pn, mfs, catalog, val);
                    sList.add(stoken);
                    sFullword.add(val);
                }
            }

        }


        for(int i=0; i<sList.size(); i++)
        {
            float weight = 0.0f;

            weight = (float)similarity(sList.get(i), sFullword.get(i));


            if(scoreMap.containsKey(sList.get(i).toUpperCase()))
            {

                String sValue = scoreMap.get(sList.get(i).toUpperCase());
                String [] token = sValue.split(",");

                double score = Double.parseDouble(token[0]);

                // 取最大值
                if(score < weight)
                    score = weight;

                String s = Double.toString(score);

                if(s.length() > 4)
                    s = s.substring(0, 4);

                s += "," + sFullword.get(i);

                scoreMap.put(sList.get(i).toUpperCase(), s);

            }
            else {
                String s = Float.toString(weight) + "," + sFullword.get(i);
                scoreMap.put(sList.get(i).toUpperCase(), s);
            }
        }

        return scoreMap;

    }

    protected static Map<String, String> segmentDataParam(String strData)
    {
        String [] strFullword = null;

        List<String> sList = new ArrayList<String>();
        List<String> sFullword = new ArrayList<String>();

        Map<String, String> scoreMap = new HashMap<String, String>();

        if(strData != null && !strData.isEmpty()) {

            try {
                JSONObject json = new JSONObject(strData);

                Iterator<String> keys = json.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    String val = null;
                    try {
                        val = json.getString(key);
                    } catch (Exception e) {
                        System.out.println(e.toString());
                    }

                    if (val != null) {

                        val = val.toUpperCase();

                        val = val.trim();

                        val = val.replace("，", " ");

                        if (val.contains("HTTP"))
                            continue;

                        if (val.contains("PDF"))
                            continue;

                        strFullword = val.split(" ");

                        if (strFullword != null) {
                            for (String stoken : strFullword) {



                                stoken = stoken.replace(" ", "");

                                if (SkipWord(stoken) || stoken.length() == 0)
                                    continue;

                                if (stoken.trim() == "")
                                    continue;

                                //InsertPostgrel(stoken, Integer.parseInt(pid), 1, 4, pn, mfs, catalog, val);
                                sList.add(stoken);
                                sFullword.add(val);
                            }
                        }

                    }


                }
            } catch (JSONException e) {
                return scoreMap;
            }

            for (int i = 0; i < sList.size(); i++) {
                float weight = 0.0f;

                weight = (float) similarity(sList.get(i), sFullword.get(i));


                if (scoreMap.containsKey(sList.get(i).toUpperCase())) {

                    String sValue = scoreMap.get(sList.get(i).toUpperCase());
                    String[] token = sValue.split(",");

                    double score = 0.0;
                    try{
                        score = Double.parseDouble(token[0]);
                    }
                    catch (NumberFormatException e)
                    {}

                    // 取最大值
                    if (score < weight)
                        score = weight;

                    String s = Double.toString(score);

                    if (s.length() > 4)
                        s = s.substring(0, 4);

                    s += "," + sFullword.get(i);

                    scoreMap.put(sList.get(i).toUpperCase(), s);

                } else {
                    String s = Float.toString(weight) + "," + sFullword.get(i);
                    scoreMap.put(sList.get(i).toUpperCase(), s);
                }
            }
        }

        return scoreMap;

    }

    protected static double similarity(String s1, String s2) {
        String longer = s1, shorter = s2;
        if (s1.length() < s2.length()) { // longer should always have greater length
            longer = s2; shorter = s1;
        }
        int longerLength = longer.length();
        if (longerLength == 0) { return 1.0; /* both strings are zero length */ }
        return (longerLength - editDistance(longer, shorter)) / (double) longerLength;
    }

    protected static int editDistance(String s1, String s2) {
        s1 = s1.toLowerCase();
        s2 = s2.toLowerCase();

        int[] costs = new int[s2.length() + 1];
        for (int i = 0; i <= s1.length(); i++) {
            int lastValue = i;
            for (int j = 0; j <= s2.length(); j++) {
                if (i == 0)
                    costs[j] = j;
                else {
                    if (j > 0) {
                        int newValue = costs[j - 1];
                        if (s1.charAt(i - 1) != s2.charAt(j - 1))
                            newValue = Math.min(Math.min(newValue, lastValue),
                                    costs[j]) + 1;
                        costs[j - 1] = lastValue;
                        lastValue = newValue;
                    }
                }
            }
            if (i > 0)
                costs[s2.length()] = lastValue;
        }
        return costs[s2.length()];
    }


    protected static Map<String, String> segmentData(String strData)
    {
        String [] strFullword = null;

        List<String> sList = new ArrayList<String>();
        List<String> sFullword = new ArrayList<String>();

        Map<String, String> scoreMap = new HashMap<String, String>();

        String delimiters = "[\\p{Punct}\\s]+";

        if(strData != null && !strData.isEmpty())
            strFullword = strData.split(delimiters);

        if(strFullword != null) {
            for (String stoken : strFullword) {

                if (SkipWord(stoken) || stoken.length() == 0)
                    continue;

                List<String> segmented = segmenter.segmentString(stoken);
                //System.out.println(segmented);


                if(segmented != null) {
                    for (String element : segmented) {

                        if (SkipWord(element) || element.length() == 0)
                            continue;

                        sList.add(element);
                        sFullword.add(stoken);
                        //System.out.println(element);
                    }
                }
            }
        }

        for(int i=0; i<sList.size(); i++)
        {
            float weight = 0.0f;

            int count = CountStringOccurrences(sFullword.get(i), sList.get(i));

            try {
                weight = (float) sList.get(i).length() / (float) sFullword.get(i).length() * count;
            }
            catch (Exception ex)
            {
                Logger lgr = Logger.getLogger(Main.class.getName());
                lgr.log(Level.SEVERE, ex.getMessage(), ex);

                weight = 0.01f;
            }

            if(weight > 1)
                weight = 0.8f;


            if(scoreMap.containsKey(sList.get(i).toUpperCase()))
            {

                String sValue = scoreMap.get(sList.get(i).toUpperCase());
                String [] token = sValue.split(",");

                double score = Double.parseDouble(token[0]);

                score += weight;

                if(score > 1.0f)
                    score = Math.sqrt(score);

                String s = Double.toString(score);

                if(s.length() > 4)
                    s = s.substring(0, 4);

                s += "," + sFullword.get(i);

                scoreMap.put(sList.get(i).toUpperCase(), s);

            }
            else {
                String s = Float.toString(weight) + "," + sFullword.get(i);
                scoreMap.put(sList.get(i).toUpperCase(), s);
            }
        }

        return scoreMap;
    }

    protected static void InsertPostgrel(String word, int page, float weight, int kind, String pn, String mfs, String catalog, String fullword)
    {
        PreparedStatement pst = null;

        fullword = fullword.substring(0, Math.min(fullword.length(), 60));
        try {

            String strSql = "INSERT INTO qeindex(word, page, weight, kind, pn, mfs, catalog, fullword) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
            pst = conWriter.prepareStatement(strSql);
            pst.setString(1, word);
            pst.setInt(2, page);
            pst.setFloat(3, weight);
            pst.setInt(4, kind);
            pst.setString(5, pn);
            pst.setString(6, mfs);
            pst.setString(7, catalog);
            pst.setString(8, fullword);
            pst.executeUpdate();

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Main.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
            System.out.println(page);
            System.out.println(word);
            System.out.println(mfs);
            System.out.println(catalog);
            System.out.println(fullword);
        } finally {

            try {
                if (pst != null) {
                    pst.close();
                }


            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(Main.class.getName());
                lgr.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
    }

    protected static void closePostgrel()
    {
        try {

            if(!stReader.isClosed())
                stReader.close();
            if(!conReader.isClosed())
                conReader.close();

            if(!stWriter.isClosed())
                stWriter.close();
            if(!conWriter.isClosed())
                conWriter.close();


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected  static void selectPostgrel()
    {
        String pid, pn, mfs, catalog, description, param;

        try
        {
            int i = 0;
            stReader.setFetchSize(50);
            ResultSet rs = stReader.executeQuery(SelectSql);
            while (rs.next()) {
                pid = rs.getString(1);
                pn = rs.getString(2);
                mfs = rs.getString(3);
                catalog = rs.getString(4);
                description = rs.getString(5);
                param = rs.getString(6);

                // for debug
                /*
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
                System.out.println(rs.getString(3));
                System.out.println(rs.getString(4));
                System.out.println(rs.getString(5));
                System.out.println(rs.getString(6));
                */

                ProcessData(pid, pn, mfs, catalog, description, param);

                System.out.println(rs.getString(1) + " done");
            }
            rs.close();

        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    protected static Connection getOracleConnection() throws Exception {
        String driver = "oracle.jdbc.driver.OracleDriver";
        String url = "jdbc:oracle:thin:@localhost:1521:scorpian";
        String username = "userName";
        String password = "pass";
        Class.forName(driver); // load Oracle driver
        Connection conn = DriverManager.getConnection(url, username, password);
        return conn;
    }

    protected static Connection getMySqlConnection() throws Exception {
        String driver = "org.gjt.mm.mysql.Driver";
        String url = "jdbc:mysql://localhost/tiger";
        String username = "root";
        String password = "root";
        Class.forName(driver); // load MySQL driver
        Connection conn = DriverManager.getConnection(url, username, password);
        return conn;
    }

    protected static Connection getMSSqlConnection() throws Exception {

        String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

        String url  = WriterUrl + ";user=" + WriteUser + ";password=" + WriterPwd;

        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url);
        return conn;
    }

    protected static Connection getPgSqlConnection() throws Exception {
        String driver = "org.postgresql.Driver";
        String url = ReaderUrl;
        String username = ReaderUser;
        String password = ReaderPwd;
        Class.forName(driver); // load MySQL driver
        Connection conn = DriverManager.getConnection(url, username, password);
        return conn;
    }

    protected static void connectPostgrel(){
        try

        {

            String url = "";
            conReader = getPgSqlConnection();

            conReader.setAutoCommit(false);
            stReader = conReader.createStatement();

            if(WriteMsSql.equalsIgnoreCase("0")) {
                url = WriterUrl;  //連線ip ,port ,table name
                conWriter = DriverManager.getConnection(url, WriteUser, WriterPwd); //帳號密碼

                stWriter = conWriter.createStatement();
            }
            else
            {
                conWriter = getMSSqlConnection();
                stWriter = conWriter.createStatement();
            }


        } catch (Exception ee)

        {
            System.out.print(ee.getMessage());

        }
    }




    protected static void initCoreLabelCRFClassifier() {
        try {
            System.setOut(new PrintStream(System.out, true, "utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        Properties props = new Properties();
        props.setProperty("sighanCorporaDict", basedir);
        // props.setProperty("NormalizationTable", "data/norm.simp.utf8");
        // props.setProperty("normTableEncoding", "UTF-8");
        // below is needed because CTBSegDocumentIteratorFactory accesses it
        props.setProperty("serDictionary", basedir + "/dict-chris6.ser.gz");


        props.setProperty("inputEncoding", "UTF-8");
        props.setProperty("sighanPostProcessing", "true");

        segmenter = new CRFClassifier<CoreLabel>(props);
        segmenter.loadClassifierNoExceptions(basedir + "/ctb.gz", props);


    }
}
