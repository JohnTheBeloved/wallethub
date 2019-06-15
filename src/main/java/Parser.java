import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

public class Parser {

    private File accessLogFile;
    private Properties dbProperties;
    private Date startDate;
    private Date endDate;
    private String duration;
    private Integer threshold;
    private static final SimpleDateFormat parameterFormat = new SimpleDateFormat("yyyy-MM-dd'.'HH:mm:ss");
    private static final SimpleDateFormat logFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Parser(File accessLogFile, Properties dbProperties, Date startDate, Date endDate, String duration,
            Integer threshold) {
        this.dbProperties = dbProperties;
        this.accessLogFile = accessLogFile;
        this.startDate = startDate;
        this.endDate = endDate;
        this.duration = duration;
        this.threshold = threshold;
    }

    public void parse()
            throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {

        MySqlUtil.loadClass();
        MySqlUtil mySqlUtil = new MySqlUtil(dbProperties);

        try (Connection databaseCreation = mySqlUtil.connect(false)) {
            if (!mySqlUtil.initDatabase()) {
                throw new SQLException("Error initializing database, check log for details...");
            }
        }

        try (Connection tableCreation = mySqlUtil.connect()) {
            if (!mySqlUtil.initTables()) {
                throw new SQLException("Error initializing table, check log for details...");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        transferToDatabase(mySqlUtil, accessLogFile, startDate, duration, threshold);

    }

    private void transferToDatabase(MySqlUtil mySqlUtil, File accessLogFile,
        Date startDate, String duration,Integer threshold) throws IOException , SQLException{
        final AtomicInteger counter = new AtomicInteger();
        final Map<String, Integer> requestCounter = new HashMap<>();
        final Map<String, Boolean> blocks = new HashMap<>();
        final String comment = String.format("Reached %s requests between %s and %s", threshold, startDate, endDate);
        try (BufferedReader reader = new BufferedReader(new FileReader(accessLogFile))) {
            try(Connection dataInsert = mySqlUtil.connect()){
                reader.lines().forEach(line -> {
                    int i = counter.incrementAndGet();
                    try {
                        LogLine logLine = parseLogLine(line);
                        if(mySqlUtil.insertLogLine(logLine)){
                            //System.out.println(String.format(" log record on line %s inserted successfully", i));
                            if(blocks.get(logLine.getIP()) == null && logLine.getDate().after(startDate)
                                && logLine.getDate().before(endDate)
                            ) {
                                //System.out.print("Access Status: " + logLine.getIP()+" --- " + requestCounter.get(logLine.getIP()));
                                Integer noOfRequests = requestCounter.get(logLine.getIP()) != null ? requestCounter.get(logLine.getIP())  : 1;
                                noOfRequests++;
                                requestCounter.put(logLine.getIP(), noOfRequests);
                                if(noOfRequests.compareTo(threshold) >= 0){
                                    mySqlUtil.insertBlockedIP(logLine.getIP(), comment);
                                    blocks.put(logLine.getIP(), true);
                                    //System.out.println(String.format("Blocked IP(%s) : ", logLine.getIP(), comment));
                                }
                            }
                        }else {
                            //System.out.println(String.format("Log record on line %s NOT inserted successfully due to (Unknown reason), Please review log record \n %s", i, line));
                        }
                    } catch (ParseException e) {
                        System.out.println(String.format("Log record on line %s NOT inserted successfully due to (%s)", i, e.getMessage()));
                        e.printStackTrace();
                    } catch (SQLException e) {
                        System.out.println(String.format("Log record on line %s NOT inserted successfully due to (%s)", i, e.getMessage()));
                        e.printStackTrace();
                    }
                });
            }
        }
        for(Entry<String, Boolean> ip: blocks.entrySet()){
            System.out.println(
                String
                    .format("%s: If you open the log file, %s has %s or more requests between %s and %s\n", 
                        ip.getKey(), ip.getKey(), threshold, parameterFormat.format(startDate), parameterFormat.format(endDate))
                        );
        }
    }

    private LogLine parseLogLine(String line) throws ParseException {
        
        String[] tokens = line.split("\\|");
        return new LogLine(logFormat
            .parse(tokens[0]), tokens[1],tokens[2],Integer.parseInt(tokens[3]), tokens[4]);
    }



    public static void main(String[] args) throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        
        Map<String, String> parameters = parseArgs(args);
        Date startDate = null;
        Date endDate = null;
        String duration = null;
        Integer threshold = null;
        String accesslog = null;
        Properties dbProperties;
        String usageMessage = "\nUsage : java -cp parser.jar --accesslog=/path/to/file  com.ef.Parser --startDate=2017-01-01.13:00:00 --duration=hourly --threshold=100";
        
        String startDateString = parameters.get("startDate");
        if(startDateString != null){
            try {
                startDate = parameterFormat.parse(startDateString);
            } catch (ParseException e) {
                appError("Cannot interprete value for startDate :- " + startDateString + usageMessage);
            }
        }else{
            appError("Please provide startDate parameter - " + usageMessage);
        }
        if(parameters.get("duration") != null){
            duration = parameters.get("duration");
            if(duration.equals("hourly")){
                Long endDateMillis = startDate.getTime() + 3599000;
                endDate = new Date(endDateMillis);
            }else if(duration.equals("daily")){
                Long endDateMillis = startDate.getTime() + 86399000;
                endDate = new Date(endDateMillis);
            }else{
                appError("only 'daily' and 'monthly' allowed for duration" + usageMessage);
            }
        }else{
            appError("Please provide duration parameter - " + usageMessage);
        }
        if(parameters.get("threshold") != null){
            threshold = Integer.parseInt(parameters.get("threshold"));
        }else{
            appError("Please provide threshold parameter - " + usageMessage);
        }
        if(parameters.get("accesslog") != null){
            accesslog = parameters.get("accesslog");
        }else{
            appError("Please provide accesslog parameter - " + usageMessage);
        }

        dbProperties = new Properties();
        System.out.println("WD-------"+FileSystems.getDefault().getPath(".").toAbsolutePath());
        try{
            dbProperties.load(new FileReader("db.properties"));
        }catch(FileNotFoundException  fx) {
            System.out.println("db.properties not found , using default config");
            dbProperties.put("url", "jdbc:mysql://localhost:3306");
            dbProperties.put("user", "root");
            dbProperties.put("password", "password");
            dbProperties.put("database", "access_log");
            dbProperties.put("table.log_line", "log_line");
            dbProperties.put("table.blocked_ip", "blocked_ip");
        }
        
        Parser parser = new Parser(new File(accesslog), dbProperties, startDate, endDate, duration, threshold);
        parser.parse();
    }

    public static void appError(String message){
        System.out.println(message);
        System.exit(-1);
    }

    public static Map<String, String> parseArgs(String[] parameters){
        //String[] parameters = {"com.ef.Parser","accesslog=/Users/john.alade/develop/java/test/src/resources/access.log", "--startDate=2017-01-01.00:08:00", "--duration=daily", "--threshold=10"};
        String EQUALS = "=";
        Map<String, String> map = new HashMap<>();
        for(String parameter: parameters){
            if(parameter.contains(EQUALS)){
                String[] pair = parameter.split(EQUALS);
                map.put(pair[0].replaceAll("-", ""), pair[1]);
            }else{
                map.put(parameter, "true");
            }
        }
        return map;

    }

    private static String getJarPath() throws IOException, URISyntaxException {
        File f = new File(Parser.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        String jarPath = f.getCanonicalPath().toString();
        String jarDir = jarPath.substring( 0, jarPath.lastIndexOf( File.separator ));
        return jarDir;
      }

}
  
                