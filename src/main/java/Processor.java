import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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

public class Processor {

    private File accessLogFile;
    private Properties dbProperties;
    private Date startDate;
    private Date endDate;
    private String duration;
    private Integer threshold;
    private static final SimpleDateFormat parameterFormat = new SimpleDateFormat("yyyy-MM-dd'.'HH:mm:ss");
    private static final SimpleDateFormat logFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Processor(File accessLogFile, Properties dbProperties, Date startDate, Date endDate, String duration,
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

}