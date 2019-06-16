package com.wallethub.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.wallethub.log.service.Processor;

public class Parser {

    private static final SimpleDateFormat parameterFormat = new SimpleDateFormat("yyyy-MM-dd'.'HH:mm:ss");
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
        
        Processor processor = new Processor(new File(accesslog), dbProperties, startDate, endDate, duration, threshold);
        processor.parse();
    }

    public static void appError(String message){
        System.out.println(message);
        System.exit(-1);
    }

    public static Map<String, String> parseArgs(String[] parameters){
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
    
}
               