package logfile;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.*;
public class LogFields {

    private static BufferedReader br;
    File f1 = null;
    public Connection con;

    public Connection getConnection() {
        return con;
    }

    public File logfieldmethod(String file) {
        try {
            FileInputStream f = new FileInputStream(file);
            br = new BufferedReader(new InputStreamReader(f));
            String str;
            String Host;
            String Time;
            String Request;
            String Method;
            String URL;
            int status;
            int size = 0;
            String Referer;
            String UserAgent;
            try {

                Class.forName("oracle.jdbc.driver.OracleDriver");
                con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe", "kittu", "manager");
                Statement stmt = con.createStatement();
                String tablequery;
                tablequery = "create table  Weblog(HOST VARCHAR2(16),TIME TIMESTAMP,REQUEST_METHOD VARCHAR2(5),URL CLOB,STATUS INT,REFERRER VARCHAR2(600),LOCATION VARCHAR2(10),USERID VARCHAR2(48))";

                if(checkTable(stmt,tablequery)==0){
                stmt.execute("drop table Weblog");
             stmt.execute(tablequery);}

                String s = "insert into Weblog" + "(HOST,TIME,REQUEST_METHOD,URL,STATUS,REFERRER,LOCATION,USERID)"
                        + "values(?,?,?,?,?,?,?,?)";
                PreparedStatement p = con.prepareStatement(s);

                while ((str = br.readLine()) != null) {
                    java.sql.Clob clob;
                    clob = oracle.sql.CLOB.createTemporary(con, false, oracle.sql.CLOB.DURATION_SESSION);

                    StringTokenizer Splitter = new StringTokenizer(str, " \t");
                    String skip;

                    Host = Splitter.nextToken();
                    skip = Splitter.nextToken();
                    skip = Splitter.nextToken("[");
                    Time = Splitter.nextToken(" \t");
                    skip = Splitter.nextToken("\"");

                    Request = Splitter.nextToken();
                    skip = Splitter.nextToken(" \t");

                    status = Integer.valueOf(Splitter.nextToken(" \t"));

                    try {
                        size = Integer.parseInt(Splitter.nextToken(" \t"));
                    } catch (NumberFormatException e) {
                        size = 0;
                    }
                    Time = Time.substring(1);
                    // DateTimeFormatter dtf=new DateTimeFormatter
                    StringTokenizer split = new StringTokenizer(Time, ":");
                    String date = split.nextToken();
                    String h, m, sec, y, min, day;
                    h = split.nextToken();
                    min = split.nextToken();
                    sec = split.nextToken();

                    split = new StringTokenizer(date, "/");
                    day = split.nextToken();
                    m = split.nextToken();
                    y = split.nextToken();
                    switch (m) {
                        case "Jan":
                            m = "01";
                            break;
                        case "Feb":
                            m = "02";
                            break;
                        case "Mar":
                            m = "03";
                            break;
                        case "Apr":
                            m = "04";
                            break;
                        case "May":
                            m = "05";
                            break;
                        case "Jun":
                            m = "06";
                            break;
                        case "Jul":
                            m = "07";
                            break;
                        case "Aug":
                            m = "08";
                            break;
                        case "Sep":
                            m = "09";
                            break;
                        case "Oct":
                            m = "10";
                            break;
                        case "Nov":
                            m = "11";
                            break;
                        case "Dec":
                            m = "12";
                            break;
                    }
                    String sdf = y + "-" + m + "-" + day + " " + h + ":" + min + ":" + sec;
                    skip = Splitter.nextToken("\"");
                    Referer = Splitter.nextToken();
                    skip = Splitter.nextToken();
                    UserAgent = Splitter.nextToken();
                    skip = Splitter.nextToken();
                    String loc = Splitter.nextToken();
                    skip = Splitter.nextToken();
                    String hash = Splitter.nextToken();
                    /*	System.out.println("Host: " + Host);
					System.out.println("Time: " + Time);
					System.out.println("request: " + Request);
					System.out.println("status: " + status);
					System.out.println("referrer: " + Referer);
					System.out.println("user: " + UserAgent);
					System.out.println("location: " + loc);
					System.out.println("hash: " + hash);
					System.out.println(" ");
                     */
                    Splitter = new StringTokenizer(Request);
                    Method = Splitter.nextToken();
                    URL = Splitter.nextToken();
                    System.out.println("insert into Weblog values('" + Host
                            + "','" + sdf + "','" + Method + "','" + URL
                            + "'," + status + ",'" + Referer + "','" + loc + "','"
                            + hash + "');");

                    p.setString(1, Host);
                    p.setTimestamp(2, Timestamp.valueOf(sdf));
                    p.setString(3, Method);
                    clob.setString(1, URL);
                    p.setClob(4, clob);
                    p.setInt(5, status);
                    // clob.setString(0, Referer);
                    p.setString(6, Referer);
                    p.setString(7, loc);
                    p.setString(8, hash);
                    // System.out.println("insert into Weblog values('" + Host +
                    // "','" + sdf + "','" + Method + "','" + URL
                    // + "'," + status + ",'" + "','" + loc + "','" + hash +
                    // "');");
                    int i =p.executeUpdate();
                    //System.out.println(i);
                    
                }
                
                Preprocess proc = new Preprocess();
                f1 = proc.logProcess(con);
            } catch (SQLException sqe) {
                sqe.printStackTrace();
            }

        } catch (Exception e) {
            System.out.println(e);
        }
        return f1;

    }

    public int checkTable(Statement stmt, String tablequery) throws SQLException {
        //To change body of generated methods, choose Tools | Templates.
        try {
            stmt.execute(tablequery);

        } catch (SQLException ex) {
          
            return 0;
        }
        return 1;
    }
}
