package logfile;

//import com.sun.xml.internal.ws.api.ResourceLoader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;


public class Preprocess {

	private int c = 0;
        public Map<String, String> urlmap = new HashMap<>();
	File logProcess(Connection con) throws SQLException {
		Statement stmt = con.createStatement();
		
		String query;
               
		query = "create table  ProcessedData(USERID VARCHAR2(48),TIME TIMESTAMP,URL CLOB)";
		if(checkTable(stmt,query)==0){
                stmt.execute("drop table ProcessedData");
             stmt.execute(query);}
                query = "create table  UrlMapperData(URL CLOB,URLID varchar2(10))";
		if(checkTable(stmt,query)==0){
                stmt.execute("drop table UrlMapperData");
             stmt.execute(query);}
		query = "select USERID,TIME,URL FROM Weblog where status<400 and status>=200";
		ResultSet rs = stmt.executeQuery(query);
		String s = "insert into ProcessedData" + "(USERID,TIME,URL)" + "values(?,?,?)";
                String s1 = "insert into UrlMapperData" + "(URL,URLID)" + "values(?,?)";
		PreparedStatement p = con.prepareStatement(s);
                PreparedStatement p1 = con.prepareStatement(s1);
		while (rs.next()) {
			Clob clob;
			clob = oracle.sql.CLOB.createTemporary(con, false, oracle.sql.CLOB.DURATION_SESSION);
			String user = rs.getString("USERID");
			Timestamp timestamp = rs.getTimestamp("TIME");
			Clob url = rs.getClob("URL");
			p.setString(1, user);
			p.setTimestamp(2, timestamp);
			p.setClob(3, url);
			int i = p.executeUpdate();
			if (!urlmap.containsKey(url)) {
				urlmap.put(clobToString(url), String.valueOf(c));
                               
                        p1.setClob(1,url );
			p1.setString(2,String.valueOf(c++));
                          i=p1.executeUpdate();
			}

		}
		/*
		 * for (Entry<String, String> e : urlmap.entrySet()) {
		 * System.out.println(e.getKey() + "\t" + e.getValue()); }
		 */

		query = "select USERID,TIME,URL FROM ProcessedData";
		rs = stmt.executeQuery(query);
		Map<String, List<UserInfo>> userMap = new LinkedHashMap<String, List<UserInfo>>();
		while (rs.next()) {
			Clob clob;
			List<UserInfo> li = new LinkedList<UserInfo>();
			clob = oracle.sql.CLOB.createTemporary(con, false, oracle.sql.CLOB.DURATION_SESSION);
			String user = rs.getString("USERID");
			Timestamp timestamp = rs.getTimestamp("TIME");

			Clob url = rs.getClob("URL");
			li.add(new UserInfo(url, timestamp));
			if (!userMap.containsKey(user))
				userMap.put(user, li);
			else {
				li = userMap.get(user);
				li.add(new UserInfo(url, timestamp));
				userMap.put(user, li);
			}

                        userMap.entrySet().forEach((e) -> {
                            List<UserInfo> t = e.getValue();
                            
                    });
		}
		return sequenceCreation(userMap, urlmap, con);

	}
        

	private File sequenceCreation(Map<String, List<UserInfo>> userMap, Map<String, String> urlmap, Connection con)
			throws SQLException {
		Map<String, List<UrlTimestamp>> userTimeMap = new HashMap<String, List<UrlTimestamp>>();
		for (Entry<String, List<UserInfo>> e : userMap.entrySet()) {
			LinkedList<UserInfo> t = (LinkedList<UserInfo>) e.getValue();
			Timestamp tsp = t.getFirst().getTimeStamp();
			LinkedList<UrlTimestamp> li = new LinkedList<UrlTimestamp>();
			for (UserInfo ui : t) {
				li.add(new UrlTimestamp(urlmap.get(clobToString(ui.getUrl())),
						(ui.getTimeStamp().getTime() - tsp.getTime()) / 1000));

			}
			
			userTimeMap.put(e.getKey(), li);

		}
		Map<String, String> UserSessionSequenceMap = new HashMap<String, String>();
		for (Entry<String, List<UrlTimestamp>> e : userTimeMap.entrySet()) {
			long sessionTime = 1800;
			String str = "";
			LinkedList<UrlTimestamp> li = (LinkedList<UrlTimestamp>) e.getValue();
			for (UrlTimestamp ut : li) {
				if (ut.getSecs() <= sessionTime) {
					str = str + ut.getUrl() + " ";
				} else {
					str = str + String.valueOf(-1) + " " + ut.getUrl() + " ";
					sessionTime += 1800;
				}
			}
			str += String.valueOf(-1);
                        str += " "+String.valueOf(-2);
			UserSessionSequenceMap.put(e.getKey(), str);

		}
		for (Entry<String, String> e : UserSessionSequenceMap.entrySet()) {
		//	System.out.println(e.getKey() + "   " + e.getValue());
		}
		return SequenceDatabase(UserSessionSequenceMap, con);

	}

	private File SequenceDatabase(Map<String, String> userSessionSequenceMap, Connection con) throws SQLException {
		Statement stmt = con.createStatement();

		String query;
		query = "create table  SequenceData(USERID VARCHAR2(48),URLSequence CLOB)";
		if(checkTable(stmt,query)==0){
                stmt.execute("drop table SequenceData");
             stmt.execute(query);}
		String s = "insert into SequenceData" + "(USERID,URLSequence)" + "values(?,?)";
		PreparedStatement p = con.prepareStatement(s);
		Clob clob;
		BufferedWriter bw = null;
		FileWriter fw = null;
		File f=null;
		try {
			f=new File("sampleOutput.txt");
			f.createNewFile();
			fw = new FileWriter(f);
			bw = new BufferedWriter(fw);

			for (Entry<String, String> e : userSessionSequenceMap.entrySet()) {
				clob = oracle.sql.CLOB.createTemporary(con, false, oracle.sql.CLOB.DURATION_SESSION);
				bw.write(e.getValue() + "\n");
				//
                                //System.out.println("done");
				p.setString(1, e.getKey());
				clob.setString(1, e.getValue());
				p.setClob(2, clob);
				// clob.free();

				int j = p.executeUpdate();
			}
                        BufferedReader in = new BufferedReader(new FileReader(new File(getClass().getResource("file1.txt").getFile())));
		
	         String str;
	         System.out.println("de");
	         while ((str = in.readLine()) != null) {
	           bw.write(str+"\n");
	           
	         }
	         in.close();
			bw.flush();
			bw.close();
		} catch (IOException e) {

			e.printStackTrace();

		} finally {
					
			try {
                                  
				if (bw != null)
					bw.close();

				if (fw != null)
					fw.close();
				

			} catch (final IOException ex) {

				ex.printStackTrace();

			}
			return f;
		}
	}

	public String clobToString(Clob data) {
		StringBuilder sb = new StringBuilder();
		try {
			Reader reader = data.getCharacterStream();
			BufferedReader br = new BufferedReader(reader);

			String line;
			while (null != (line = br.readLine())) {
				sb.append(line);
			}
			br.close();
		} catch (SQLException e) {
			// handle this exception
		} catch (IOException e) {
			// handle this exception
		}
		return sb.toString();
	}

    private int  checkTable(Statement stmt, String query) {
       try{
       stmt.execute(query);
       }    catch (SQLException ex) {
           
               return 0;
            }
    
        return 1;
    }
}
