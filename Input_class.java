package dbs;
import java.io.*;
import java.sql.*;
public class Input_class {
	 public static void main(String[] args) {
	        String jdbcURL = "jdbc:mysql://localhost:3306/DBS_db";
	        String username = "user";
	        String password = "password";
	        String csvFilePath = "file_name.csv";
	        int batchSize = 20;
	        Connection connection = null;
	        try {
	        	connection = DriverManager.getConnection(jdbcURL, username, password);
	            connection.setAutoCommit(false);
	            String sql = "INSERT INTO  (customer_key, customer_name) VALUES (?, ?)";
	            PreparedStatement statement = connection.prepareStatement(sql);
	 
	            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
	            String lineText = null;
	            int count = 0;
	            lineReader.readLine();
	            while ((lineText = lineReader.readLine()) != null) {
	                String[] data = lineText.split(",");
	                String customer_key = data[0];
	                String customer_name = data[1];
	                statement.setString(1,customer_key);
	                statement.setString(2, customer_name);
	                statement.addBatch();
	                if (count % batchSize == 0) {
	                    statement.executeBatch();
	                }
	            }
	 
	            lineReader.close();
	            statement.executeBatch();
	            connection.commit();
	            connection.close();
	 
	        } 
	        catch (IOException ex) {
	            System.err.println(ex);
	        } 
	        catch (SQLException ex) {
	            ex.printStackTrace();
	        }
	        try {
	                connection.rollback();
	            }
	        catch (SQLException e) {
	                e.printStackTrace();
	            }
	        }
	 
	    }
