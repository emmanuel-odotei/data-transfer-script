package com.demo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import java.sql.*;
import java.util.Objects;

public class DataTransfer {
    public static void main (String[] args) {
        // MySQL's connection details
        String mysqlUrl = System.getenv( "MYSQL_URL" );
        String mysqlUser = System.getenv( "MYSQL_USER" );
        String mysqlPassword = System.getenv( "MYSQL_PASSWORD" );
        
        // PostgreSQL's connection details
        String postgresUrl = System.getenv( "POSTGRES_URL" );
        String postgresUser = System.getenv( "POSTGRES_USER" );
        String postgresPassword = System.getenv( "POSTGRES_PASSWORD" );
        
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        
        try ( Connection mysqlConn = DriverManager.getConnection( mysqlUrl, mysqlUser, mysqlPassword );
              Connection postgresConn = DriverManager.getConnection( postgresUrl, postgresUser, postgresPassword ) ) {
            
            // Retrieve data from MySQL
            String selectSQL = "SELECT id AS journal_id, transactions FROM journal_vouchers WHERE YEAR(created_at) = " +
                    "2024";
            try ( Statement mysqlStatement = mysqlConn.createStatement();
                  ResultSet results = mysqlStatement.executeQuery( selectSQL ) ) {
                
                // Create table in PostgresSQL if it doesn't exist
                String createTableSQL = "CREATE TABLE IF NOT EXISTS temporary_particulars (" +
                        "id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY," +
                        "journal_id BIGINT," +
                        "name VARCHAR(255)," +
                        "department VARCHAR(255)," +
                        "rate DOUBLE PRECISION," +
                        "credit DOUBLE PRECISION," +
                        "debit DOUBLE PRECISION" +
                        ")";
                try ( Statement postgresStatement = postgresConn.createStatement() ) {
                    postgresStatement.execute( createTableSQL );
                } catch ( SQLException e ) {
                    throw new RuntimeException( e );
                }
                
                // Prepare insert statement for PostgreSQL
                String insertSQL = "INSERT INTO temporary_particulars (journal_id, name, department, rate, " +
                        "credit, debit) VALUES ( ?, ?, ?, ?, ?, ?)";
                try ( PreparedStatement preparedStatement = postgresConn.prepareStatement( insertSQL ) ) {
                    while ( results.next() ) {
                        long journalId = results.getInt( "journal_id" );
                        String transactionsJson = results.getString( "transactions" ).replace( "\\", "" ).replaceAll( "^\"|\"$", "" );
                        
                        JsonObject transactionsObject = gson.fromJson( transactionsJson, JsonObject.class );
                        for (String key : transactionsObject.keySet()) {
                            JsonObject transaction = transactionsObject.getAsJsonObject(key);
                            System.out.println( "transaction = " + transaction );
                            String name = transaction.get("particulars").getAsString();
                            String department = transaction.get("costCenter").getAsString();
                            String rateStr = transaction.get("rate").getAsString();
                            String creditStr = transaction.get("credit").getAsString();
                            String debitStr = transaction.get("debit").getAsString();
                            
                            
                            Double rate = !rateStr.isEmpty() ? Double.valueOf( rateStr ): null;
                            Double credit = !creditStr.isEmpty() ? Double.valueOf( creditStr ) : null;
                            Double debit = !debitStr.isEmpty() ? Double.valueOf( debitStr ) : null;
                            
                            preparedStatement.setLong( 1, journalId );
                            preparedStatement.setString( 2, name );
                            preparedStatement.setString( 3, department );
                            preparedStatement.setDouble( 4, Objects.requireNonNullElse( rate, 0.0 ) );
                            preparedStatement.setDouble( 5, Objects.requireNonNullElse( credit, 0.0 ) );
                            preparedStatement.setDouble( 6, Objects.requireNonNullElse( debit, 0.0 ) );
                            
                            preparedStatement.addBatch();
                        }
                    }
                    preparedStatement.executeBatch();
                }
            }
        } catch ( SQLException e ) {
            System.out.println( e.getMessage() );
        }
    }
}