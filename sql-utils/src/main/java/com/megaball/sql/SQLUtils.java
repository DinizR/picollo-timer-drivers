/*
* SQLUtils.java
 */
package com.megaball.sql;

import com.megaball.model.DetailedResult;
import com.megaball.model.Game;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class to deal with SQL connections.
 * @author rod
 * @since 2018-10
 */
public final class SQLUtils {
    private static final Logger log = LoggerFactory.getLogger(SQLUtils.class);
    private static final String SPACE = " ";

    public static Connection getDBConnection(String url, String user, String password) throws SQLException {
        final Connection connection;

        //Class.forName(driver); // JDBC 4 doesn't need upfront loading driver class
        connection = DriverManager.getConnection(url,user,password);
        connection.setAutoCommit(false);
        return connection;
    }

    public static DetailedResult loadResult(final ResultSet rs, final String gameId) throws SQLException {
        return DetailedResult.builder()
           .date(rs.getDate("date").toLocalDate())
           .id(rs.getLong("id"))
           .gameId(Long.valueOf(gameId))
           .numbers(Arrays.stream(rs.getString("numbers").split(SPACE))
              .map(Integer::valueOf)
              .collect(Collectors.toList()))
           .stars(Arrays.stream(rs.getString("stars").split(SPACE))
              .map(Integer::valueOf)
              .collect(Collectors.toList()))
           .build();
    }

    public static Game loadGame(final ResultSet rs) throws SQLException {
        return Game.builder()
           .id(rs.getLong("id"))
           .description(rs.getString("description"))
           .numbers(rs.getInt("numbers"))
           .extraNumbers(rs.getInt("extra_numbers"))
           .build();
    }

    /*
    public static void createLocalDB(String driver, String url, String user, String password) {
        Connection connection = null;
        Statement statement;

        try {
            connection = SQLUtils.getDBConnection(driver,url,user,password);
            connection.setAutoCommit(true);
            statement = connection.createStatement();
            statement.execute("CREATE SEQUENCE IF NOT EXISTS INTERACTION_SEQ;");
            statement.execute("CREATE TABLE IF NOT EXISTS interaction(id CHAR(18) NOT NULL,sequence BIGINT DEFAULT INTERACTION_SEQ.NEXTVAL,type CHAR(20) NOT NULL,startdate TIMESTAMP,enddate TIMESTAMP,timeshift INT DEFAULT 0,xml CLOB, finished BOOLEAN DEFAULT false,PRIMARY KEY(ID,type));");
            statement.execute("CREATE TABLE IF NOT EXISTS token(api CHAR(20) PRIMARY KEY,retrievaldate TIMESTAMP,expirationdate TIMESTAMP,accesstoken VARCHAR(128),refreshtoken VARCHAR(128));");
            statement.execute("CREATE INDEX IF NOT EXISTS date_index_interaction ON interaction(startdate);");
            statement.execute("CREATE INDEX IF NOT EXISTS seq_index_interaction ON interaction(sequence);");
        } catch (SQLException | ClassNotFoundException e) {
            log.error("Error creating new local db, message:{}",e.getMessage(),e);
        } finally {
            if( connection != null ) {
                try {
                    connection.close();
                } catch (SQLException e) {
                }
            }
        }
    }
*/

/*
    public static boolean countIsGreaterThanZero(final Connection connection,final String query) throws SQLException {
        final Statement statement = connection.createStatement();
        final ResultSet rs = statement.executeQuery(query);
        boolean r = false;

        if( rs.next() ) {
            if( rs.getLong(1) > 0 ) {
                r = true;
            }
        }

        return r;
    }
*/

/*
    public static Optional<String> fetchLastId(Connection connection, String query) throws SQLException {
        final PreparedStatement fetchStatement = connection.prepareStatement(query);
        final ResultSet rs = fetchStatement.executeQuery();
        Optional<String> id = Optional.empty();

        if( rs.next() ) {
            id = Optional.of(rs.getString(1));
        }

        return id;
    }
*/
}
