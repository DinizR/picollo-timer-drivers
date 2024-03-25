package nz.co.spark.cg.extractor.util;

import nz.co.spark.cg.extractor.model.AccessToken;

import java.sql.*;
import java.time.LocalDateTime;

public final class VoiceUtils {
    private static final String DUBBER = "Dubber";

    public static AccessToken fetchToken(final Connection connection) throws SQLException {
        final String FETCH_TOKEN = "SELECT api,retrievaldate,expirationdate,accesstoken,refreshtoken FROM token WHERE api = '"+DUBBER+"'";
        final PreparedStatement fetchStatement = connection.prepareStatement(FETCH_TOKEN);
        final ResultSet rs = fetchStatement.executeQuery();
        AccessToken accessToken = null;

        if( rs.next() ) {
            accessToken = new AccessToken();
            accessToken.setApi(rs.getString(1));
            accessToken.setRetrievalDate(rs.getTimestamp(2).toLocalDateTime());
            accessToken.setExpirationDate(rs.getTimestamp(3).toLocalDateTime());
            accessToken.setAccess_token(rs.getString(4));
            accessToken.setRefresh_token(rs.getString(5));
        }

        return accessToken;
    }

    public static void insertToken(final Connection connection,final AccessToken accessToken) throws SQLException {
        final String INSERT_TOKEN = "INSERT INTO token (api,retrievaldate,expirationdate,accesstoken,refreshtoken) VALUES (?,?,?,?,?);";
        final PreparedStatement statementInsert = connection.prepareStatement(INSERT_TOKEN);
        final LocalDateTime now = LocalDateTime.now();

        statementInsert.setString(1, DUBBER);
        statementInsert.setTimestamp(2, Timestamp.valueOf(now));
        statementInsert.setTimestamp(3, Timestamp.valueOf(now.plusSeconds(accessToken.getExpires_in())));
        statementInsert.setString(4,accessToken.getAccess_token());
        statementInsert.setString(5,accessToken.getRefresh_token());
        statementInsert.executeUpdate();
        connection.commit();
    }

    public static void updateToken(final Connection connection,final AccessToken accessToken) throws SQLException {
        final String UPDATE_TOKEN = "UPDATE token "+
                                    "SET retrievaldate = ?," +
                                        "expirationdate = ?,"+
                                        "accesstoken = ?,"+
                                        "refreshtoken = ? "+
                                    "WHERE api = '"+DUBBER+"';";
        final PreparedStatement statementUpdate = connection.prepareStatement(UPDATE_TOKEN);
        final LocalDateTime now = LocalDateTime.now();

        statementUpdate.setTimestamp(1, Timestamp.valueOf(now));
        statementUpdate.setTimestamp(2, Timestamp.valueOf(now.plusSeconds(accessToken.getExpires_in())));
        statementUpdate.setString(3,accessToken.getAccess_token());
        statementUpdate.setString(4,accessToken.getRefresh_token());
        statementUpdate.executeUpdate();
        connection.commit();
    }
}