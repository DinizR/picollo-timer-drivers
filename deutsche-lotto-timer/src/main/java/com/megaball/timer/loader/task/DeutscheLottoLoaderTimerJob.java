/*
* LoaderTimerJob.java
 */
package com.megaball.timer.loader.task;

import com.megaball.model.Result;
import com.megaball.sql.SQLUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.isNumeric;

/**
 * Generic Lottery Loader Job.
 * @author rod
 * @since 2021-08
*/
@DisallowConcurrentExecution
public class DeutscheLottoLoaderTimerJob implements Job {
    private static final String LOADER_LOAD_DIRECTORY = "loader.load.directory";
    private static final String LOADER_SQL_URL = "loader.sql.url";
    private static final String LOADER_SQL_USER = "loader.sql.user";
    private static final String LOADER_SQL_PASSWORD = "loader.sql.password";
    private static final String LOADER_GAME_NAME = "loader.game.name";
    private static final String LOADER_GAME_ID = "loader.game.id";
    private static final String LOADER_DATE_FORMAT = "loader.date.format";
    /*
     * Mapping:
     * 0=date,1+2+3+4+5+6=numbers,7=stars
     */
    private static final String LOADER_MAPPING = "loader.mapping";
    private static final String LOADER_NUMBER_OF_BALLS = "loader.numberOfBalls";
    private static final String COMMA = ",";
    private static final String PLUS = "+";
    private static final String DB_FIELD_NUMBERS = "numbers";
    private static final String DB_FIELD_TERMINATION = "termination";
    private static final String DB_FIELD_NUMBERS_PAIR_ODD = "numbers_pair_odd";
    private static final String DB_FIELD_STARS_PAIR_ODD = "stars_pair_odd";
    private static final String DB_FIELD_STARS = "stars";
    private static final String DB_FIELD_DECIMAL_NUMBERS = "decimal_numbers";
    private static final String DB_FIELD_DECIMAL_STARS = "decimal_stars";
    private static final String DB_FIELD_DISTANCE_NUMBERS = "distance_numbers";

    public DeutscheLottoLoaderTimerJob() {
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext){
        final String directory = LoaderTimer.configMap.get(LOADER_LOAD_DIRECTORY);
        final String mappings = LoaderTimer.configMap.get(LOADER_MAPPING);

        LoaderTimer.logger.info("Starting " + LoaderTimer.configMap.get(LOADER_GAME_NAME) + " Loader...");
        try (Stream<Path> stream = Files.list(Paths.get(directory))) {
            stream
               .filter(i -> ! Files.isDirectory(i))
               .forEach(i -> wrapException(i,mappings));
            LoaderTimer.logger.info(LoaderTimer.configMap.get(LOADER_GAME_NAME) + " Loader finished successfully.");
        } catch (IOException e) {
            LoaderTimer.logger.error(LoaderTimer.configMap.get(LOADER_GAME_NAME) + " Loader had problems processing files.",e);
        } catch (TaskException e) {
            LoaderTimer.logger.error(LoaderTimer.configMap.get(LOADER_GAME_NAME) + " Loader had problems processing files.", e);
        }
    }

    private void processFile(final Path f, final String mappings, final Connection connection) throws IOException, SQLException {
        final Reader reader = Files.newBufferedReader(f);
        final Map<String,String> maps = parseMappings(mappings);
        final Map<String,String> values = new HashMap<>();
        final long[] counter = new long[]{-1L};
        final PreparedStatement fetchLastYearTotalStmt = connection.prepareStatement(
           "SELECT MAX(total) FROM results");

        new CSVParser(reader,CSVFormat.DEFAULT
           .withFirstRecordAsHeader()
           .withIgnoreHeaderCase()
           .withAllowMissingColumnNames())
           .stream()
           .forEach(
              record -> {
                  LoaderTimer.logger.info("Record = {}", record);
                  maps.forEach((k, v) -> values.put(v, extractValueFromKey(k,record)));
                  calculateTermination(values);
                  calculateNumbersPairOdd(values);
                  calculateStarsPairOdd(values);
                  calculateDecimalNumbers(values);
                  calculateDecimalStars(values);
                  calculateDistanceNumbers(values);

                  try {
                      final ResultSet rsFetchLastYear;

                      if (counter[0] == -1) {
                          rsFetchLastYear = fetchLastYearTotalStmt.executeQuery();
                          if (rsFetchLastYear.next()) {
                              counter[0] = rsFetchLastYear.getLong(1) + 1;
                          } else {
                              counter[0] = 1;
                          }
                      }
                      writeNewRecordToDB(connection, values, counter);
                  } catch (SQLException e) {
                      try {
                          connection.rollback();
                      } catch (SQLException e1) {
                      }
                      throw new TaskException(e);
                  }
              }

           );
        try {
            organizeGameSequencing(connection);
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
            }
            throw new TaskException(e);
        }

        connection.commit();
    }

    private void writeNewRecordToDB(final Connection connection, final Map<String,String> values, long[] yearCounter) throws SQLException {
        final ResultSet rs;
        final PreparedStatement insertStmt = connection.prepareStatement(
           "INSERT INTO results (game_id,date,num_year,total,numbers,stars,termination,numbers_pair_odd,stars_pair_odd,decimal_numbers,decimal_stars,distance_numbers,created_stamp) " +
              "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");
        final PreparedStatement findByGameIdAndDate = connection.prepareStatement(
           "SELECT * FROM results " +
              "WHERE game_id = ? AND date = ?");
        final PreparedStatement updateStmt = connection.prepareStatement(
           "UPDATE results " +
              "SET numbers = ?," +
              "stars = ?," +
              "termination = ?," +
              "numbers_pair_odd = ?," +
              "stars_pair_odd = ?," +
              "decimal_numbers = ?," +
              "decimal_stars = ?," +
              "distance_numbers = ?," +
              "created_stamp = ? " +
              "WHERE id = ?");

        findByGameIdAndDate.setLong(1,Long.valueOf(LoaderTimer.configMap.get(LOADER_GAME_ID)));
        findByGameIdAndDate.setDate(2,Date.valueOf(LocalDate.parse(values.get("date"),
           DateTimeFormatter.ofPattern(LoaderTimer.configMap.get(LOADER_DATE_FORMAT)))));
        rs = findByGameIdAndDate.executeQuery();
        if (rs.next()) {
            final Result result = fillResultRow(rs);
            updateStmt.setString(1,values.get("numbers"));
            updateStmt.setString(2,values.get("stars"));
            updateStmt.setString(3,values.get("termination"));
            updateStmt.setString(4,values.get("numbers_pair_odd"));
            updateStmt.setString(5,values.get("stars_pair_odd"));
            updateStmt.setString(6,values.get("decimal_numbers"));
            updateStmt.setString(7,values.get("decimal_stars"));
            updateStmt.setString(8,values.get("distance_numbers"));
            updateStmt.setTimestamp(9,Timestamp.valueOf(LocalDateTime.now()));
            updateStmt.setLong(10,result.getId());
            updateStmt.execute();
        } else {
            insertStmt.setLong(1,Long.valueOf(LoaderTimer.configMap.get(LOADER_GAME_ID)));
            insertStmt.setDate(2,Date.valueOf(LocalDate.parse(values.get("date"),
               DateTimeFormatter.ofPattern(LoaderTimer.configMap.get(LOADER_DATE_FORMAT)))));
            insertStmt.setInt(3, 0);
            insertStmt.setLong(4, yearCounter[0]++);
            insertStmt.setString(5, values.get("numbers"));
            insertStmt.setString(6, values.get("stars"));
            insertStmt.setString(7, values.get("termination"));
            insertStmt.setString(8, values.get("numbers_pair_odd"));
            insertStmt.setString(9, values.get("stars_pair_odd"));
            insertStmt.setString(10, values.get("decimal_numbers"));
            insertStmt.setString(11,values.get("decimal_stars"));
            insertStmt.setString(12, values.get("distance_numbers"));
            insertStmt.setTimestamp(13, Timestamp.valueOf(LocalDateTime.now()));
            insertStmt.execute();
        }
    }

    private Result fillResultRow(final ResultSet rs) throws SQLException {
        return Result.builder()
           .createdStamp(rs.getTimestamp("created_stamp").toLocalDateTime())
           .date(rs.getDate("date").toLocalDate())
           .decimalNumbers(rs.getString("decimal_numbers"))
           .decimalStars(rs.getString("decimal_stars"))
           .id(rs.getLong("id"))
           .gameId(rs.getLong("game_id"))
           .numbersPairOdd(rs.getString("numbers_pair_odd"))
           .numYear(rs.getInt("num_year"))
           .termination(rs.getString("termination"))
           .numbers(rs.getString("numbers"))
           .starsPairOdd(rs.getString("stars_pair_odd"))
           .stars(rs.getString("stars"))
           .build();
    }

    private void organizeGameSequencing(final Connection connection) throws SQLException {
        final PreparedStatement fetchStmt = connection.prepareStatement(
           "SELECT id,date FROM results " +
                "WHERE game_id = ? " +
                "ORDER BY date");
        final PreparedStatement updateStmt = connection.prepareStatement(
            "UPDATE results " +
                "SET total = ?, num_year = ? " +
                "WHERE id = ?"
        );
        final ResultSet rs;
        long total = 0;
        long numYear = 0;
        LocalDate processingDate = null;

        fetchStmt.setLong(1,Long.valueOf(LoaderTimer.configMap.get(LOADER_GAME_ID)));
        fetchStmt.setFetchSize(100);
        rs = fetchStmt.executeQuery();
        while (rs.next()) {
            if (processingDate == null || processingDate.getYear() != rs.getDate("date").toLocalDate().getYear()) {
                processingDate = rs.getDate("date").toLocalDate();
                numYear = 0;
            }
            updateStmt.setLong(1, ++total);
            updateStmt.setLong(2, ++numYear);
            updateStmt.setLong(3, rs.getLong("id"));
            updateStmt.execute();
        }
    }

    private void calculateTermination(final Map<String,String> values) {
        final String numbers = values.get(DB_FIELD_NUMBERS);
        final String termination = Arrays.stream(numbers.split(" "))
                .map(Integer::valueOf)
                .map(i -> i >= 10 ? String.valueOf(i - ((i / 10)*10)) : i)
                .map(String::valueOf)
                .collect(Collectors.joining(" "));
        values.put(DB_FIELD_TERMINATION,termination);
    }

    private void calculateNumbersPairOdd(final Map<String,String> values) {
        final String numbers = values.get(DB_FIELD_NUMBERS);
        final String pairOdd = Arrays.stream(numbers.split(" "))
           .map(i -> Integer.valueOf(i) % 2 == 0 ? "P" : "I")
           .collect(Collectors.joining(" "));
        values.put(DB_FIELD_NUMBERS_PAIR_ODD,pairOdd);
    }

    private void calculateStarsPairOdd(final Map<String,String> values) {
        final String stars = values.get(DB_FIELD_STARS);
        final String pairOdd = Arrays.stream(stars.split(" "))
           .map(i -> Integer.valueOf(i) % 2 == 0 ? "P" : "I")
           .collect(Collectors.joining(" "));
        values.put(DB_FIELD_STARS_PAIR_ODD,pairOdd);
    }

    private void calculateDecimalNumbers(final Map<String,String> values) {
        final String stars = values.get(DB_FIELD_NUMBERS);
        final String decimalNumbers = Arrays.stream(stars.split(" "))
           .map(i -> Integer.valueOf(i) < 10 ? "0" : String.valueOf((Integer.valueOf(i) / 10) * 10))
           .collect(Collectors.joining(" "));
        values.put(DB_FIELD_DECIMAL_NUMBERS, decimalNumbers);
    }

    private void calculateDecimalStars(final Map<String,String> values) {
        final String stars = values.get(DB_FIELD_STARS);
        final String decimalStars = Arrays.stream(stars.split(" "))
           .map(i -> Integer.valueOf(i) < 10 ? "0" : String.valueOf((Integer.valueOf(i) / 10) * 10))
           .collect(Collectors.joining(" "));
        values.put(DB_FIELD_DECIMAL_STARS, decimalStars);
    }

    private void calculateDistanceNumbers(final Map<String,String> values) {
        final String[] numbers = values.get(DB_FIELD_NUMBERS).split(" ");
        final StringBuilder distanceNumbers = new StringBuilder();
        final String numberOfBalls = LoaderTimer.configMap.get(LOADER_NUMBER_OF_BALLS);

        for (int i = 0; i < numbers.length; i++) {
            if (i == 0) {
                distanceNumbers.append(numbers[i] + " ");
            } else if (i == (numbers.length-1) ) {
                distanceNumbers.append(Integer.valueOf(numbers[i]) - Integer.valueOf(numbers[i-1]) + " ");
                distanceNumbers.append(Integer.valueOf(numberOfBalls) - Integer.valueOf(numbers[i]));
            } else {
                distanceNumbers.append(Integer.valueOf(numbers[i]) - Integer.valueOf(numbers[i-1]) + " ");
            }
        }
        values.put(DB_FIELD_DISTANCE_NUMBERS, distanceNumbers.toString());
    }

    /*
     * Mapping:
     * 0=date,1+2+3+4+5+6=numbers,7=stars
     */
    private Map<String, String> parseMappings(final String mapping) {
        return Arrays.stream(mapping.split(COMMA))
           .map(s -> s.split("="))
           .collect(Collectors.toMap(k -> k[0], v -> v[1]));
    }

    private String extractValueFromKey(final String key, final CSVRecord record) {
        String ret;

        if (key.contains(PLUS)) {
            ret = parseColumns(key)
               .stream()
               .map(s -> Integer.valueOf(record.get(Integer.valueOf(s))))
               .map(String::valueOf)
               .collect(Collectors.joining(" "));;
        } else {
            ret = record.get(Integer.valueOf(key));
            if (isNumeric(ret)) {
               ret = String.valueOf(Integer.valueOf(ret));
            }
        }

        return ret;
    }

    private List<Integer> parseColumns(final String columns) {
        return Arrays.stream(columns.split("\\+"))
           .map(s -> Integer.valueOf(s))
           .collect(Collectors.toList());
    }

    void wrapException(final Path p, final String mapping) {
        final String directory = LoaderTimer.configMap.get(LOADER_LOAD_DIRECTORY);
        final String processedDestination = directory + "/processed";
        final String errorDestination = directory + "/error";
        final String sqlUrl = LoaderTimer.configMap.get(LOADER_SQL_URL);
        final String sqlUser = LoaderTimer.configMap.get(LOADER_SQL_USER);
        final String sqlPassword = LoaderTimer.configMap.get(LOADER_SQL_PASSWORD);
        Connection connection = null;

        try {
            connection = SQLUtils.getDBConnection(sqlUrl,sqlUser,sqlPassword);
            processFile(p, mapping, connection);
            Files.move(p, Paths.get(processedDestination + "/" + p.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            connection.close();
        } catch (Exception e) {
            try {
                Files.move(p, Paths.get(errorDestination + "/" + p.getFileName()), StandardCopyOption.REPLACE_EXISTING);
                if (connection != null) {
                    connection.close();
                }
            } catch (IOException e1) {
            } catch (SQLException e2) {
            }
            throw new TaskException(e);
        }
    }

    static class TaskException extends RuntimeException {
        public TaskException(Exception e) {
            super(e);
        }
    }
}
