/*
* VoiceExtractorJob.java
 */
package nz.co.spark.extractor.task;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import nz.co.spark.cg.extractor.model.*;
import nz.co.spark.cg.extractor.serializer.RecordingDeserializer;
import nz.co.spark.cg.extractor.util.VoiceUtils;
import nz.co.spark.cg.shared.model.*;
import nz.co.spark.cg.shared.sql.ExtractionUtils;
import nz.co.spark.cg.shared.sql.SQLUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Voice Extractor Job.
 * @author rod
 * @since 2018-12-04
 */
@DisallowConcurrentExecution
public class VoiceExtractorJob implements Job {
    private static final String OUTPUT_DIRECTORY = "output.directory";

    private static final String DATABASE_LOCAL_DRIVER = "database.local.driver";
    private static final String DATABASE_LOCAL_URL    = "database.local.url";
    private static final String DATABASE_LOCAL_USER   = "database.local.user";
    private static final String DATABASE_LOCAL_PASSWORD = "database.local.password";

    private static final String DUBBER_TOKEN_URL = "dubber.token.url";
    private static final String DUBBER_CLIENT_ID = "dubber.client.id";
    private static final String DUBBER_CLIENT_SECRET = "dubber.client.secret";
    private static final String DUBBER_TOKEN_AUTH_ID = "dubber.token.auth.id";
    private static final String DUBBER_TOKEN_AUTH_TOKEN = "dubber.token.auth.token";
    private static final String DUBBER_RECORDINGS_URL = "dubber.recordings.url";
    private static final String DUBBER_RECORDINGS_FETCH_SIZE = "dubber.recordings.fetch.size";
    private static final String DUBBER_RECORDING_AUTH_EMAIL = "dubber.recording.auth.email";
    private static final String DUBBER_RECORDING_URL = "dubber.recording.url";
    private static final String DUBBER_RECORDING_CALL_DELAY = "dubber.recording.api.call.delay";
    private static final String DUBBER_RECORDING_PROXY = "dubber.recording.proxy";
    private static final String DUBBER_RECORDING_PROXY_URL_FROM = "dubber.recording.proxy.url.from";
    private static final String DUBBER_RECORDING_PROXY_URL_TO = "dubber.recording.proxy.url.to";
    private static final String DUBBER_RECORDING_PROXY_AWS_URL_FROM = "dubber.recording.proxy.aws.url.from";
    private static final String DUBBER_RECORDING_PROXY_AWS_URL_TO = "dubber.recording.proxy.aws.url.to";
    private static final String DUBBER_RECORDING_PROXY_DUBBER_FROM = "dubber.recording.proxy.dubber.from";
    private static final String DUBBER_RECORDING_PROXY_DUBBER_TO = "dubber.recording.proxy.dubber.to";
    private static final String DUBBER_ACCOUNT_DUB_POINTS_URL = "dubber.account.dub.points.url";
    private static final String DUBBER_ACCOUNT_NAMES = "dubber.account.names";
    private static final String DUBBER_ACCOUNT_URL = "dubber.account.url";
    private static final String DUBBER_SITE_MAPPING = "dubber.site.mapping";
    private static final String DUBBER_RECORDINGS_FETCH_DATE = "dubber.recordings.fetch.date";

    private static final Map<String,DubPoint> dubMap = new HashMap<>();
    private static final Map<String,Account> accountMap = new HashMap<>();

    private static final String WAV = ".wav";
    private static final String DUBBER = "Dubber";
    private static final String GRANT_TYPE_PASSWORD = "password";
    private static final String GRANT_TYPE_REFRESH_TOKEN = "refresh_token";
    private static final String PART_ID = "0";
    private static final String COMMA = ",";
    public static final String COLON = ":";

    @Autowired
    private RestTemplate restTemplate = new RestTemplate();

    public VoiceExtractorJob() {
        Connection localConnection = null;

        try {
            SQLUtils.createLocalDB(
               VoiceExtractorTimer.configMap.get(DATABASE_LOCAL_DRIVER),
               VoiceExtractorTimer.configMap.get(DATABASE_LOCAL_URL),
               VoiceExtractorTimer.configMap.get(DATABASE_LOCAL_USER),
               VoiceExtractorTimer.configMap.get(DATABASE_LOCAL_PASSWORD)
            );
            localConnection = SQLUtils.getDBConnection(
               VoiceExtractorTimer.configMap.get(DATABASE_LOCAL_DRIVER),
               VoiceExtractorTimer.configMap.get(DATABASE_LOCAL_URL),
               VoiceExtractorTimer.configMap.get(DATABASE_LOCAL_USER),
               VoiceExtractorTimer.configMap.get(DATABASE_LOCAL_PASSWORD)
            );
        } catch (SQLException e) {
            VoiceExtractorTimer.logger.error("Error connecting or running query to local database.",e);
        } catch (ClassNotFoundException e) {
            VoiceExtractorTimer.logger.error("Database driver class could not be found.",e);
        } finally {
            if( localConnection != null ) {
                try {
                    localConnection.close();
                } catch (SQLException e) {
                    VoiceExtractorTimer.logger.error("Problems closing database connection.",e);
                }
            }
        }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        final String urlRecording = processProxy(DUBBER_RECORDING_URL);
        final String[] accounts = VoiceExtractorTimer.configMap.get(DUBBER_ACCOUNT_NAMES).split(COMMA);
        Optional<String> id;
        ResponseEntity<Recordings> recordings;
        Connection localConnection = null;
        AccessToken accessToken;
        HttpEntity<String> entity;
        Account accountDetail;

        VoiceExtractorTimer.logger.info("Running voice extraction task for accounts ={}...",accounts);
        for( String account : accounts ) {
            try {
                VoiceExtractorTimer.logger.info("Running account : {}.",account);
                localConnection = SQLUtils.getDBConnection(
                        VoiceExtractorTimer.configMap.get(DATABASE_LOCAL_DRIVER),
                        VoiceExtractorTimer.configMap.get(DATABASE_LOCAL_URL),
                        VoiceExtractorTimer.configMap.get(DATABASE_LOCAL_USER),
                        VoiceExtractorTimer.configMap.get(DATABASE_LOCAL_PASSWORD)
                );
                // fetch token from db //
                accessToken = VoiceUtils.fetchToken(localConnection);
                if (accessToken == null) {
                    VoiceExtractorTimer.logger.debug("No token present, getting new token...");
                    accessToken = fetchApiToken(localConnection, GRANT_TYPE_PASSWORD, null);
                }
                // Fetching the last id
                id = fetchLastId(localConnection);
                entity = new HttpEntity<>("params", createHttpHeaders(accessToken));
                accountDetail = fetchAccountDetail(account,entity);
                recordings = fetchRecordings(account,localConnection,accessToken,entity,id);
                // Fetching specific recording metadata
                if (recordings.getBody() != null && recordings.getBody().getRecordings() != null && recordings.getBody().getRecordings().size() > 0) {
                    processRecordings(id, recordings, localConnection, entity, urlRecording, accountDetail);
                } else {
                    VoiceExtractorTimer.logger.info("No new voice recordings found.");
                }
                VoiceExtractorTimer.logger.info("Voice extraction task finished.");
            } catch (SQLException e) {
                VoiceExtractorTimer.logger.error("Error connecting or running query to local database.", e);
            } catch (ClassNotFoundException e) {
                VoiceExtractorTimer.logger.error("Database driver class could not be found.", e);
            } catch (Throwable e) {
                VoiceExtractorTimer.logger.error("Problems running voice extraction task.", e);
            } finally {
                if (localConnection != null) {
                    try {
                        localConnection.close();
                    } catch (SQLException e) {
                        VoiceExtractorTimer.logger.error("Problems closing database connection.", e);
                    }
                }
            }
        }
    }

    private void processRecordings(final Optional<String> id, final ResponseEntity<Recordings> recordings, final Connection finalLocalConnection, final HttpEntity<String> entity, final String urlRecording, final Account accountDetail) {
        final Map<String,String> siteMap = processSiteMapping();

        recordings.getBody().getRecordings()
            .stream()
            .filter(r -> !id.isPresent() || !r.getId().equals(id.get()))
            .forEach(r -> {
                // Fetching the wav files
                final Map<String, String> params = new HashMap<>();
                final ResponseEntity<String> responseEntity;
                final Contact contact = new Contact(r.getId(),
                        ContactType.Voice,
                        r.getStart_time().withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime().format(DateTimeFormatter.ISO_DATE_TIME),
                        r.getStart_time().plus(r.getDuration(), ChronoUnit.SECONDS).withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime().format(DateTimeFormatter.ISO_DATE_TIME));
                final Metadata metadata = new Metadata();
                final DubPoint dubPoint;
                final ObjectMapper mapper = new ObjectMapper();
                final SimpleModule module = new SimpleModule();

                contact.setMetadata(metadata);
                contact.addParty(PART_ID, new Party(PART_ID, "0", r.getTo(), PartyType.AGENT));
                try {
                    params.put("listener", VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_AUTH_EMAIL));
                    params.put("recordingId", r.getId());
                    VoiceExtractorTimer.logger.debug("Calling the URL={},params={}", urlRecording, params);
                    Thread.sleep(Integer.valueOf(VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_CALL_DELAY)));
                    responseEntity = restTemplate.exchange(urlRecording, HttpMethod.GET, entity, String.class, params);
                    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    module.addDeserializer(Recording.class,new RecordingDeserializer());
                    mapper.registerModule(module);
                    final Recording recording = mapper.readValue(responseEntity.getBody(),Recording.class);
                    metadata.addItem(new MetadataItem("account", accountDetail.getName()));
                    metadata.addItem(new MetadataItem("from", recording.getFrom()));
                    metadata.addItem(new MetadataItem("fromLabel", recording.getFrom_label()));
                    metadata.addItem(new MetadataItem("to", recording.getTo()));
                    metadata.addItem(new MetadataItem("toLabel", recording.getTo_label()));
                    metadata.addItem(new MetadataItem("callType", recording.getCall_type()));
                    metadata.addItem(new MetadataItem("duration", String.valueOf(recording.getDuration())));
                    metadata.addItem(new MetadataItem("dubPointId", recording.getDub_point_id()));
                    if (recording.getMetaTags() != null) {
                        metadata.addItem(new MetadataItem("recordingPlatform", recording.getMetaTags().getRecordingPlatform()));
                        metadata.addItem(new MetadataItem("originalFilename", recording.getMetaTags().getOriginalFileName()));
                        metadata.addItem(new MetadataItem("externalTrackingIds", recording.getMetaTags().getExternalTrakingIds()));
                        metadata.addItem(new MetadataItem("recorderCallId", recording.getMetaTags().getRecorderCallId()));
                        metadata.addItem(new MetadataItem("recorderIdentifier", recording.getMetaTags().getRecorderIdentifier()));
                        metadata.addItem(new MetadataItem("externalCallId", recording.getMetaTags().getExternalTrakingIds()));
                    }
                    if( (dubPoint = fetchDubPointDetail(recording.getDub_point_id(),entity)) != null ) {
                        metadata.addItem(new MetadataItem("site", siteMap.get(dubPoint.getExternal_group()) == null ? "" : siteMap.get(dubPoint.getExternal_group())));
                        metadata.addItem(new MetadataItem("externalGroup", dubPoint.getExternal_group()));
                    }
                    VoiceExtractorTimer.logger.debug("Getting Recording recording={}", responseEntity.getBody());
                    if (responseEntity.getStatusCode() == HttpStatus.OK) {
                        try {
                            if (checkExistentId(finalLocalConnection, "SELECT id FROM interaction WHERE id = ? AND type='Voice'", r.getId())) {
                                VoiceExtractorTimer.logger.error("Trying to insert/fetch existent voice recording. id={}", r.getId());
                            } else {
                                insertLocal(finalLocalConnection, recording);
                                if( metadata.getMetadataItem("site").isPresent() ) {
                                    final String filePath = "Voice-"+accountDetail.getName().toUpperCase()+"-"+metadata.getMetadataItem("site").get().getValue()+"-"+r.getStart_time().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))+"-"+r.getId()+WAV;
                                    Thread.sleep(Integer.valueOf(VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_CALL_DELAY)));
                                    metadata.addItem(new MetadataItem("audioFileName",filePath));
                                    fetchFile(VoiceExtractorTimer.configMap.get(OUTPUT_DIRECTORY)+File.separator+filePath,recording.getRecording_url());
                                    ExtractionUtils.load(contact, VoiceExtractorTimer.configMap.get(OUTPUT_DIRECTORY), PART_ID, accountDetail.getName(), metadata.getMetadataItem("site").get().getValue());
                                } else {
                                    VoiceExtractorTimer.logger.error("Error. Site metadata is not present. id={}", r.getId());
                                }
                            }
                        } catch (IOException e) {
                            VoiceExtractorTimer.logger.error("Error fetching or writing the recording voice file.", e);
                        } catch (SQLException e) {
                            VoiceExtractorTimer.logger.error("Error writing the recording voice file in the local database.", e);
                        } catch (URISyntaxException e) {
                            VoiceExtractorTimer.logger.error("Error parsing URL to URI.", e);
                        } catch (InterruptedException e) {
                            VoiceExtractorTimer.logger.error("Error interrupting the current Thread.", e);
                        }
                    } else {
                        VoiceExtractorTimer.logger.error("Error fetching recording. HTTP status={}, body={}.", responseEntity.getStatusCode(), responseEntity.getBody());
                    }
                } catch (HttpClientErrorException ex) {
                    VoiceExtractorTimer.logger.error("HTTP Client Error.", ex);
                } catch (JsonParseException ex) {
                    VoiceExtractorTimer.logger.error("Error parsing the returned JSON.", ex);
                } catch (JsonMappingException ex) {
                    VoiceExtractorTimer.logger.error("Error parsing the returned JSON.", ex);
                } catch (IOException ex) {
                    VoiceExtractorTimer.logger.error("General I/O Exception.", ex);
                } catch (InterruptedException e) {
                    VoiceExtractorTimer.logger.error("Error interrupting the current Thread.", e);
                }
            });

    }

    private boolean checkExistentId(Connection connection,String query,String id) throws SQLException {
        final PreparedStatement fetchStatement = connection.prepareStatement(query);
        final ResultSet rs;

        fetchStatement.setString(1,id);
        rs =  fetchStatement.executeQuery();
        return rs.next();
    }

    private void fetchFile(String filePath,String url) throws IOException, URISyntaxException {
        final RestTemplate restTemplate = new RestTemplate();
        final HttpHeaders headers = new HttpHeaders();
        final HttpEntity<String> entity = new HttpEntity<>(headers);
        final ResponseEntity<byte[]> response;
        String urlReverseProxy = VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_PROXY).equalsIgnoreCase("yes") ?
                     url.replace(VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_PROXY_AWS_URL_FROM),VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_PROXY_AWS_URL_TO)) :
                     url;
        final URI uri;

        if( ! VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_PROXY_DUBBER_FROM).isEmpty() && url.startsWith(VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_PROXY_DUBBER_FROM)) ) {
            urlReverseProxy = VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_PROXY).equalsIgnoreCase("yes") ?
                url.replace(VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_PROXY_DUBBER_FROM), VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_PROXY_DUBBER_TO)) :
                url;
        }

        uri = new URI(urlReverseProxy);
        restTemplate.getMessageConverters().add(new ByteArrayHttpMessageConverter());
        headers.setAccept(Collections.singletonList(MediaType.ALL));

        VoiceExtractorTimer.logger.debug("Fetching file from AWS url={}",urlReverseProxy);
        VoiceExtractorTimer.logger.debug("Calling the URI={}", uri);
        response = restTemplate.exchange(uri,HttpMethod.GET,entity,byte[].class);
        VoiceExtractorTimer.logger.debug("Fetching file status={}.",response.getStatusCode());
        VoiceExtractorTimer.logger.debug("File path={}.",filePath);
        if( response.getStatusCode() == HttpStatus.OK  && response.getBody() != null ) {
            Files.write(Paths.get(filePath), response.getBody());
        } else {
            throw new IOException("Failed to fetch voice file from S3. status="+response.getStatusCode());
        }
    }

    private void insertLocal(Connection localConnection,Recording recording) throws SQLException {
        final String INSERT_LOCAL = "INSERT INTO interaction (id,type,startdate,enddate) VALUES (?,?,?,?);"; /*,xml,timeshift*/
        final PreparedStatement statementInsert = localConnection.prepareStatement(INSERT_LOCAL);

        statementInsert.setString(1, recording.getId());
        statementInsert.setString(2, ContactType.Voice.toString());
        statementInsert.setTimestamp(3,Timestamp.valueOf(recording.getStart_time().toLocalDateTime()));
        statementInsert.setTimestamp(4,Timestamp.valueOf(recording.getStart_time().plus(recording.getDuration(),ChronoUnit.SECONDS).toLocalDateTime()));

        statementInsert.execute();
        localConnection.commit();
    }

    private HttpHeaders createHttpHeaders(AccessToken accessToken) {
        HttpHeaders headers = new HttpHeaders();

        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.add("Authorization", "Bearer " + accessToken.getAccess_token());

        return headers;
    }

    private AccessToken fetchApiToken(Connection localConnection, String grantType, AccessToken oldToken) throws IOException,SQLException {
        final HttpHeaders headers = new HttpHeaders();
        final MultiValueMap<String,String> httpParams = new LinkedMultiValueMap<>();
        final HttpEntity<MultiValueMap<String,String>> request = new HttpEntity<>(httpParams,headers);
        AccessToken accessToken;
        ResponseEntity<String> entity;
        final String url = processProxy(DUBBER_TOKEN_URL);

        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        httpParams.add("client_id",VoiceExtractorTimer.configMap.get(DUBBER_CLIENT_ID));
        httpParams.add("client_secret",VoiceExtractorTimer.configMap.get(DUBBER_CLIENT_SECRET));
        if( grantType.equals(GRANT_TYPE_PASSWORD) ) {
            httpParams.add("username",VoiceExtractorTimer.configMap.get(DUBBER_TOKEN_AUTH_ID));
            httpParams.add("password",VoiceExtractorTimer.configMap.get(DUBBER_TOKEN_AUTH_TOKEN));
        } else if( grantType.equals(GRANT_TYPE_REFRESH_TOKEN) ) {
            httpParams.add("refresh_token",oldToken.getRefresh_token());
        }
        httpParams.add("grant_type", grantType);

        VoiceExtractorTimer.logger.debug("Calling the URL={},params={}", url, httpParams);
        try {
            entity = restTemplate.postForEntity(url, request, String.class);
        } catch (HttpClientErrorException ex) {
            VoiceExtractorTimer.logger.error("Problems refreshing token. HTTP status={},response body={}",ex.getStatusCode(),ex.getResponseBodyAsString(),ex);
            if( ex.getStatusCode() == HttpStatus.BAD_REQUEST ) {
                httpParams.clear();
                httpParams.add("client_id",VoiceExtractorTimer.configMap.get(DUBBER_CLIENT_ID));
                httpParams.add("client_secret",VoiceExtractorTimer.configMap.get(DUBBER_CLIENT_SECRET));
                httpParams.add("username",VoiceExtractorTimer.configMap.get(DUBBER_TOKEN_AUTH_ID));
                httpParams.add("password",VoiceExtractorTimer.configMap.get(DUBBER_TOKEN_AUTH_TOKEN));
                httpParams.add("grant_type", GRANT_TYPE_PASSWORD);
                entity = restTemplate.postForEntity(url, request, String.class);
            } else {
                throw ex;
            }
        }
        ObjectMapper mapper = new ObjectMapper();

        VoiceExtractorTimer.logger.debug("Returned body={}",entity.getBody());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);
        accessToken = mapper.readValue(entity.getBody(),AccessToken.class);
        accessToken.setApi(DUBBER);
        VoiceExtractorTimer.logger.debug("Returned Token={}",accessToken);
        if( VoiceUtils.fetchToken(localConnection) == null ) {
            VoiceUtils.insertToken(localConnection,accessToken);
        } else {
            VoiceUtils.updateToken(localConnection,accessToken);
        }

        return accessToken;
    }

    private Optional<String> fetchLastId(Connection localConnection) throws SQLException {
        final Optional<String> id = SQLUtils.fetchLastId(localConnection, "SELECT id FROM interaction WHERE type='Voice' ORDER BY id DESC LIMIT 1;");
        VoiceExtractorTimer.logger.debug("Last Record id={}",id);
        return id;
    }

    private ResponseEntity<Recordings> fetchRecordings(final String account,final Connection localConnection, AccessToken accessToken, HttpEntity<String> entity, Optional<String> id) throws SQLException, IOException {
        final Map<String, String> urlParams = new HashMap<>();
        String urlRecordings = processProxy(DUBBER_RECORDINGS_URL).replace("{ACCOUNT_ID}",account);
        ResponseEntity<Recordings> recordings = null;

        if( id.isPresent() ) {
            VoiceExtractorTimer.logger.debug("ID is present id={}",id);
            urlParams.put("after_id", id.get());
            urlParams.put("count", VoiceExtractorTimer.configMap.get(DUBBER_RECORDINGS_FETCH_SIZE));
            urlRecordings = urlRecordings + "?after_id="+id.get()+"&count="+VoiceExtractorTimer.configMap.get(DUBBER_RECORDINGS_FETCH_SIZE);
        } else {
            final String date = VoiceExtractorTimer.configMap.get(DUBBER_RECORDINGS_FETCH_DATE).isEmpty() ?
                    VoiceExtractorTimer.configMap.get(DUBBER_RECORDINGS_FETCH_DATE) :
                    LocalDate.now().format(DateTimeFormatter.ISO_DATE);
            urlParams.put("on",date);
        }
        try {
            VoiceExtractorTimer.logger.debug("Calling the URL={},params={},headers={}",urlRecordings,urlParams,entity);
            Thread.sleep(Integer.valueOf(VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_CALL_DELAY)));
            recordings = restTemplate.exchange(urlRecordings, HttpMethod.GET, entity, Recordings.class, urlParams);
        } catch (HttpClientErrorException ex) {
            VoiceExtractorTimer.logger.debug("Error:",ex);
            if( ex.getStatusCode() == HttpStatus.UNAUTHORIZED ) {
                VoiceExtractorTimer.logger.debug("Expired token, renew process is starting...");
                accessToken = fetchApiToken(localConnection, GRANT_TYPE_REFRESH_TOKEN, accessToken);
                entity = new HttpEntity<>("params",createHttpHeaders(accessToken));
                VoiceExtractorTimer.logger.debug("Calling the URL={},params={}",urlRecordings,urlParams);
                try {
                    Thread.sleep(Integer.valueOf(VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_CALL_DELAY)));
                } catch (InterruptedException e) {
                }
                recordings = restTemplate.exchange(urlRecordings, HttpMethod.GET, entity, Recordings.class, urlParams);
            } else {
                throw ex;
            }
        } catch (InterruptedException e) {
            VoiceExtractorTimer.logger.error("Error interrupting the current Thread.", e);
        }
        VoiceExtractorTimer.logger.debug("Getting Recordings recordings={}", recordings);

        return recordings;
    }

    private DubPoint fetchDubPointDetail(String dubPointId, HttpEntity<String> entity) {
        final ResponseEntity<DubPoint> dubPoint;
        String urlDubPointDetail = processProxy(DUBBER_ACCOUNT_DUB_POINTS_URL);

        if( dubMap.get(dubPointId) == null ) {
            urlDubPointDetail = urlDubPointDetail.replace("{DUB_POINT_ID}", dubPointId);
            try {
                VoiceExtractorTimer.logger.debug("Calling the URL={},headers={}", urlDubPointDetail, entity);
                Thread.sleep(Integer.valueOf(VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_CALL_DELAY)));
                dubPoint = restTemplate.exchange(urlDubPointDetail, HttpMethod.GET, entity, DubPoint.class);
                dubMap.put(dubPointId,dubPoint.getBody());
            } catch (HttpClientErrorException ex) {
                VoiceExtractorTimer.logger.debug("Error processing dub.point.id={}:",dubPointId, ex);
            } catch (InterruptedException e) {
                VoiceExtractorTimer.logger.error("Error interrupting the current Thread.", e);
            }
        }

        return dubMap.get(dubPointId);
    }

    private String processProxy(String originalUrlKey) {
        return VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_PROXY).equalsIgnoreCase("yes") ?
               VoiceExtractorTimer.configMap.get(originalUrlKey).replace(VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_PROXY_URL_FROM), VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_PROXY_URL_TO)) :
                VoiceExtractorTimer.configMap.get(originalUrlKey);
    }

    private Account fetchAccountDetail(String accountAddress, HttpEntity<String> entity) {
        final ResponseEntity<Account> account;
        String urlAccountDetail = processProxy(DUBBER_ACCOUNT_URL);

        if( accountMap.get(accountAddress) == null ) {
            urlAccountDetail = urlAccountDetail.replace("{ACCOUNT_ID}", accountAddress);
            try {
                VoiceExtractorTimer.logger.debug("Calling the URL={},headers={}", urlAccountDetail, entity);
                Thread.sleep(Integer.valueOf(VoiceExtractorTimer.configMap.get(DUBBER_RECORDING_CALL_DELAY)));
                account = restTemplate.exchange(urlAccountDetail, HttpMethod.GET, entity, Account.class);
                accountMap.put(accountAddress,account.getBody());
            } catch (HttpClientErrorException ex) {
                VoiceExtractorTimer.logger.debug("Error processing account detail, account address={}:",accountAddress, ex);
            } catch (InterruptedException e) {
                VoiceExtractorTimer.logger.error("Error interrupting the current Thread.", e);
            }
        }

        return accountMap.get(accountAddress);
    }

    private Map<String,String> processSiteMapping() {
        return Arrays.asList(VoiceExtractorTimer.configMap.get(DUBBER_SITE_MAPPING).split(COMMA))
                .stream()
                .map(s->s.split(COLON))
                .collect(Collectors.toMap(s->s[0],s->s[1]));
    }
}