/*
* TPDeviceExtractorJob.java
 */
package nz.co.sparkiot.device.task;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import nz.co.sparkiot.dynamodb.DynamoDBUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * ThinkPark Device Extractor Job.
 * @author rod
 * @since 2019-02-18
 */
@DisallowConcurrentExecution
public class TPDeviceExtractorJob implements Job {
    private static final String FTP_INPUT_DIRECTORY = "ftp.input.directory";
    private static final String FTP_OUTPUT_DIRECTORY = "ftp.output.directory";
    private static final String FTP_SERVER = "ftp.server";
    private static final String FTP_PORT = "ftp.port";
    private static final String FTP_USER = "ftp.user";
    private static final String FTP_KEY_PATH = "ftp.key.path";
    private static final String DYNAMODB_WRITE_CAPACITY_UNITS = "dynamodb.write.capacity.units";
    private static final String DYNAMODB_READ_CAPACITY_UNITS = "dynamodb.read.capacity.units";
    private static final String UDR_EXTRACTOR = "udr-extractor";
    private static final String DEVICE_TABLE = "device";

    public TPDeviceExtractorJob() {
        super();
        final DynamoDbClient dynamoDbClient = DynamoDbClient.create();
        TPDeviceExtractorTimer.logger.debug("Table {} exists {}",UDR_EXTRACTOR,DynamoDBUtils.hasTable(dynamoDbClient,UDR_EXTRACTOR));
        if( ! DynamoDBUtils.hasTable(dynamoDbClient,UDR_EXTRACTOR) ) {
            TPDeviceExtractorTimer.logger.info("Creating DynamoDB table {}...",UDR_EXTRACTOR);
            try {
                final ProvisionedThroughput provisionedThroughput = ProvisionedThroughput
                .builder()
                .writeCapacityUnits(Long.parseLong(TPDeviceExtractorTimer.configMap.get(DYNAMODB_WRITE_CAPACITY_UNITS)))
                .readCapacityUnits(Long.parseLong(TPDeviceExtractorTimer.configMap.get(DYNAMODB_READ_CAPACITY_UNITS)))
                .build();
                final CreateTableRequest createTableRequest = CreateTableRequest
                        .builder()
                        .tableName(UDR_EXTRACTOR)
                        .provisionedThroughput(provisionedThroughput)
                        .attributeDefinitions(
                            AttributeDefinition
                            .builder()
                            .attributeName("entity")
                            .attributeType(ScalarAttributeType.S)
                            .build(),
                             AttributeDefinition
                            .builder()
                            .attributeName("reference")
                            .attributeType(ScalarAttributeType.S)
                            .build())
                        .keySchema(
                             KeySchemaElement
                             .builder()
                             .attributeName("entity")
                             .keyType(KeyType.HASH)
                             .build(),
                             KeySchemaElement
                            .builder()
                            .attributeName("reference")
                            .keyType(KeyType.RANGE)
                            .build())
                        .build();
                dynamoDbClient.createTable(createTableRequest);
                TPDeviceExtractorTimer.logger.info("DynamoDB table {} has been created.",UDR_EXTRACTOR);
            } catch( DynamoDbException e ) {
                TPDeviceExtractorTimer.logger.error("Error. Failed creating DynamoDB table {}.",UDR_EXTRACTOR,e);
            }
        }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        final SSHClient sshClient = new SSHClient();
        //final DynamoDbClient dynamoDbClient = DynamoDbClient.create();
        SFTPClient sftpClient = null;

        try {
            TPDeviceExtractorTimer.logger.info("Running Actility UDR extraction task...");
            sshClient.loadKnownHosts();
            Path path = Paths.get(TPDeviceExtractorTimer.configMap.get(FTP_KEY_PATH));
            TPDeviceExtractorTimer.logger.debug("Path:{}, Exists:{}",path,Files.exists(path));
            sshClient.connect(TPDeviceExtractorTimer.configMap.get(FTP_SERVER),Integer.valueOf(TPDeviceExtractorTimer.configMap.get(FTP_PORT)));
            sshClient.authPublickey(TPDeviceExtractorTimer.configMap.get(FTP_USER), TPDeviceExtractorTimer.configMap.get(FTP_KEY_PATH));
            sftpClient = sshClient.newSFTPClient();

            final List<RemoteResourceInfo> list = sftpClient.ls(TPDeviceExtractorTimer.configMap.get(FTP_INPUT_DIRECTORY));
            list.stream().forEach(System.out::println);
            //inputStream = new FileInputStream(TPDeviceExtractorTimer.configMap.get(OUTPUT_DIRECTORY)+ File.separator+fileName);
            //ftpClient.storeFile(fileName,inputStream);
            //ZipUtils.removeZip(fileName);

            TPDeviceExtractorTimer.logger.info("Finished Actility UDR extraction task.");
        } catch (IOException e) {
            TPDeviceExtractorTimer.logger.error("Error connecting to FTP server.", e);
        } catch (Throwable e) {
            TPDeviceExtractorTimer.logger.error("General error.", e);
        } finally {
            try {
                if( sftpClient != null )
                    sftpClient.close();
                if( sshClient != null )
                    sshClient.disconnect();
            } catch (IOException e) {
            }
        }
    }
}
