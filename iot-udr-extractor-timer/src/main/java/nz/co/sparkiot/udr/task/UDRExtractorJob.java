/*
* UDRExtractorJob.java
 */
package nz.co.sparkiot.udr.task;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import nz.co.sparkiot.dynamodb.DynamoDBUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * UDR Extractor Job.
 * @author rod
 * @since 2019-02-13
 */
@DisallowConcurrentExecution
public class UDRExtractorJob implements Job {
    private static final String FTP_INPUT_DIRECTORY = "ftp.input.directory";
    private static final String FTP_OUTPUT_DIRECTORY = "ftp.output.directory";
    private static final String FTP_SERVER = "ftp.server";
    private static final String FTP_PORT = "ftp.port";
    private static final String FTP_USER = "ftp.user";
    private static final String FTP_KEY_PATH = "ftp.key.path";
    private static final String DYNAMODB_WRITE_CAPACITY_UNITS = "dynamodb.write.capacity.units";
    private static final String DYNAMODB_READ_CAPACITY_UNITS = "dynamodb.read.capacity.units";
    private static final String UDR_EXTRACTOR = "actility-extractor";

    private static String MONTH_YEAR_PATTERN = ("^\\d{4}-\\d{2}$");

    public UDRExtractorJob() {
        super();
        final DynamoDbClient dynamoDbClient = DynamoDbClient.create();
        UDRExtractorTimer.logger.debug("Table {} exists {}",UDR_EXTRACTOR,DynamoDBUtils.hasTable(dynamoDbClient,UDR_EXTRACTOR));
        if( ! DynamoDBUtils.hasTable(dynamoDbClient,UDR_EXTRACTOR) ) {
            UDRExtractorTimer.logger.info("Creating DynamoDB table {}...",UDR_EXTRACTOR);
            try {
                final ProvisionedThroughput provisionedThroughput = ProvisionedThroughput
                .builder()
                .writeCapacityUnits(Long.parseLong(UDRExtractorTimer.configMap.get(DYNAMODB_WRITE_CAPACITY_UNITS)))
                .readCapacityUnits(Long.parseLong(UDRExtractorTimer.configMap.get(DYNAMODB_READ_CAPACITY_UNITS)))
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
                            .attributeName("period")
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
                            .attributeName("period")
                            .keyType(KeyType.RANGE)
                            .build())
                        .build();
                dynamoDbClient.createTable(createTableRequest);
                UDRExtractorTimer.logger.info("DynamoDB table {} has been created.",UDR_EXTRACTOR);
            } catch( DynamoDbException e ) {
                UDRExtractorTimer.logger.error("Error. Failed creating DynamoDB table {}.",UDR_EXTRACTOR,e);
            }
        }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        final SSHClient sshClient = new SSHClient();
        //final DynamoDbClient dynamoDbClient = DynamoDbClient.create();
        SFTPClient sftpClient = null;

        try {
            UDRExtractorTimer.logger.info("Running Actility UDR extraction task...");
            sshClient.loadKnownHosts();
            Path path = Paths.get(UDRExtractorTimer.configMap.get(FTP_KEY_PATH));
            UDRExtractorTimer.logger.debug("Path:{}, Exists:{}",path,Files.exists(path));
            sshClient.connect(UDRExtractorTimer.configMap.get(FTP_SERVER),Integer.valueOf(UDRExtractorTimer.configMap.get(FTP_PORT)));
            sshClient.authPublickey(UDRExtractorTimer.configMap.get(FTP_USER),UDRExtractorTimer.configMap.get(FTP_KEY_PATH));
            sftpClient = sshClient.newSFTPClient();
            final SFTPClient ftp = sftpClient;

            final List<RemoteResourceInfo> list = sftpClient.ls(UDRExtractorTimer.configMap.get(FTP_INPUT_DIRECTORY));
            list
            .stream()
            .filter(dir -> dir.getName().matches(MONTH_YEAR_PATTERN) && dir.isDirectory())
            .forEach(dir -> {
                Path pt = Paths.get(UDRExtractorTimer.configMap.get(FTP_OUTPUT_DIRECTORY) + File.separator + dir.getName());
                if( ! Files.exists(pt) ) {
                    try {
                        final List<RemoteResourceInfo> fileList = ftp.ls(dir.getPath());
                        Files.createDirectory(pt);
                        fileList.forEach(i -> {
                            try {
                                UDRExtractorTimer.logger.debug("Copying from {} to {}",i.getPath(),UDRExtractorTimer.configMap.get(FTP_OUTPUT_DIRECTORY) + "/" + dir.getName() + "/" + i.getName());
                                ftp.get(i.getPath(),UDRExtractorTimer.configMap.get(FTP_OUTPUT_DIRECTORY) + "/" + dir.getName() + "/" + i.getName());
                            } catch (IOException e) {
                                UDRExtractorTimer.logger.error("Error copying file {} to {}",i.getPath(),UDRExtractorTimer.configMap.get(FTP_OUTPUT_DIRECTORY) + "/" + dir.getName() + "/" + i.getName(),e);
                            }
                        });
                    } catch (IOException e) {
                        UDRExtractorTimer.logger.error("Error creating directory {}",pt,e);
                    }
                }
            });

            //inputStream = new FileInputStream(UDRExtractorTimer.configMap.get(OUTPUT_DIRECTORY)+ File.separator+fileName);
            //ftpClient.storeFile(fileName,inputStream);
            //ZipUtils.removeZip(fileName);

            UDRExtractorTimer.logger.info("Finished Actility UDR extraction task.");
        } catch (IOException e) {
            UDRExtractorTimer.logger.error("Error connecting to FTP server.", e);
        } catch (Throwable e) {
            UDRExtractorTimer.logger.error("General error.", e);
        } finally {
            try {
                if( sftpClient != null )
                    sftpClient.close();
                sshClient.disconnect();
            } catch (IOException e) {
                UDRExtractorTimer.logger.error("Error closing connections.", e);
            }
        }
    }
}
