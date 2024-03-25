/*
* DynamoDBUtils.java
 */
package nz.co.sparkiot.dynamodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * DynamoDB utility class.
 * @author rod
 * @since 2019-02-13
 */
public final class DynamoDBUtils {
    private static final Logger log = LoggerFactory.getLogger(DynamoDBUtils.class);

    public static boolean hasTable(DynamoDbClient dynamoDbClient,String tableName) {
        boolean ret = true;
        try {
            final DescribeTableRequest request = DescribeTableRequest
                                                .builder()
                                                .tableName(tableName)
                                                .build();
            final DescribeTableResponse describeTableResponse = dynamoDbClient.describeTable(request);
            log.debug("Describe table response : {}",describeTableResponse);
        } catch (Throwable e) {
            log.debug("Caught exception : {}",e.getClass().getName());
            ret = false;
        }
        return ret;
    }
}