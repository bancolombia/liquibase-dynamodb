package liquibase.ext.dynamodb.database;

import liquibase.CatalogAndSchema;
import liquibase.exception.LiquibaseException;
import liquibase.nosql.database.AbstractNoSqlDatabase;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;


@NoArgsConstructor
public class DynamoDBDatabase extends AbstractNoSqlDatabase {
	
    public static final String DYNAMODB_PRODUCT_NAME = "DynamoDB";
    public static final String DYNAMODB_PRODUCT_SHORT_NAME = "dynamodb";
    public static final String ADMIN_DATABSE_NAME = "admin";
    
    @Override
    public void dropDatabaseObjects(CatalogAndSchema schemaToDrop) throws LiquibaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'dropDatabaseObjects'");
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith(DynamoConnection.DYNAMO_PREFIX)) {
            return DynamoClientDriver.class.getName();
        }
        return null;
    }

    public DynamoDbClient getDynamoDbClient() {
        return ((DynamoConnection) getConnection()).getDynamoDbClient();
    }

    @Override
    public String getDatabaseProductName() {
        return DYNAMODB_PRODUCT_NAME;
    }

    @Override
    public String getShortName() {
        return DYNAMODB_PRODUCT_SHORT_NAME;
    }

    @Override
    public Integer getDefaultPort() {
        return 8000;
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return DYNAMODB_PRODUCT_NAME;
    }
    
    @Override
    public String getSystemSchema() {
        return ADMIN_DATABSE_NAME;
    }
}
