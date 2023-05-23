package liquibase.ext.dynamodb.database;

import java.sql.Driver;
import java.util.Properties;

import liquibase.exception.DatabaseException;
import liquibase.util.StringUtil;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import static java.util.Optional.ofNullable;
import static java.util.Objects.isNull;
import static liquibase.ext.dynamodb.database.DynamoDBDatabase.DYNAMODB_PRODUCT_SHORT_NAME;
import liquibase.nosql.database.AbstractNoSqlConnection;


@Getter
@Setter
@NoArgsConstructor
public class DynamoConnection extends AbstractNoSqlConnection {
	
    public static final int DEFAULT_PORT = 27017;
    public static final String DYNAMO_PREFIX = DYNAMODB_PRODUCT_SHORT_NAME + "+";
    
    protected DynamoDbClient dynamoDbClient;

    @Override
    public boolean supports(String url) {
        if (url == null) {
            return false;
        }
        return url.toLowerCase().startsWith(DYNAMODB_PRODUCT_SHORT_NAME);
    }

	@Override
	public String getCatalog() throws DatabaseException {
		try {
			return dynamoDbClient.serviceName();
		} catch (final Exception e) {
			throw new DatabaseException(e);
		}
	}
	
	@Override
	public String getDatabaseProductName() throws DatabaseException {
		return DynamoDBDatabase.DYNAMODB_PRODUCT_NAME;
	}
	
	@Override
	public String getURL() {
		return ofNullable(dynamoDbClient).map(DynamoDbClient::serviceName).orElse(null);
	}
	
	@Override
	public String getConnectionUserName() {
		return "sdk";
	}
	
	@Override
	public boolean isClosed() throws DatabaseException {
	 	return isNull(dynamoDbClient);
	}

	@Override
	public void open(String url, Driver driverObject, Properties driverProperties) throws DatabaseException {
		
        try {
			System.out.println("DynamoConnection.open() - driverProperties: " + driverProperties.toString());
			String endpoint = extractDynamoDBUrl(url);
			dynamoDbClient = ((DynamoClientDriver) driverObject).connect(endpoint);
						
			System.out.println(dynamoDbClient.describeEndpoints().toString());
        } catch (final Exception e) {
            throw new DatabaseException("Could not open connection to database: "
                    + ofNullable(dynamoDbClient).map(DynamoDbClient::serviceName).orElse(url), e);
        }
	}

	private String extractDynamoDBUrl(String input) {
		if (input == null || input.isEmpty()) {
			throw new IllegalArgumentException("Input cannot be null or empty");
		}
		
		String[] parts = input.split("\\+");
	
		if (parts.length != 2 || !parts[0].equals("dynamodb")) {
			throw new IllegalArgumentException("Invalid input format");
		}
	
		return parts[1];
	}

	@Override
	public void close() throws DatabaseException {
        try {
            if (!isClosed()) {
                dynamoDbClient.close();
                dynamoDbClient = null;
            }
        } catch (final Exception e) {
            throw new DatabaseException(e);
        }
	}
}
