package liquibase.ext.dynamodb.database;

import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import liquibase.Scope;
import liquibase.exception.DatabaseException;
import liquibase.util.StringUtil;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoClientDriver implements Driver {

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
        throw new UnsupportedOperationException("Cannot initiate a SQL Connection for a DynamoDB");
	}

	public DynamoDbClient connect(final String url) throws DatabaseException {
        final DynamoDbClient client;
        try {
			DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

			client = DynamoDbClient.builder()
			.endpointOverride(URI.create(url))
			.credentialsProvider(credentialsProvider)
			.build();

        } catch (final Exception e) {
            throw new DatabaseException("Connection could not be established to: "
                    + url, e);
        }
        return client;
    }

	@Override
	public boolean acceptsURL(String url) throws SQLException {
        final String trimmedUrl = StringUtil.trimToEmpty(url);
        return trimmedUrl.startsWith(DynamoConnection.DYNAMO_PREFIX);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
	}

	@Override
	public int getMajorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMinorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return (Logger) Scope.getCurrentScope().getLog(getClass());
	}

}
