package liquibase.ext.dynamodb.change;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import liquibase.change.AbstractChange;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.dynamodb.database.DynamoDBDatabase;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import liquibase.change.ChangeMetaData;


@DatabaseChange(name = "putItem",
description = "Creates a new item " +
        "https://docs.aws.amazon.com/es_es/amazondynamodb/latest/APIReference/API_PutItem.html",
priority = ChangeMetaData.PRIORITY_DEFAULT)
@NoArgsConstructor
@Getter
@Setter
public class PutItemChange extends AbstractChange {

    private String tableName;
    private String itemJson;

    @Override
    public String getConfirmationMessage() {
        return "Item inserted into table " + getTableName();
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {

        DynamoDbClient dynamoDbClient = ((DynamoDBDatabase) database).getDynamoDbClient();
        
        Map<String, AttributeValue> itemValues = new HashMap<>();

        JsonObject jsonObject = JsonParser.parseString(itemJson).getAsJsonObject();
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            String key = entry.getKey();
            JsonElement value = entry.getValue();
            itemValues.put(key, AttributeValue.builder().s(value.getAsString()).build());
        }

        System.out.println(itemValues.toString());

        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(itemValues)
                .build();

        dynamoDbClient.putItem(putItemRequest);
        
        return new SqlStatement[0];
    }
    
}
