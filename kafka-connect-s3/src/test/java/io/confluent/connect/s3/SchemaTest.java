package io.confluent.connect.s3;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SchemaTest {

    public static void main(String[] args) {
        Schema schema = SchemaBuilder.struct().name("record").version(1).field("boolean", Schema.BOOLEAN_SCHEMA)
                                     .field("int", Schema.INT32_SCHEMA).field("long", Schema.INT64_SCHEMA)
                                     .field("float", Schema.FLOAT32_SCHEMA).field("double", Schema.FLOAT64_SCHEMA)
                                     .build();
        Schema newSchema = SchemaBuilder.struct().name("record").version(2).field("boolean", Schema.BOOLEAN_SCHEMA)
                                        .field("int", Schema.INT32_SCHEMA).field("long", Schema.INT64_SCHEMA)
                                        .field("float", Schema.FLOAT32_SCHEMA).field("double", Schema.FLOAT64_SCHEMA)
                                        .field("string", SchemaBuilder.string().defaultValue("abc").build()).build();

        System.out.println(compareSchema(schema, newSchema));

    }

    private static boolean compareSchema(Schema schema, Schema newSchema) {
        List<Field> fields1 = schema.fields();
        List<Field> fields2 = newSchema.fields();
        Map<String, Field> fieldsMap1 = fields1.stream().collect(Collectors.toMap(Field::name, Function.identity()));
        Map<String, Field> fieldsMap2 = fields2.stream().collect(Collectors.toMap(Field::name, Function.identity()));

        if (fieldsMap1.keySet().containsAll(fieldsMap2.keySet())){
            Map<String, Field> correlatedMap = new HashMap<>(fieldsMap1);
            correlatedMap.keySet().retainAll(fieldsMap2.keySet());
            for (Map.Entry<String, Field> entry: correlatedMap.entrySet()){
                Field field1 = entry.getValue();
                Field field2 = fieldsMap2.get(entry.getKey());
                if (!compareFields(field1, field2)){
                    return false;
                }
            }
        }else {
            return false;
        }

        return true;
    }

    private static boolean compareFields(Field field1, Field field2) {
        if (field1 != null && field2 != null && field1.name().equals(field2.name())){
            if (!field1.schema().type().isPrimitive() && field1.schema().type() == Schema.Type.STRUCT){
                return compareSchema(field1.schema(), field2.schema());
            }else {
                return field1.schema().type() == field2.schema().type();
            }
        }
        return false;
    }
}
