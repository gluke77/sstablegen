package com.mirantis.magnetodb.sstablegen;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.DeleteInsertCQLSSTableWriter;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.ByteBufferUtil.string;

public class SSTableGenerator {

    public static final String USER_PREFIX = "user_";

    public static final String INDEX_NAME = "index_name";
    public static final String INDEX_VALUE_STRING = "index_value_string";
    public static final String INDEX_VALUE_NUMBER = "index_value_number";
    public static final String INDEX_VALUE_BLOB = "index_value_blob";

    public static final String ATTR_EXIST = "attr_exist";
    public static final String EXTRA_ATTR_DATA = "extra_attr_data";
    public static final String EXTRA_ATTR_TYPES = "extra_attr_types";

    protected static final Map<String, String> JSON_TO_CAS_TYPES = new HashMap<String, String>(){{
        put("S", "text");
        put("N", "decimal");
        put("B", "blob");
        put("SS", "set<text>");
        put("NS", "set<decimal>");
        put("BS", "set<blob>");
    }};

    private DeleteInsertCQLSSTableWriter writer;

    private String indexName;
    private String indexType;
    private String hash;
    private String range;

    public SSTableGenerator(String keyspace, String table, String hash, String range,
                            String indexName, Map<String, String> attrMap) {
        String pathname = USER_PREFIX + keyspace + File.separator + USER_PREFIX + table;
        File directory = new File(pathname);

        if (!directory.exists()) {
            directory.mkdirs();
        }

        this.writer = DeleteInsertCQLSSTableWriter.builder()
                .forTable(makeSchema(keyspace, table, hash, range, attrMap))
                .inDirectory(pathname)
                .using(makeInsert(keyspace, table, attrMap.keySet()),
                        makeDelete(keyspace, table, hash, range))
                .withBufferSizeInMB(64)
                .build();

        this.indexName = indexName;
        this.indexType = attrMap.get(indexName);
        this.hash = hash;
        this.range = range;
    }

    public void close() throws IOException {
        this.writer.close();
    }

    public void writeRow(Map<String, Object> row) throws InvalidRequestException, IOException {
        if (StringUtils.isNotBlank(indexName)) {
            if (StringUtils.isNotBlank(range)) {
                writer.deleteRow(row.get(USER_PREFIX + hash), row.get(USER_PREFIX + range));
            } else {
                writer.deleteRow(row.get(USER_PREFIX + hash));
            }
        }

        row.put(INDEX_NAME, "");
        row.put(INDEX_VALUE_BLOB, ByteBuffer.allocate(1).put((byte) 0));
        row.put(INDEX_VALUE_STRING, "");
        row.put(INDEX_VALUE_NUMBER, new BigDecimal(0));

        writer.addRow(row);

        if (StringUtils.isNotBlank(indexName)) {
            row.put(INDEX_NAME, indexName);

            switch (indexType) {
                case "S":
                    row.put(INDEX_VALUE_STRING, row.get(USER_PREFIX + indexName));
                    break;
                case "N":
                    row.put(INDEX_VALUE_NUMBER, row.get(USER_PREFIX + indexName));
                    break;
                case "B":
                    row.put(INDEX_VALUE_BLOB, row.get(USER_PREFIX + indexName));
                    break;
            }

            writer.addRow(row);
        }
    }

    protected String makeDelete(String keyspace, String table, String hash, String range) {
        StringBuilder delete = new StringBuilder();

        delete.append("DELETE FROM ");

        delete.append(USER_PREFIX + keyspace);
        delete.append(".");
        delete.append(USER_PREFIX + table);
        delete.append(" WHERE ");
        delete.append(USER_PREFIX + hash);
        delete.append(" = ?");

        if (StringUtils.isNotBlank(range)) {
            delete.append(" AND " + USER_PREFIX + range + " = ?");
        }

        return delete.toString();
    }

    protected String makeInsert(String keyspace, String table, Collection<String> attrs) {
        StringBuilder insert = new StringBuilder();

        insert.append("INSERT INTO ");
        insert.append(USER_PREFIX + keyspace);
        insert.append(".");
        insert.append(USER_PREFIX + table);
        insert.append(" (");

        for (String attr: attrs) {
            insert.append(USER_PREFIX + attr + ", ");
        }

        insert.append(INDEX_NAME +", ");
        insert.append(INDEX_VALUE_STRING +", ");
        insert.append(INDEX_VALUE_NUMBER +", ");
        insert.append(INDEX_VALUE_BLOB +", ");
        insert.append(ATTR_EXIST +", ");
        insert.append(EXTRA_ATTR_DATA +", ");
        insert.append(EXTRA_ATTR_TYPES);

        insert.append(") VALUES (");

        for (String attr: attrs) {
            insert.append("?, ");
        }

        insert.append("?, ?, ?, ?, ?, ?, ?)");

        return insert.toString();
    }

    protected String makeSchema(String keyspace, String table, String hash, String range, Map<String, String> attr_schema) {
        StringBuilder schema = new StringBuilder();

        schema.append("CREATE TABLE ");
        schema.append(USER_PREFIX + keyspace);
        schema.append(".");
        schema.append(USER_PREFIX + table);
        schema.append(" (");

        for (Map.Entry<String, String> attr: attr_schema.entrySet()) {
            schema.append(USER_PREFIX + attr.getKey() + " ");
            schema.append(JSON_TO_CAS_TYPES.get(attr.getValue()));
            schema.append(", ");
        }

        schema.append(INDEX_NAME + " text, ");
        schema.append(INDEX_VALUE_STRING + " text, ");
        schema.append(INDEX_VALUE_NUMBER + " decimal, ");
        schema.append(INDEX_VALUE_BLOB + " blob, ");
        schema.append(EXTRA_ATTR_DATA + " map<text, blob>, ");
        schema.append(EXTRA_ATTR_TYPES + " map<text, text>, ");
        schema.append(ATTR_EXIST + " set<text>, ");

        schema.append("PRIMARY KEY (");
        schema.append(USER_PREFIX + hash +", ");

        if (StringUtils.isNotBlank(range)) {
            schema.append(USER_PREFIX + range + ", ");
        }

        schema.append(INDEX_NAME + ", ");
        schema.append(INDEX_VALUE_STRING + ", ");
        schema.append(INDEX_VALUE_NUMBER + ", ");
        schema.append(INDEX_VALUE_BLOB + "))");

        return schema.toString();
    }

}