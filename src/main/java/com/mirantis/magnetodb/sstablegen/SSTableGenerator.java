package com.mirantis.magnetodb.sstablegen;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.json.*;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.marshal.CompositeType.Builder;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.UUIDGen.decompose;

public class SSTableGenerator {

    static String filename;

    public static void main(String[] args) throws IOException {

        String keySpace = "cdrs";
        String table = "events";

        File directory = new File(keySpace + File.separator + table);

        if (!directory.exists()) {
            directory.mkdirs();
        }

        List<AbstractType<?>> compositeColumnValues = new ArrayList<AbstractType<?>>();
        compositeColumnValues.add(IntegerType.instance);
        compositeColumnValues.add(UTF8Type.instance);
        CompositeType compositeColumn = CompositeType
                .getInstance(compositeColumnValues);

        SSTableSimpleUnsortedWriter eventWriter = new SSTableSimpleUnsortedWriter(
                directory, new Murmur3Partitioner(), keySpace, table, compositeColumn,
                null, 8);

        long timestamp = System.currentTimeMillis() * 1000;


        for (int lineNumber = 0; lineNumber < 10; lineNumber++) {
            System.out.println(lineNumber);

            eventWriter.newRow(bytes(UUID.randomUUID().toString()));

            Builder builder = compositeColumn.builder();

// First column is a cql3 row marker
            builder.add(bytes(1));
            builder.add(bytes(""));
            eventWriter.addColumn(builder.build(), bytes(""), timestamp);

// Second column with a column value
            builder = compositeColumn.builder();
            builder.add(bytes(2));
            builder.add(bytes("f"));

            eventWriter.addColumn(
                    builder.build(),
                    bytes("value" + lineNumber),
                    timestamp);
        }

        System.out.println("a");

        eventWriter.close();

        System.out.println("b");

        System.exit(0);
    }

}