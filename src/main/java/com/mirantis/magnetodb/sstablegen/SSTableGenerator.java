package com.mirantis.magnetodb.sstablegen;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class SSTableGenerator {

    public static void main(String[] args) throws IOException {

        String keySpace = "cdrs";
        String table = "events";

        String schema = "create table " + keySpace + "." + table +
                " (id text, range text," +
                " fstr text, fnum int, fblob blob," +
                " fsstr set<text>, fmap map<text, int>," +
                " primary key (id, range))";

        String insert = "insert into " + keySpace + "." + table +
                " (id, range, fstr, fnum, fblob, fsstr, fmap)" +
                " values(?, ?, ?, ?, ?, ?, ?)";

        String pathname = keySpace + File.separator + table;
        File directory = new File(pathname);

        if (!directory.exists()) {
            directory.mkdirs();
        }

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .forTable(schema)
                .inDirectory(pathname)
                .using(insert)
                .withBufferSizeInMB(64)
                .build();

        for (int lineNumber = 0; lineNumber < 10; lineNumber++) {
            System.out.println(lineNumber);

            String id = "id" + lineNumber;

            Set<String> fsstr = new HashSet<>();
            fsstr.add("val1");
            fsstr.add("val2");

            Map<String, Integer> map = new HashMap<>();
            map.put("f1", 1);
            map.put("f2", 2);

            try {
                writer.addRow(id, "r1", "value" + lineNumber, lineNumber, bytes("value" + lineNumber), fsstr, map);
            } catch (InvalidRequestException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

            try {
                writer.addRow(id, "r2", "value" + lineNumber, lineNumber, bytes("value" + lineNumber), fsstr, map);
            } catch (InvalidRequestException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

        }

        writer.close();

        System.exit(0);
    }

}