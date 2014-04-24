package com.mirantis.magnetodb.sstablegen;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.InsertDeleteCQLSSTableWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeleteSSTableGenerator {

    public static void main(String[] args) throws IOException {

        String keySpace = "cdrs";
        String table = "events";

        String schema = "create table " + keySpace + "." + table +
                " (id text, range text," +
                " fstr text, fnum int, fblob blob," +
                " fsstr set<text>, fmap map<text, int>," +
                " primary key (id, range))";

        String insert = "delete from " + keySpace + "." + table +
                " where id = ?";

        String pathname = keySpace + File.separator + table;
        File directory = new File(pathname);

        if (!directory.exists()) {
            directory.mkdirs();
        }

        InsertDeleteCQLSSTableWriter writer = InsertDeleteCQLSSTableWriter.builder()
                .forTable(schema)
                .inDirectory(pathname)
                .using(insert)
                .withBufferSizeInMB(64)
                .sorted()
                .build();

        for (int lineNumber = 0; lineNumber < 10; lineNumber += 2) {
            System.out.println(lineNumber);

            List<Object> row = new ArrayList<>();

            String id = "id" + lineNumber;
            row.add(id);

            try {
                writer.addRow(row);
            } catch (InvalidRequestException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        writer.close();

        System.exit(0);
    }
}
