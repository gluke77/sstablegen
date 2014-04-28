package com.mirantis.magnetodb.sstablegen;

import org.apache.cassandra.exceptions.InvalidRequestException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Runner {

    public static void main(String[] args) throws IOException {
        if (args.length != 7) {
            System.out.println(
                    "Usage: java -Dcassandra-foreground" +
                            " -Dcassandra.config=file://<path_to_cassandra_yaml>" +
                            " -ea -Xmx1G com.mirantis.magnetodb.sstablegen.Runner" +
                            " <keyspaceName> <tableName> <hashName> <indexName> <tableDefFile> <dataFile> <count>");
        }

        String keyspace = args[0];
        String table = args[1];
        String hash = args[2];
        String range = null;
        String index = args[3];
        String tableDefFile = args[4];
        String dataFile = args[5];
        int count = Integer.parseInt(args[6]);

        BufferedReader tableDefReader = new BufferedReader(new FileReader(tableDefFile));
        String line = null;

        Map<String, String> attrMap = new HashMap<>();
        List<String> attrNames = new ArrayList<>();


        while ((line = tableDefReader.readLine()) != null) {
            String[] attr = line.split(" ");
//            attrMap.put(attr[0].trim(), attr[1].trim());
            attrMap.put(attr[0].trim(), "S");
            attrNames.add(attr[0].trim());
        }

        SSTableGenerator writer = new SSTableGenerator(keyspace, table, hash, range, index, attrMap);

        long lineNo = 0;

        DateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");

        for (int chunkNo = 0; chunkNo < count; chunkNo++) {

            BufferedReader dataReader = new BufferedReader(new FileReader(dataFile));

            while ((line = dataReader.readLine()) != null) {
                String[] attrValues = line.split("\u0001");

                Map<String, Object> row = new HashMap<>();

                for (int i = 0; i < attrNames.size(); i++) {

                    String name = attrNames.get(i);
                    String rawValue = attrValues[i];

                    if (hash.equals(name)) {
                        rawValue = UUID.randomUUID().toString() + "::" + rawValue;
                    }

                    String type = attrMap.get(name);

                    row.put(SSTableGenerator.USER_PREFIX + name, rawValue);
                }


                try {
                    writer.writeRow(row);
                } catch (InvalidRequestException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

                if (lineNo % 10000 == 0) {
                    System.out.println(lineNo);
                }

                lineNo++;
            }
        }

        writer.close();
        System.exit(0);

    }
}
