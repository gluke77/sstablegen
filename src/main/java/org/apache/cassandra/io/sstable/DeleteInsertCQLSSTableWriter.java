/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.Pair;

/**
 * Utility to write SSTables.
 * <p/>
 * Typical usage looks like:
 * <pre>
 *   String schema = "CREATE TABLE myKs.myTable ("
 *                 + "  k int PRIMARY KEY,"
 *                 + "  v1 text,"
 *                 + "  v2 int"
 *                 + ")";
 *   String insert = "INSERT INTO myKs.myTable (k, v1, v2) VALUES (?, ?, ?)";
 *
 *   // Creates a new writer. You need to provide at least the directory where to write the created sstable,
 *   // the schema for the sstable to write and a (prepared) insert statement to use. If you do not use the
 *   // default partitioner (Murmur3Partitioner), you will also need to provide the partitioner in use, see
 *   // DeleteInsertCQLSSTableWriter.Builder for more details on the available options.
 *   DeleteInsertCQLSSTableWriter writer = DeleteInsertCQLSSTableWriter.builder()
 *                                             .inDirectory("path/to/directory")
 *                                             .forTable(schema)
 *                                             .using(insert).build();
 *
 *   // Adds a nember of rows to the resulting sstable
 *   writer.addRow(0, "test1", 24);
 *   writer.addRow(1, "test2", null);
 *   writer.addRow(2, "test3", 42);
 *
 *   // Close the writer, finalizing the sstable
 *   writer.close();
 * </pre>
 */
public class DeleteInsertCQLSSTableWriter {
    private final AbstractSSTableSimpleWriter writer;
    private final UpdateStatement insert;
    private final DeleteStatement delete;
    private final List<ColumnSpecification> insertBoundNames;
    private final List<ColumnSpecification> deleteBoundNames;

    private DeleteInsertCQLSSTableWriter(AbstractSSTableSimpleWriter writer,
                                         UpdateStatement insert, List<ColumnSpecification> insertBoundNames,
                                         DeleteStatement delete, List<ColumnSpecification> deleteBoundNames) {
        this.writer = writer;
        this.insert = insert;
        this.insertBoundNames = insertBoundNames;
        this.delete = delete;
        this.deleteBoundNames = deleteBoundNames;
    }

    /**
     * Returns a new builder for a DeleteInsertCQLSSTableWriter.
     *
     * @return the new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Adds a new row to the writer.
     * <p/>
     * This is a shortcut for {@code addRow(Arrays.asList(values))}.
     *
     * @param values the row values (corresponding to the bind variables of the
     *               insertion statement used when creating by this writer).
     * @return this writer.
     */
    public DeleteInsertCQLSSTableWriter addRow(Object... values)
            throws InvalidRequestException, IOException {
        return addRow(Arrays.asList(values));
    }

    /**
     * Adds a new row to the writer.
     * <p/>
     * Each provided value type should correspond to the types of the CQL column
     * the value is for. The correspondance between java type and CQL type is the
     * same one than the one documented at
     * www.datastax.com/drivers/java/2.0/apidocs/com/datastax/driver/core/DataType.Name.html#asJavaClass().
     * <p/>
     * If you prefer providing the values directly as binary, use
     * {@link #rawAddRow} instead.
     *
     * @param values the row values (corresponding to the bind variables of the
     *               insertion statement used when creating by this writer).
     * @return this writer.
     */
    public DeleteInsertCQLSSTableWriter addRow(List<Object> values)
            throws InvalidRequestException, IOException {
        int size = Math.min(values.size(), insertBoundNames.size());
        List<ByteBuffer> rawValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            rawValues.add(values.get(i) == null ? null : ((AbstractType) insertBoundNames.get(i).type).decompose(
                    values.get(i)));
        }
        return rawAddRow(rawValues);
    }

    /**
     * Adds a new row to the writer.
     * <p/>
     * This is equivalent to the other addRow methods, but takes a map whose
     * keys are the names of the columns to add instead of taking a list of the
     * values in the order of the insert statement used during construction of
     * this write.
     * <p/>
     * Please note that the column names in the map keys must be in lowercase unless
     * the declared column name is a
     * <a href="http://cassandra.apache.org/doc/cql3/CQL.html#identifiers">case-sensitive quoted identifier</a>
     * (in which case the map key must use the exact case of the column).
     *
     * @param values a map of colum name to column values representing the new
     *               row to add. Note that if a column is not part of the map, it's value will
     *               be {@code null}. If the map contains keys that does not correspond to one
     *               of the column of the insert statement used when creating this writer, the
     *               the corresponding value is ignored.
     * @return this writer.
     */
    public DeleteInsertCQLSSTableWriter addRow(Map<String, Object> values)
            throws InvalidRequestException, IOException {
        int size = insertBoundNames.size();
        List<ByteBuffer> rawValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ColumnSpecification spec = insertBoundNames.get(i);
            Object value = values.get(spec.name.toString());
            rawValues.add(value == null ? null : ((AbstractType) spec.type).decompose(value));
        }
        return rawAddRow(rawValues);
    }

    /**
     * Adds a new row to the writer given already serialized values.
     * <p/>
     * This is a shortcut for {@code rawAddRow(Arrays.asList(values))}.
     *
     * @param values the row values (corresponding to the bind variables of the
     *               insertion statement used when creating by this writer) as binary.
     * @return this writer.
     */
    public DeleteInsertCQLSSTableWriter rawAddRow(List<ByteBuffer> values)
            throws InvalidRequestException, IOException {
        if (values.size() != insertBoundNames.size()) {
            throw new InvalidRequestException(
                    String.format("Invalid number of arguments, expecting %d values but got %d",
                            insertBoundNames.size(), values.size()));
        }

        long now = System.currentTimeMillis() * 1000 + 1;

        List<ByteBuffer> insertKeys = insert.buildPartitionKeyNames(values);
        ColumnNameBuilder insertClusteringPrefix = insert.createClusteringPrefixBuilder(values);

        UpdateParameters insertParams = new UpdateParameters(insert.cfm,
                values,
                insert.getTimestamp(now + 1, values),
                insert.getTimeToLive(values),
                Collections.<ByteBuffer, ColumnGroupMap>emptyMap());

        for (ByteBuffer key : insertKeys) {
            if (writer.currentKey() == null || !key.equals(writer.currentKey().key)) {
                writer.newRow(key);
            }
            insert.addUpdateForKey(writer.currentColumnFamily(), key, insertClusteringPrefix, insertParams);
        }
        return this;
    }

    /**
     * Adds a new row to the writer.
     * <p/>
     * This is a shortcut for {@code addRow(Arrays.asList(values))}.
     *
     * @param values the row values (corresponding to the bind variables of the
     *               insertion statement used when creating by this writer).
     * @return this writer.
     */
    public DeleteInsertCQLSSTableWriter deleteRow(Object... values)
            throws InvalidRequestException, IOException {
        return deleteRow(Arrays.asList(values));
    }

    /**
     * Adds a new row to the writer.
     * <p/>
     * Each provided value type should correspond to the types of the CQL column
     * the value is for. The correspondance between java type and CQL type is the
     * same one than the one documented at
     * www.datastax.com/drivers/java/2.0/apidocs/com/datastax/driver/core/DataType.Name.html#asJavaClass().
     * <p/>
     * If you prefer providing the values directly as binary, use
     * {@link #rawAddRow} instead.
     *
     * @param values the row values (corresponding to the bind variables of the
     *               insertion statement used when creating by this writer).
     * @return this writer.
     */
    public DeleteInsertCQLSSTableWriter deleteRow(List<Object> values)
            throws InvalidRequestException, IOException {
        int size = Math.min(values.size(), insertBoundNames.size());
        List<ByteBuffer> rawValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            rawValues.add(values.get(i) == null ? null : ((AbstractType) insertBoundNames.get(i).type).decompose(
                    values.get(i)));
        }
        return rawDeleteRow(rawValues);
    }

    /**
     * Adds a new row to the writer.
     * <p/>
     * This is equivalent to the other addRow methods, but takes a map whose
     * keys are the names of the columns to add instead of taking a list of the
     * values in the order of the insert statement used during construction of
     * this write.
     * <p/>
     * Please note that the column names in the map keys must be in lowercase unless
     * the declared column name is a
     * <a href="http://cassandra.apache.org/doc/cql3/CQL.html#identifiers">case-sensitive quoted identifier</a>
     * (in which case the map key must use the exact case of the column).
     *
     * @param values a map of colum name to column values representing the new
     *               row to add. Note that if a column is not part of the map, it's value will
     *               be {@code null}. If the map contains keys that does not correspond to one
     *               of the column of the insert statement used when creating this writer, the
     *               the corresponding value is ignored.
     * @return this writer.
     */
    public DeleteInsertCQLSSTableWriter deleteRow(Map<String, Object> values)
            throws InvalidRequestException, IOException {
        int size = insertBoundNames.size();
        List<ByteBuffer> rawValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ColumnSpecification spec = insertBoundNames.get(i);
            Object value = values.get(spec.name.toString());
            rawValues.add(value == null ? null : ((AbstractType) spec.type).decompose(value));
        }
        return rawDeleteRow(rawValues);
    }

    /**
     * Adds a new row to the writer given already serialized values.
     * <p/>
     * This is a shortcut for {@code rawAddRow(Arrays.asList(values))}.
     *
     * @param values the row values (corresponding to the bind variables of the
     *               insertion statement used when creating by this writer) as binary.
     * @return this writer.
     */
    public DeleteInsertCQLSSTableWriter rawDeleteRow(List<ByteBuffer> values)
            throws InvalidRequestException, IOException {
        if (values.size() != deleteBoundNames.size()) {
            throw new InvalidRequestException(
                    String.format("Invalid number of arguments, expecting %d values but got %d",
                            deleteBoundNames.size(), values.size()));
        }

        List<ByteBuffer> deleteKeys = delete.buildPartitionKeyNames(values);
        ColumnNameBuilder deleteClusteringPrefix = delete.createClusteringPrefixBuilder(values);

        long now = System.currentTimeMillis() * 1000;

        UpdateParameters deleteParams = new UpdateParameters(delete.cfm,
                values,
                delete.getTimestamp(now, values),
                delete.getTimeToLive(values),
                Collections.<ByteBuffer, ColumnGroupMap>emptyMap());

        for (ByteBuffer key : deleteKeys) {
            if (writer.currentKey() == null || !key.equals(writer.currentKey().key)) {
                writer.newRow(key);
            }
            delete.addUpdateForKey(writer.currentColumnFamily(), key, deleteClusteringPrefix, deleteParams);
        }

        return this;
    }

    /**
     * Close this writer.
     * <p/>
     * This method should be called, otherwise the produced sstables are not
     * guaranteed to be complete (and won't be in practice).
     */
    public void close() throws IOException {
        writer.close();
    }

    /**
     * A Builder for a DeleteInsertCQLSSTableWriter object.
     */
    public static class Builder {
        private File directory;
        private IPartitioner partitioner = new Murmur3Partitioner();

        private CFMetaData schema;
        private UpdateStatement insert;
        private DeleteStatement delete;
        private List<ColumnSpecification> insertBoundNames;
        private List<ColumnSpecification> deleteBoundNames;

        private boolean sorted = false;
        private long bufferSizeInMB = 128;

        private Builder() {
        }

        /**
         * The directory where to write the sstables.
         * <p/>
         * This is a mandatory option.
         *
         * @param directory the directory to use, which should exists and be writable.
         * @return this builder.
         * @throws IllegalArgumentException if {@code directory} doesn't exist or is not writable.
         */
        public Builder inDirectory(String directory) {
            return inDirectory(new File(directory));
        }

        /**
         * The directory where to write the sstables (mandatory option).
         * <p/>
         * This is a mandatory option.
         *
         * @param directory the directory to use, which should exists and be writable.
         * @return this builder.
         * @throws IllegalArgumentException if {@code directory} doesn't exist or is not writable.
         */
        public Builder inDirectory(File directory) {
            if (!directory.exists()) {
                throw new IllegalArgumentException(directory + " doesn't exists");
            }
            if (!directory.canWrite()) {
                throw new IllegalArgumentException(directory + " exists but is not writable");
            }

            this.directory = directory;
            return this;
        }

        /**
         * The schema (CREATE TABLE statement) for the table for which sstable are to be created.
         * <p/>
         * Please note that the provided CREATE TABLE statement <b>must</b> use a fully-qualified
         * table name, one that include the keyspace name.
         * <p/>
         * This is a mandatory option.
         *
         * @param schema the schema of the table for which sstables are to be created.
         * @return this builder.
         * @throws IllegalArgumentException if {@code schema} is not a valid CREATE TABLE statement
         *                                  or does not have a fully-qualified table name.
         */
        public Builder forTable(String schema) {
            try {
                this.schema = getStatement(schema, CreateTableStatement.class, "CREATE TABLE").left
                        .getCFMetaData()
                        .rebuild();

                // We need to register the keyspace/table metadata through Schema,
                // otherwise we won't be able to properly
                // build the insert statement in using().
                KSMetaData ksm = KSMetaData.newKeyspace(this.schema.ksName,
                        AbstractReplicationStrategy.getClass("org.apache.cassandra.locator.SimpleStrategy"),
                        ImmutableMap.of("replication_factor", "1"),
                        true,
                        Collections.singleton(this.schema));

                Schema.instance.load(ksm);
                return this;
            } catch (RequestValidationException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }

        /**
         * The partitioner to use.
         * <p/>
         * By default, {@code Murmur3Partitioner} will be used. If this is not the partitioner used
         * by the cluster for which the SSTables are created, you need to use this method to
         * provide the correct partitioner.
         *
         * @param partitioner the partitioner to use.
         * @return this builder.
         */
        public Builder withPartitioner(IPartitioner partitioner) {
            this.partitioner = partitioner;
            return this;
        }

        /**
         * The INSERT statement defining the order of the values to add for a given CQL row.
         * <p/>
         * Please note that the provided INSERT statement <b>must</b> use a fully-qualified
         * table name, one that include the keyspace name. Morewover, said statement must use
         * bind variables since it is those bind variables that will be bound to values by the
         * resulting writer.
         * <p/>
         * This is a mandatory option, and this needs to be called after foTable().
         *
         * @param insertStatement an insertion statement that defines the order
         *                        of column values to use.
         * @return this builder.
         * @throws IllegalArgumentException if {@code insertStatement} is not a valid insertion
         *                                  statement, does not have a fully-qualified table name or have no bind
         *                                  variables.
         */
        public Builder using(String insertStatement, String deleteStatement) {
            if (schema == null) {
                throw new IllegalStateException(
                        "You need to define the schema by calling forTable() prior to this call.");
            }

            Pair<UpdateStatement, List<ColumnSpecification>> up = getStatement(insertStatement, UpdateStatement.class,
                    "INSERT");
            this.insert = up.left;
            this.insertBoundNames = up.right;
            if (this.insert.hasConditions()) {
                throw new IllegalArgumentException("Conditional statements are not supported");
            }
            if (this.insertBoundNames.isEmpty()) {
                throw new IllegalArgumentException("Provided insert statement has no bind variables");
            }

            Pair<DeleteStatement, List<ColumnSpecification>> dp = getStatement(deleteStatement, DeleteStatement.class,
                    "DELETE");
            this.delete = dp.left;
            this.deleteBoundNames = dp.right;
            if (this.delete.hasConditions()) {
                throw new IllegalArgumentException("Conditional statements are not supported");
            }
            if (this.deleteBoundNames.isEmpty()) {
                throw new IllegalArgumentException("Provided insert statement has no bind variables");
            }

            return this;
        }

        /**
         * The size of the buffer to use.
         * <p/>
         * This defines how much data will be buffered before being written as
         * a new SSTable. This correspond roughly to the data size that will have the created
         * sstable.
         * <p/>
         * The default is 128MB, which should be reasonable for a 1GB heap. If you experience
         * OOM while using the writer, you should lower this value.
         *
         * @param size the size to use in MB.
         * @return this builder.
         */
        public Builder withBufferSizeInMB(int size) {
            this.bufferSizeInMB = size;
            return this;
        }

        /**
         * Creates a DeleteInsertCQLSSTableWriter that expects sorted inputs.
         * <p/>
         * If this option is used, the resulting writer will expect rows to be
         * added in SSTable sorted order (and an exception will be thrown if that
         * is not the case during insertion). The SSTable sorted order means that
         * rows are added such that their partition key respect the partitioner
         * order and for a given partition, that the rows respect the clustering
         * columns order.
         * <p/>
         * You should thus only use this option is you know that you can provide
         * the rows in order, which is rarely the case. If you can provide the
         * rows in order however, using this sorted might be more efficient.
         * <p/>
         * Note that if used, some option like withBufferSizeInMB will be ignored.
         *
         * @return this builder.
         */
        public Builder sorted() {
            this.sorted = true;
            return this;
        }

        private static <T extends CQLStatement> Pair<T, List<ColumnSpecification>> getStatement(String query,
                                                                                                Class<T> klass,
                                                                                                String type) {
            try {
                ClientState state = ClientState.forInternalCalls();
                ParsedStatement.Prepared prepared = QueryProcessor.getStatement(query, state);
                CQLStatement stmt = prepared.statement;
                stmt.validate(state);

                if (!stmt.getClass().equals(klass)) {
                    throw new IllegalArgumentException("Invalid query, must be a " + type + " statement");
                }

                return Pair.create(klass.cast(stmt), prepared.boundNames);
            } catch (RequestValidationException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }

        public DeleteInsertCQLSSTableWriter build() {
            if (directory == null) {
                throw new IllegalStateException(
                        "No ouptut directory specified, you should provide a directory with inDirectory()");
            }
            if (schema == null) {
                throw new IllegalStateException(
                        "Missing schema, you should provide the schema for the SSTable to create with forTable()");
            }
            if (insert == null || delete == null) {
                throw new IllegalStateException(
                        "Insert or delete statement not specified, you should provide a delete and an insert " +
                                "statement through using()");
            }

            AbstractSSTableSimpleWriter writer;
            if (sorted) {
                writer = new SSTableSimpleWriter(directory,
                        schema,
                        partitioner);
            } else {
                writer = new SSTableSimpleUnsortedWriter(directory,
                        schema,
                        partitioner,
                        bufferSizeInMB);
            }
            return new DeleteInsertCQLSSTableWriter(writer, insert, insertBoundNames, delete, deleteBoundNames);
        }
    }
}
