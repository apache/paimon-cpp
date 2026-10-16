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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/** Generates the Java-written table consumed by the C++ interoperability test. */
public class GenerateDedicatedVectorFixture {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expected an empty warehouse directory argument");
        }
        Options options = new Options();
        options.set("warehouse", args[0]);
        Identifier identifier =
                Identifier.create("dedicated_vector_java_compat", "dedicated_vector_java_compat");
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase(identifier.getDatabaseName(), false);
            Schema schema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("embedding", DataTypes.VECTOR(3, DataTypes.FLOAT()))
                            .column("tag", DataTypes.STRING())
                            .option("file.format", "parquet")
                            .option("vector.file.format", "vortex")
                            .option("bucket", "-1")
                            .option("row-tracking.enabled", "true")
                            .option("data-evolution.enabled", "true")
                            .option("write-only", "true")
                            .build();
            catalog.createTable(identifier, schema, false);
            Table table = catalog.getTable(identifier);
            append(
                    table,
                    row(1, new float[] {1, 2, 3}, "a"),
                    row(2, null, "b"),
                    row(3, new float[] {-1, 0, 1}, "c"));
            append(
                    table,
                    row(4, null, "d"),
                    row(5, new float[] {7, 8, 9}, "e"),
                    row(6, new float[] {10, 11, 12}, "f"));

            ReadBuilder read = table.newReadBuilder();
            Map<Integer, String> actual = new TreeMap<>();
            try (CloseableIterator<InternalRow> rows =
                    read.newRead().createReader(read.newScan().plan()).toCloseableIterator()) {
                while (rows.hasNext()) {
                    InternalRow row = rows.next();
                    String vector =
                            row.isNullAt(1)
                                    ? "null"
                                    : Arrays.toString(row.getVector(1).toFloatArray());
                    actual.put(row.getInt(0), vector + ":" + row.getString(2));
                }
            }
            Map<Integer, String> expected = new TreeMap<>();
            expected.put(1, "[1.0, 2.0, 3.0]:a");
            expected.put(2, "null:b");
            expected.put(3, "[-1.0, 0.0, 1.0]:c");
            expected.put(4, "null:d");
            expected.put(5, "[7.0, 8.0, 9.0]:e");
            expected.put(6, "[10.0, 11.0, 12.0]:f");
            if (!actual.equals(expected)) {
                throw new AssertionError("Unexpected Java readback: " + actual);
            }
            System.out.println("Java readback verified: " + actual);
            Map<Long, Integer> ranges = new TreeMap<>();
            for (ManifestEntry entry : ((FileStoreTable) table).store().newScan().plan().files()) {
                DataFileMeta file = entry.file();
                if (file.rowCount() != 3) {
                    throw new AssertionError("Unexpected file row count: " + file);
                }
                ranges.merge(file.nonNullFirstRowId(), 1, Integer::sum);
                System.out.println(
                        file.fileName()
                                + " firstRowId="
                                + file.firstRowId()
                                + " rowCount="
                                + file.rowCount()
                                + " writeCols="
                                + file.writeCols());
            }
            Map<Long, Integer> expectedRanges = new TreeMap<>();
            expectedRanges.put(0L, 2);
            expectedRanges.put(3L, 2);
            if (!ranges.equals(expectedRanges)) {
                throw new AssertionError("Expected one main/vector pair per commit: " + ranges);
            }
        }
    }

    private static GenericRow row(int id, float[] vector, String tag) {
        return GenericRow.of(
                id,
                vector == null ? null : BinaryVector.fromPrimitiveArray(vector),
                BinaryString.fromString(tag));
    }

    private static void append(Table table, InternalRow... rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }
}
