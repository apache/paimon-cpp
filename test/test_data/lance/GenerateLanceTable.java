/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.TimeZone;

/** Generate a real Paimon table, including schemas, snapshots and manifests. */
public class GenerateLanceTable {
    private static final RowType NESTED =
            RowType.of(
                    new org.apache.paimon.types.DataType[] {
                        DataTypes.INT(), DataTypes.STRING()
                    },
                    new String[] {"number", "label"});

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2
                || (args.length == 2 && !"null-parent".equals(args[1]))) {
            throw new IllegalArgumentException("Usage: GenerateLanceTable NEW_WAREHOUSE [null-parent]");
        }
        // Never overwrite a checked-in fixture or an existing warehouse.
        if (Files.exists(Paths.get(args[0]))) {
            throw new IllegalArgumentException("Output must not exist: " + args[0]);
        }
        boolean nullParent = args.length == 2;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Options options = new Options();
        options.set("warehouse", Paths.get(args[0]).toAbsolutePath().toUri().toString());
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("append_java_compat", false);
            Identifier id = Identifier.create("append_java_compat", "append_java_compat");
            Schema schema = Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("f_boolean", DataTypes.BOOLEAN())
                    .column("f_tinyint", DataTypes.TINYINT())
                    .column("f_smallint", DataTypes.SMALLINT())
                    .column("f_bigint", DataTypes.BIGINT())
                    .column("f_float", DataTypes.FLOAT())
                    .column("f_double", DataTypes.DOUBLE())
                    .column("f_char", DataTypes.CHAR(8))
                    .column("f_string", DataTypes.STRING())
                    .column("f_binary", DataTypes.BINARY(4))
                    .column("f_varbinary", DataTypes.BYTES())
                    .column("f_date", DataTypes.DATE())
                    .column("f_ts_0", DataTypes.TIMESTAMP(0))
                    .column("f_ts_3", DataTypes.TIMESTAMP(3))
                    .column("f_ts_6", DataTypes.TIMESTAMP(6))
                    .column("f_ts_9", DataTypes.TIMESTAMP(9))
                    .column("f_decimal_1_0", DataTypes.DECIMAL(1, 0))
                    .column("f_decimal_18_2", DataTypes.DECIMAL(18, 2))
                    .column("f_decimal_19_2", DataTypes.DECIMAL(19, 2))
                    .column("f_decimal_38_18", DataTypes.DECIMAL(38, 18))
                    .column("f_array_int", DataTypes.ARRAY(DataTypes.INT()))
                    .column("f_array_array_int", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))
                    .column("f_struct", NESTED.notNull())
                    .column("f_nullable_struct", NESTED)
                    .column("f_vector", DataTypes.VECTOR(3, DataTypes.FLOAT()))
                    .option("file.format", "lance")
                    .option("bucket", "-1")
                    .option("metadata.stats-mode", "none")
                    .option("fields.id.stats-mode", "full")
                    .build();
            catalog.createTable(id, schema, false);
            Table table = catalog.getTable(id);
            InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
            // Separate commits ensure a predicate can prune an entire data file.
            for (int first : new int[] {1, 10}) {
                BatchWriteBuilder builder = table.newBatchWriteBuilder();
                try (BatchTableWrite write = builder.newWrite();
                        BatchTableCommit commit = builder.newCommit()) {
                    write.write(row(first, false));
                    write.write(row(first + 1, nullParent));
                    commit.commit(write.prepareCommit());
                }
            }
            ReadBuilder readBuilder = table.newReadBuilder();
            int count = 0;
            try (CloseableIterator<InternalRow> rows = readBuilder.newRead()
                    .createReader(readBuilder.newScan().plan()).toCloseableIterator()) {
                while (rows.hasNext()) {
                    InternalRow actual = rows.next();
                    int rowId = actual.getInt(0);
                    boolean expectedNullParent = nullParent && (rowId == 2 || rowId == 11);
                    if (nullParent) {
                        System.out.println("id=" + rowId + " expected null parent="
                                + expectedNullParent + " actual null parent=" + actual.isNullAt(23));
                    }
                    if (!serializer.toBinaryRow(actual).copy()
                            .equals(serializer.toBinaryRow(row(rowId, expectedNullParent)).copy())) {
                        throw new AssertionError("Java round trip mismatch for id " + rowId);
                    }
                    count++;
                }
            }
            if (count != 4) {
                throw new AssertionError("Expected 4 rows, got " + count);
            }
            System.out.println("Verified Java table round trip: 4 rows, 25 columns, 2 commits");
        }
    }

    private static GenericRow row(int id, boolean nullParent) {
        if (id == 2) {
            GenericRow row = new GenericRow(25);
            row.setField(0, id);
            row.setField(22, GenericRow.of(null, null));
            row.setField(23, nullParent ? null : GenericRow.of(null, null));
            return row;
        }
        return GenericRow.of(
                id, id % 2 == 1, (byte) -5, (short) -1000, 10000000000L + id,
                1.25f, -2.5d, BinaryString.fromString("char0001"),
                BinaryString.fromString(id == 10 ? "" : "value-" + id),
                "bin1".getBytes(StandardCharsets.UTF_8), new byte[] {0, 1, 2, 127},
                id == 1 ? -1 : 20000,
                Timestamp.fromEpochMillis(1000),
                Timestamp.fromEpochMillis(1123),
                Timestamp.fromEpochMillis(1123, 456000),
                Timestamp.fromEpochMillis(1123, 456789),
                Decimal.fromBigDecimal(new BigDecimal("-3"), 1, 0),
                Decimal.fromBigDecimal(new BigDecimal("1.25"), 18, 2),
                Decimal.fromBigDecimal(new BigDecimal("12345678901234567.89"), 19, 2),
                Decimal.fromBigDecimal(
                        new BigDecimal("12345678901234567890.123456789012345678"), 38, 18),
                new GenericArray(id == 10 ? new Integer[] {} : new Integer[] {id, null, -id}),
                new GenericArray(new GenericArray[] {
                    new GenericArray(new Integer[] {id, null}), null,
                    new GenericArray(new Integer[] {})
                }),
                GenericRow.of(id, BinaryString.fromString("required")),
                nullParent ? null : GenericRow.of(id, BinaryString.fromString("nullable")),
                BinaryVector.fromPrimitiveArray(new float[] {id, -1.5f, 0.25f}));
    }
}
