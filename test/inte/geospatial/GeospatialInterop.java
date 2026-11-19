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

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.ParquetSchemaConverter;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.EdgeAlgorithm;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GeospatialInterop {
    private static final byte[] COLLECTION = {1, 7, 0, 0, 0, 0, 0, 0, 0};
    private static final byte[] POINT = point(1, 2);
    private static final byte[] EMPTY_POINT = point(Double.NaN, Double.NaN);
    private static final String[] EDGES = {"spherical", "vincenty", "thomas", "andoyer", "karney"};

    private static byte[] point(double x, double y) {
        return ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN)
                .put((byte) 1).putInt(1).putDouble(x).putDouble(y).array();
    }

    private static RowType schema() {
        DataType geometry = DataTypes.GEOMETRY("EPSG:3857");
        List<DataField> fields = new ArrayList<>();
        fields.add(DataTypes.FIELD(0, "geom", geometry));
        for (String edge : EDGES) {
            fields.add(DataTypes.FIELD(fields.size(), edge,
                    DataTypes.GEOGRAPHY("EPSG:4326", EdgeAlgorithm.fromName(edge))));
        }
        fields.add(DataTypes.FIELD(6, "nested", DataTypes.ROW(
                DataTypes.FIELD(20, "g", geometry), DataTypes.FIELD(21, "h", fields.get(5).type()))));
        fields.add(DataTypes.FIELD(7, "arr", DataTypes.ARRAY(geometry)));
        fields.add(DataTypes.FIELD(8, "mapping", DataTypes.MAP(geometry, fields.get(5).type())));
        fields.add(DataTypes.FIELD(9, "bytes", DataTypes.BYTES()));
        return new RowType(fields);
    }

    private static byte[] value(int row) {
        return row == 0 ? COLLECTION : row == 1 ? null : POINT;
    }

    private static void write(FileFormat format, LocalFileIO io, Path path, RowType schema)
            throws Exception {
        try (PositionOutputStream out = io.newOutputStream(path, false);
                FormatWriter writer = format.createWriterFactory(schema).create(out, "uncompressed")) {
            for (int i = 0; i < 3; i++) {
                byte[] wkb = value(i);
                GenericRow row = new GenericRow(schema.getFieldCount());
                for (int col = 0; col < 6; col++) {
                    row.setField(col, wkb);
                }
                row.setField(6, GenericRow.of(wkb, wkb));
                row.setField(7, new GenericArray(new Object[] {wkb}));
                row.setField(8, GenericMap.fromBinaryKeyMap(Collections.singletonMap(EMPTY_POINT, wkb)));
                row.setField(9, wkb);
                writer.addElement(row);
            }
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void checkWkb(DataGetters data, int column, byte[] expected) {
        require(data.isNullAt(column) == (expected == null), "Unexpected null at column " + column);
        if (expected != null) {
            require(Arrays.equals(expected, data.getBinary(column)), "WKB changed at column " + column);
        }
    }

    private static void check(FileFormat format, LocalFileIO io, Path path, RowType schema)
            throws Exception {
        long size = io.getFileSize(path);
        try (var parquet = ParquetUtil.getParquetReader(io, path, size, new Options())) {
            var footer = parquet.getFooter();
            RowType inferred = ParquetSchemaConverter.convertToPaimonRowType(
                    footer.getFileMetaData().getSchema());
            require(schema.equals(inferred), "Unexpected native Parquet schema: " + inferred);
            var columns = footer.getBlocks().get(0).getColumns();
            for (int i = 0; i < columns.size(); i++) {
                var stats = columns.get(i).getStatistics();
                require(stats.isNumNullsSet() && stats.getNumNulls() == (i == 9 ? 0 : 1),
                        "Null count for " + columns.get(i).getPath());
                require(stats.hasNonNullValue() == (i == 11), "Min/max for " + columns.get(i).getPath());
            }
        }
        FormatReaderContext context = new FormatReaderContext(io, path, size, null, null);
        try (CloseableIterator<InternalRow> rows = format
                .createReaderFactory(schema, schema, Collections.emptyList())
                .createReader(context).toCloseableIterator()) {
            for (int i = 0; i < 3; i++) {
                require(rows.hasNext(), "Missing row " + i);
                InternalRow row = rows.next();
                byte[] expected = value(i);
                for (int col = 0; col < 6; col++) {
                    checkWkb(row, col, expected);
                }
                InternalRow nested = row.getRow(6, 2);
                checkWkb(nested, 0, expected);
                checkWkb(nested, 1, expected);
                InternalArray array = row.getArray(7);
                require(array.size() == 1, "Array size");
                checkWkb(array, 0, expected);
                InternalMap map = row.getMap(8);
                require(map.size() == 1, "Map size");
                checkWkb(map.keyArray(), 0, EMPTY_POINT);
                checkWkb(map.valueArray(), 0, expected);
                checkWkb(row, 9, expected);
            }
            require(!rows.hasNext(), "Unexpected extra row");
        }
    }

    public static void main(String[] args) throws Exception {
        FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
        LocalFileIO io = LocalFileIO.create();
        Path path = new Path(Paths.get(args[1]).toUri());
        RowType schema = schema();
        if (args[0].equals("write")) {
            write(format, io, path, schema);
        } else {
            check(format, io, path, schema);
        }
    }
}
