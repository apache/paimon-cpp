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

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.predicate.ConcatTransform;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.NullTransform;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.SubstringTransform;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitSerializer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Range;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes the Java {@code SplitSerializer} bytes of the {@code QueryAuthSplit} fixtures read by
 * {@code query_auth_split_test.cpp}. The checked-in fixtures were written by paimon-core
 * 2.2-SNAPSHOT built from apache/paimon commit c2e001b7f2c0214cc501d23a89be44ae031b447d:
 *
 * <pre>
 * javac -cp "$PAIMON_CLASSPATH" -d classes QueryAuthSplitFixtures.java
 * java -cp "classes:$PAIMON_CLASSPATH" QueryAuthSplitFixtures test/test_data/query_auth
 * </pre>
 */
public class QueryAuthSplitFixtures {

    public static void main(String[] args) throws Exception {
        Path dir = Paths.get(args[0]);
        RowType rowType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"id", "name", "phone", "salary"});
        PredicateBuilder builder = new PredicateBuilder(rowType);
        List<String> filter =
                Arrays.asList(
                        JsonSerdeUtil.toFlatJson(builder.greaterThan(0, 1)),
                        JsonSerdeUtil.toFlatJson(
                                builder.notEqual(1, BinaryString.fromString("测试"))));
        Map<String, String> masking = new HashMap<>();
        masking.put(
                "name",
                JsonSerdeUtil.toFlatJson(
                        new ConcatTransform(
                                Collections.singletonList(BinaryString.fromString("名字")))));
        masking.put(
                "phone",
                JsonSerdeUtil.toFlatJson(
                        new SubstringTransform(
                                Arrays.asList(
                                        new FieldRef(2, "phone", DataTypes.STRING()), 1, 3))));
        masking.put("salary", JsonSerdeUtil.toFlatJson(NullTransform.INSTANCE));
        TableQueryAuthResult rules = new TableQueryAuthResult(filter, masking);

        DataFileMeta file =
                DataFileMeta.create(
                        "file1.orc",
                        100L,
                        200L,
                        BinaryRow.EMPTY_ROW,
                        BinaryRow.EMPTY_ROW,
                        SimpleStats.EMPTY_STATS,
                        SimpleStats.EMPTY_STATS,
                        50L,
                        249L,
                        0L,
                        0,
                        Collections.emptyList(),
                        Timestamp.fromEpochMillis(1765535214349L),
                        0L,
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        50L,
                        null,
                        null);
        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("data/test_table/bucket-0")
                        .withDataFiles(Collections.singletonList(file))
                        .isStreaming(false)
                        .rawConvertible(true)
                        .build();
        IndexedSplit indexedSplit =
                new IndexedSplit(
                        dataSplit,
                        Collections.singletonList(new Range(55, 56)),
                        new float[] {0.5f});

        write(dir, "query_auth_split-data_split", new QueryAuthSplit(dataSplit, rules));
        write(dir, "query_auth_split-indexed_split", new QueryAuthSplit(indexedSplit, rules));
        write(
                dir,
                "query_auth_split-empty_rules",
                new QueryAuthSplit(
                        dataSplit,
                        new TableQueryAuthResult(
                                Collections.emptyList(), Collections.emptyMap())));
        write(
                dir,
                "query_auth_split-null_rules",
                new QueryAuthSplit(dataSplit, new TableQueryAuthResult(null, null)));
        write(dir, "query_auth_split-null_auth_result", new QueryAuthSplit(dataSplit, null));

        for (String json : filter) {
            System.out.println("filter: " + json);
        }
        for (Map.Entry<String, String> mask : masking.entrySet()) {
            System.out.println("mask " + mask.getKey() + ": " + mask.getValue());
        }
    }

    private static void write(Path dir, String name, Split split) throws Exception {
        byte[] bytes = SplitSerializer.serialize(split);
        if (!Arrays.equals(bytes, SplitSerializer.serialize(SplitSerializer.deserialize(bytes)))) {
            throw new IllegalStateException(name + " does not round-trip in Java");
        }
        Files.write(dir.resolve(name), bytes);
    }
}
