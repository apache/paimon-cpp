# Lance compatibility fixture

`java_lance_0_39_0.lance` was written with Apache Paimon Java commit
`63ffd403648e1cf9848e32bb29a398c1b3b16ab1` and `com.lancedb:lance-core:0.39.0`.

Its schema is `id INT, name STRING, score DOUBLE`, and its rows are:

```text
1, alpha, 1.25
2, beta, -2.5
3, null, null
4, delta, 4.75
```

The fixture verifies that the C++ reader remains compatible with files written by the Java
implementation. The reciprocal C++-write/Java-read check is run during compatibility validation.

SHA-256 checksum:

```text
b8aae32740ad048c926380d976f6beed5f44326d4553d692b4d81d8ed17d8334  java_lance_0_39_0.lance
```
