## Feed tangle

(Lipmaa backlinks are not shown in the diagram below, but they should exist)

```mermaid
graph RL

R["(Feed root)"]:::feedroot
A[updates age]
B[updates name]
C[updates age]
D[updates name]
E[updates age & name]

C-->B-->A-->R
D--->A
E-->D & C
classDef default fill:#999,stroke:#fff0,color:#000
```

Reducing the tangle above in a topological sort allows you to build a record
(a JSON object) `{age, name}`.

## Msg type

`msg.metadata.type` MUST start with `record_v1__`. E.g. `record_v1__profile`.

## Msg content

`msg.content` format:

```typescript
interface MsgContent {
  update: Record<string, any>,
  supersedes: Array<MsgHash>,
}
```

RECOMMENDED that the `msg.content.update` is as flat as possible (no nesting).

## Supersedes links

When you update a field in a record, in the `supersedes` array you MUST point
to the currently-known highest-depth msg that updated that field.

The set of *not-transitively-superseded-by-anyone* msgs comprise the
"field roots" of the record. To allow pruning the tangle, we can delete
(or, if we want to keep metadata, "erase") all msgs preceding the field roots.

Suppose the tangle is grown in the order below, then the field roots are
highlighted in blue.

```mermaid
graph RL

R["(Feed root)"]:::feedroot
A[updates age]:::blue
A-->R
classDef default fill:#999,stroke:#fff0,color:#000
classDef blue fill:#6af,stroke:#fff0,color:#000
```

----

```mermaid
graph RL

R["(Feed root)"]:::feedroot
A[updates age]:::blue
B[updates name]:::blue
B-->A-->R
classDef default fill:#999,stroke:#fff0,color:#000
classDef blue fill:#6af,stroke:#fff0,color:#000
```

-----


```mermaid
graph RL

R["(Feed root)"]:::feedroot
A[updates age]
B[updates name]:::blue
C[updates age]:::blue

C-->B-->A-->R
C-- supersedes -->A

linkStyle 3 stroke-width:1px,stroke:#05f
classDef default fill:#999,stroke:#fff0,color:#000
classDef blue fill:#6af,stroke:#fff0,color:#000
```

-----


```mermaid
graph RL

R["(Feed root)"]:::feedroot
A[updates age]
B[updates name]:::blue
C[updates age]:::blue
D[updates name]:::blue

C-->B-->A-->R
D--->A
C-- supersedes -->A

linkStyle 4 stroke-width:1px,stroke:#05f
classDef default fill:#999,stroke:#fff0,color:#000
classDef blue fill:#6af,stroke:#fff0,color:#000
```
-----


```mermaid
graph RL

R["(Feed root)"]:::feedroot
A[updates age]
B[updates name]
C[updates age]
D[updates name]
E[updates age & name]:::blue

C-->B-->A-->R
C-- supersedes -->A
D-->A
E-->D & C
E-- supersedes -->C
E-- supersedes -->D

linkStyle 3,7,8 stroke-width:1px,stroke:#05f
classDef default fill:#999,stroke:#fff0,color:#000
classDef blue fill:#6af,stroke:#fff0,color:#000
```
