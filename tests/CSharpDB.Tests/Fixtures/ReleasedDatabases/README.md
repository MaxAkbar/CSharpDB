# Released database fixtures

These immutable databases were created by running the CSharpDB CLI from the
listed released Git tag and commit, then issuing the same SQL workload and
`.checkpoint` before exit.

| Fixture | Tag | Commit | File format | SHA-256 |
|---|---|---|---:|---|
| `csharpdb-v4.0.0.db` | `v4.0.0` | `c87f30261f77fa9372dfc809726f686f85605984` | 1 | `f96465d2541c744f533859dddd6ce6003475b71ca6a08466fc6efaded3ae5a35` |
| `csharpdb-v4.0.1.db` | `v4.0.1` | `05174bbf7c94160336087aeed385fc6291652ff0` | 1 | `4e8ba1b47a0cdc129f84a476ad261432948f9f86932bbe889477ecec6dada3d0` |
| `csharpdb-v4.0.2.db` | `v4.0.2` | `ebe5c114735c6b29c500b9752592da1dc4ab760a` | 1 | `fb2bea18690a58ed6e027bc7fc013d810cf644c2d633330bf005b384ce527315` |
| `csharpdb-v4.0.3.db` | `v4.0.3` | `0960c866b05a1609e72480091040a5d70e709051` | 2 | `0cfe60af58d778b72f83429513bf43dbfe3e164511066a18f067dece8533d953` |
| `csharpdb-v4.0.4.db` | `v4.0.4` | `522f98de48353f7d7822532ad1c5e3617c4551f3` | 2 | `dab0c94039d049edcb76a7c2615e40c67710da7c27e3e50e858143618da7c0ee` |
| `csharpdb-v4.1.0.db` | `v4.1.0` | `c7b7af8365f93b24a2161cb2b794ea12ae42aaa9` | 2 | `4cb3d49d410e38b65ecbf8d45e5e5e6ff361e7ee44a983119a15e39ee6421530` |
| `csharpdb-v4.2.0.db` | `v4.2.0` | `55f00ae780f37992b633471d659bb6c62b315bd7` | 2 | `989b70ddd00a0e11a399e156f29309069b3c6b197e72752b9b8942289add8d7c` |
| `csharpdb-v4.3.0.db` | `v4.3.0` | `7880dad112f3fdf011c134db2f8a08ec646ee326` | 2 | `fc642aecd3cc0d909bf5a71ef828b2de9f2de47d6321bdffd3767b408747bb20` |

Each fixture contains:

- `fixture_origin`, identifying the producing release;
- `accounts`, with INTEGER, TEXT, REAL, BLOB, NULL, and secondary-index data;
- `events`, with multiple rows and a secondary index.

The generation workload was:

```sql
CREATE TABLE fixture_origin (id INTEGER PRIMARY KEY, release_tag TEXT);
INSERT INTO fixture_origin VALUES (1, '<release-tag>');
CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, score REAL, payload BLOB);
CREATE INDEX ix_accounts_name ON accounts (name);
INSERT INTO accounts VALUES (1, 'Alpha', 1.5, X'0102');
INSERT INTO accounts VALUES (2, NULL, -2.25, X'');
CREATE TABLE events (id INTEGER PRIMARY KEY, account_id INTEGER, note TEXT);
CREATE INDEX ix_events_account_id ON events (account_id);
INSERT INTO events VALUES (10, 1, 'created');
INSERT INTO events VALUES (11, 2, 'reviewed');
```

Do not regenerate these files with the current engine. A fixture change must be
produced from its named release source and accompanied by reviewed provenance
and checksum updates.
