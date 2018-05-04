Vena Command Line ETL Tool
===========

Help and Usage
--------------
See the [ETL Tool Guide](https://docs.vena.io/etl-guide/command-line-ETL/).

Development
-----------
By default the tool hits production servers, i.e. https://vena.io:443. To override this, supply `host`, `port`, and `ssl`/`nossl` options as necessary.
e.g.
```
java -jar etl.jar --host localhost --port 8080 --nossl
```

### Versioning

We use the Maven versioning scheme.
Release versions look like `1.2`, `1.3`, etc. Development and staging versions look like `1.2-SNAPSHOT`.
A release version should unambiguously refer to _only one_ Github commit and tag. You can check the version number by running the `--version` command, e.g.
```
$ java -jar etl.jar --version
artifactId: cmdline-etl-tool
version: 2.2
git.branch: refs/tags/cmdline-etl-tool-2.2
git.commit.id: 562db22a90c6c9b8cb7f8cb17f69af3fd96ca1ea
git.commit.id.describe: cmdline-etl-tool-2.2
git.commit.time: 16.09.2015 @ 14:25:09 UTC
git.build.time: 16.09.2015 @ 14:26:45 UTC

```

### Debugging

For SSL or network issues, try:
```
java -Djavax.net.debug=ssl -jar ...
```
or
```
java -Djavax.net.debug=all -jar ...
```
