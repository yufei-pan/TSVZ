This lib provides some helper funtions to interact with tsv ( tab seperated values ) files.

TSVZ can also funtion like an in memory DB that is able to perform non-blocking read / write to TSV files.

Import as a lib or use console tool:

```bash
tsvz -h
```

```bash
TSVZ -h
```

```bash
usage: TSVZ [-h] [-c HEADER] [-f] [-v] [-V] filename [{read,append,delete,clear}] [line ...]

TSVZed: A TSV file manager

positional arguments:
  filename              The TSV file to read
  {read,append,delete,clear}
                        The operation to perform. Default: read
  line                  The line to append to the TSV file. it follows as : \{key\} \{value1\} \{value2\} ... if a key without value be
                        inserted, the value will get deleted.

options:
  -h, --help            show this help message and exit
  -c HEADER, --header HEADER
                        Perform checks with this header of the TSV file. seperate using \t
  -f, --force           Force the operation. Ignore checks for column numbers / headers
  -v, --verbose         Print verbose output
  -V, --version         show program's version number and exit
```