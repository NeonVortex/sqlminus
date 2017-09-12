sqlminus
--------
    A CLI tool to connect to Oracle database and run sql command without having to install Oracle instance

Usage

    dbUrl dbUser dbPass \[statement\] \[update\]
  * If [statement] not provided, enter interactive loop mode.
  * [update] is a flag to set if this statement is a query or an update; should be 1 (for update) or 0 (for query). Default is 0 if not provided

JVM Parameters
- LOGRESULTSONLY=1/0 Print results only or debug messages

