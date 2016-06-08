# inventory
this is inventory log parser, parsing SURF, ROUTE, and IME logs to get and write  statistics data in csv file format.

To run this program, you need create one directory called "log" in the same level with this program.
Supposed this program's name is "ictutility", the directory structure should be:

directory/
  ictutility.py
  log/
      --a.log
      --b.log
      --c.log
      .....

Then type command in terminal:
python ictutility.py

A file called "Invent Monitoring.csv" will be created with statistics data.
