# kafka-utils

This is a tool which I wrote to automate bulk operations on Kafka consumer groups and topics management. 
It is a set of calls of the Kafka system tools shell scripts wrapped into Python code. It can be used as a part of monitoring
subsystem for containerized instances.

Available features:
* describe given consumer group or all consumer groups
* reset selected topics for all consumer groups to the earliest, latest position or to the given date


### Example calls:

to describe all groups run:

```
 python show_offsets.py -g _all_
```


to reset offsets run:

```
 python reset_offsets.py -t _all_ --offset to_date --date "2019-01-18T00:00:00.000" --backup-to /home/offsets/20190122
```


