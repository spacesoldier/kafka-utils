# kafka-utils

This is a tool which I wrote to automate bulk operations on Kafka consumer groups and topics management

At this moment it can help to reset selected topics for all consumer groups to the earliest, latest position or to the given date

Example call:

```
 python reset_offsets.py -t _all_ -o to_date -d 2019-01-1800:00:00.000 --backup-to /home/offsets/20190122
```


