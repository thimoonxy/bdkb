# BDKB
*Big Data Knowledge Base*

Record basic `Hadoop` component practical demos, small tricks, common api usages.

![image](https://gradle.org/images/homepage/gradle-org-hero.png)


## Example `spark.Stream`

> Capture data from 5min ago until 2min ago

> Topic = simonT1

> Value of record = real timestamps

> Spark Streaming progress will be terminated in 60s automatically

```
Current Time: 2019-02-15 15:31:48

Capture time offsets:
partition = 0, time = 2019-02-15 15:27:01, offset = 115
partition = 3, time = 2019-02-15 15:26:51, offset = 123
partition = 1, time = 2019-02-15 15:26:56, offset = 111
partition = 2, time = 2019-02-15 15:27:06, offset = 97
Done

Capture time offsets:
partition = 0, time = 2019-02-15 15:29:51, offset = 119
partition = 3, time = 2019-02-15 15:29:56, offset = 133
partition = 1, time = 2019-02-15 15:30:01, offset = 121
partition = 2, time = 2019-02-15 15:30:26, offset = 109
Done

appname=SimonStream, appId=local-1550215930545
before terminate: 2019-02-15 15:32:11
key=simonT1#3#123,value=2019-02-15 15:26:51
key=simonT1#0#115,value=2019-02-15 15:27:01
key=simonT1#3#124,value=2019-02-15 15:27:16
key=simonT1#0#116,value=2019-02-15 15:28:21
key=simonT1#3#125,value=2019-02-15 15:27:26
key=simonT1#0#117,value=2019-02-15 15:28:31
key=simonT1#3#126,value=2019-02-15 15:27:41
key=simonT1#0#118,value=2019-02-15 15:28:51
key=simonT1#3#127,value=2019-02-15 15:28:16
key=simonT1#0#119,value=2019-02-15 15:29:51
key=simonT1#3#128,value=2019-02-15 15:28:41
key=simonT1#3#129,value=2019-02-15 15:29:01
key=simonT1#3#130,value=2019-02-15 15:29:06
key=simonT1#3#131,value=2019-02-15 15:29:16
key=simonT1#3#132,value=2019-02-15 15:29:46
key=simonT1#3#133,value=2019-02-15 15:29:56
key=simonT1#1#111,value=2019-02-15 15:26:56
key=simonT1#1#112,value=2019-02-15 15:27:11
key=simonT1#1#113,value=2019-02-15 15:27:46
key=simonT1#1#114,value=2019-02-15 15:27:51
key=simonT1#1#115,value=2019-02-15 15:28:01
key=simonT1#1#116,value=2019-02-15 15:28:06
key=simonT1#1#117,value=2019-02-15 15:29:11
key=simonT1#1#118,value=2019-02-15 15:29:21
key=simonT1#1#119,value=2019-02-15 15:29:26
key=simonT1#1#120,value=2019-02-15 15:29:41
key=simonT1#1#121,value=2019-02-15 15:30:01
key=simonT1#2#97,value=2019-02-15 15:27:06
key=simonT1#2#98,value=2019-02-15 15:27:21
key=simonT1#2#99,value=2019-02-15 15:27:31
key=simonT1#2#100,value=2019-02-15 15:27:36
key=simonT1#2#101,value=2019-02-15 15:27:56
key=simonT1#2#102,value=2019-02-15 15:28:11
key=simonT1#2#103,value=2019-02-15 15:28:26
key=simonT1#2#104,value=2019-02-15 15:28:36
key=simonT1#2#105,value=2019-02-15 15:28:46
key=simonT1#2#106,value=2019-02-15 15:28:56
key=simonT1#2#107,value=2019-02-15 15:29:31
key=simonT1#2#108,value=2019-02-15 15:29:36
key=simonT1#2#109,value=2019-02-15 15:30:26
after terminate: 2019-02-15 15:33:11

Process finished with exit code 0
```