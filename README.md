# Interview test Solution 

Solution to interview question at wallethub

## Question

 Can be found [here](https://www.dropbox.com/s/k5780mbc691zat4/Java_MySQL_Test.zip?dl=0&file_subpath=%2FJava_MySQL_Test_Instructions.txt)

 ### Solution 1

- Java program that can be run from command line

  Download java file [here](https://www.dropbox.com/sh/mi4tjnqyr8t39y0/AABx_nxt2NoRBTQH2-v5Gf0wa?dl=0)

  build tool - gradle


### Solution 2

- database name 

access_log

- log table

```
  CREATE TABLE `blocked_ip_3` (
  `id` int(64) NOT NULL AUTO_INCREMENT,
  `ip_address` varchar(20) DEFAULT NULL,
  `comment` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

```

- blocked IP Table
```
CREATE TABLE `log_line_3` (
  `id` int(64) NOT NULL AUTO_INCREMENT,
  `log_time` date DEFAULT NULL,
  `ip_address` varchar(20) DEFAULT NULL,
  `request` varchar(100) DEFAULT NULL,
  `status` int(64) DEFAULT NULL,
  `user_agent` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=116485 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

```


### Solution 3

- select * from log_line where ip_address = '192.168.203.111'
- select * from log_line where (log_time BETWEEN '2017-01-01.13:00:00' AND '2017-01-01.14:00:00')