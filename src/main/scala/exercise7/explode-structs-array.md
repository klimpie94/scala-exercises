# Exploding structs array

Write a structured query that "explodes" an array of structs (of open and close hours).

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
$ cat input.json
{
  "business_id": "abc",
  "full_address": "random_address",
  "hours": {
    "Monday": {
      "close": "02:00",
      "open": "11:00"
    },
    "Tuesday": {
      "close": "02:00",
      "open": "11:00"
    },
    "Friday": {
      "close": "02:00",
      "open": "11:00"
    },
    "Wednesday": {
      "close": "02:00",
      "open": "11:00"
    },
    "Thursday": {
      "close": "02:00",
      "open": "11:00"
    },
    "Sunday": {
      "close": "00:00",
      "open": "11:00"
    },
    "Saturday": {
      "close": "02:00",
      "open": "11:00"
    }
  }
}
```

```text
scala> input.show(truncate = false)
+-----------+--------------+----------------------------------------------------------------------------------------------------------------+
|business_id|full_address  |hours                                                                                                           |
+-----------+--------------+----------------------------------------------------------------------------------------------------------------+
|abc        |random_address|[[02:00, 11:00], [02:00, 11:00], [02:00, 11:00], [00:00, 11:00], [02:00, 11:00], [02:00, 11:00], [02:00, 11:00]]|
+-----------+--------------+----------------------------------------------------------------------------------------------------------------+
```

## Result

```text
scala> solution.show(truncate = false)
+-----------+--------------+---------+---------+----------+
|business_id|full_address  |day      |open_time|close_time|
+-----------+--------------+---------+---------+----------+
|abc        |random_address|Friday   |11:00    |02:00     |
|abc        |random_address|Monday   |11:00    |02:00     |
|abc        |random_address|Saturday |11:00    |02:00     |
|abc        |random_address|Sunday   |11:00    |00:00     |
|abc        |random_address|Thursday |11:00    |02:00     |
|abc        |random_address|Tuesday  |11:00    |02:00     |
|abc        |random_address|Wednesday|11:00    |02:00     |
+-----------+--------------+---------+---------+----------+
```