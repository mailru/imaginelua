# project

### Step 1: run tarantool
```sh
make
make run
```

### Step 2: connect
```sh
telnet localhost 39032
```

### Step 3: play
```
$ telnet localhost 39032
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
Tarantool 1.10.3 (Lua console)
type 'help' for interactive help

key.create('abracadabra', 1,2,3,4)
---
...

key.get('abracadabra')
---
- - ['abracadabra', 1577270503, 1, 2, 3, 4]
...

```
