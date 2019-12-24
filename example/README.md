# project

### Step 1: run tarantool
```sh
make
make run
```

### Step 2: connect
```sh
rlwrap -I telnet localhost 39032
```

### Step 3: play
```
$ rlwrap -I telnet localhost 39032
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
Tarantool 1.10.3 (Lua console)
type 'help' for interactive help

users.create('test@mail.ru')
---
...

users.list()
---
- - ['test@mail.ru', 1577206924.5969]
...

```
