FROM tarantool/tarantool:1.10.3
COPY example/*.lua /opt/tarantool/
EXPOSE 3302
CMD ["tarantool", "/opt/tarantool/init-docker.lua"]
