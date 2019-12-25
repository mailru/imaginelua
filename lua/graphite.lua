local fiber = require('fiber')
local socket = require('socket')
local log = require('log')

local _M = { }
local metrics = { }
local initialized = false
local common_stat_fiber = nil
local stat_fiber = nil

local sock = nil
local host = ''
local port = 0
local prefix = ''

local METRIC_SEC_TIMER = 0
local METRIC_SUM_PER_MIN = 1
local METRIC_SUM_PER_SEC = 2
local METRIC_VALUE = 3
local METRIC_AVG_PER_MIN = 4
local METRIC_MIN_PER_MIN = 5
local METRIC_MAX_PER_MIN = 6
local METRIC_CALLBACK = 100

local function send_graph(name, res, ts)
	if initialized == true then
		local graph = prefix .. name .. ' ' .. tostring(res) .. ' ' .. tostring(ts) .. '\n'
		sock:sendto(host, port, graph)
	end
end

local function send_metrics(ts, dt)
	for id, metric in pairs(metrics) do
		local mtype = metric[1]
		local name = metric[2]
		if mtype == METRIC_SEC_TIMER then
			local cnt = metric[3]
			local prev_cnt = metric[4]
			local values = metric[5]
			local aggr_fn = metric[7]

			if cnt > prev_cnt + 60 then
				prev_cnt = cnt - 60
			end

			if cnt ~= prev_cnt then
				local res = aggr_fn(prev_cnt, cnt - 1, values, dt)
				metric[4] = cnt
				send_graph(name, res, ts)
			end
		elseif mtype == METRIC_SUM_PER_MIN then
			local res = metric[3]
			send_graph(name, res, ts)
			metric[3] = 0
		elseif mtype == METRIC_SUM_PER_SEC then
			local res = metric[3] / dt
			send_graph(name, res, ts)
			metric[3] = 0
		elseif mtype == METRIC_VALUE then
			local res = metric[3]
			send_graph(name, res, ts)
		elseif mtype == METRIC_AVG_PER_MIN then
			local res = metric[3]
			if res ~= nil then
				metric[3] = nil
				if metric[5] > 1 then
					res = res + metric[4] / metric[5]
				end
				send_graph(name, res, ts)
			end
		elseif mtype == METRIC_MIN_PER_MIN then
			local res = metric[3]
			if res ~= nil then
				metric[3] = nil
				send_graph(name, res, ts)
			end
		elseif mtype == METRIC_MAX_PER_MIN then
			local res = metric[3]
			if res ~= nil then
				metric[3] = nil
				send_graph(name, res, ts)
			end
        elseif mtype == METRIC_CALLBACK then
            local res = metric[3]()
            if res ~= nil then
                if type(res) == 'table' then
                    for _, m in ipairs(res) do
                        send_graph(name .. '.' .. m[1], m[2], ts)
                    end
                else
                    send_graph(name, res, ts)
                end
            end
		end
	end
end

local function send_net_stats(ostats_net, stats_net, ts, dt)
	local res = 0

	res = (stats_net.SENT.total - ostats_net.SENT.total) / dt
	send_graph('net.sent_rps_avg', res, ts)
	send_graph('net.sent_total', stats_net.SENT.total, ts)

	if stats_net.EVENTS then
		res = (stats_net.EVENTS.total - ostats_net.EVENTS.total) / dt
		send_graph('net.events_rps_avg', res, ts)
		send_graph('net.events_total', stats_net.EVENTS.total, ts)
	end

	if stats_net.LOCKS then
		res = (stats_net.LOCKS.total - ostats_net.LOCKS.total) / dt
		send_graph('net.locks_rps_avg', res, ts)
		send_graph('net.locks_total', stats_net.LOCKS.total, ts)
	end

	res = (stats_net.RECEIVED.total - ostats_net.RECEIVED.total) / dt
	send_graph('net.received_rps_avg', res, ts)
	send_graph('net.received_total', stats_net.RECEIVED.total, ts)
end

local function send_mem_stats(stats_mem, ts, dt)
	-- https://www.tarantool.io/ru/doc/1.10/book/box/box_info/#box-info-memory
	-- Any stat written in bytes
	for k, v in pairs({
		['mem.cache_sz']            = stats_mem.cache, -- vinyl cache size
		['mem.data_sz']             = stats_mem.data,  -- arena
		['mem.transactions_buf_sz'] = stats_mem.tx,    -- vinyl transactions buffers size
		['mem.lua_interp_sz']       = stats_mem.lua,   -- lua interpreter mem (XXX: interpreter have a limit: 2GB)
		['mem.net_io_buf_sz']       = stats_mem.net,   -- net IO buffers size
		['mem.index_sz']            = stats_mem.index, -- tarantool indexes size
	}) do
		send_graph(k, v, ts)
	end
end

local function send_box_stats(ostats_box, stats_box, ts, dt)
	local res = 0

	res = (stats_box.SELECT.total - ostats_box.SELECT.total) / dt
	send_graph('select_rps_avg', res, ts)

	res = (stats_box.REPLACE.total - ostats_box.REPLACE.total) / dt
	send_graph('replace_rps_avg', res, ts)

	res = (stats_box.UPDATE.total - ostats_box.UPDATE.total) / dt
	send_graph('update_rps_avg', res, ts)

	res = (stats_box.DELETE.total - ostats_box.DELETE.total) / dt
	send_graph('delete_rps_avg', res, ts)

	res = (stats_box.INSERT.total - ostats_box.INSERT.total) / dt
	send_graph('insert_rps_avg', res, ts)

	res = (stats_box.UPSERT.total - ostats_box.UPSERT.total) / dt
	send_graph('upsert_rps_avg', res, ts)

	res = (stats_box.CALL.total - ostats_box.CALL.total) / dt
	send_graph('call_rps_avg', res, ts)

	res = (stats_box.AUTH.total - ostats_box.AUTH.total) / dt
	send_graph('auth_rps_avg', res, ts)

	res = (stats_box.ERROR.total - ostats_box.ERROR.total) / dt
	send_graph('error_rps_avg', res, ts)
end

local function send_slab_stats(ts, dt)
	local slab_info = box.slab.info()
	for name, stat_ in pairs(slab_info) do
		local stat = string.gsub(stat_, '%%', '')
		send_graph(name, stat, ts)
	end

	if slab_info['quota_used'] and slab_info['quota_size'] then
		local quota_used = tonumber(slab_info['quota_used']) or 0
		local quota_size = tonumber(slab_info['quota_size']) or 0
		if quota_size > 0 then
			local quota_used_ratio = quota_used * 100 / quota_size
			send_graph("quota_used_ratio", quota_used_ratio, ts)
		end
	end

	for _, name in ipairs({
		'slab_alloc_arena',
		'slab_alloc_factor',
		'slab_alloc_minimal',
		'slab_alloc_maximal',
	}) do
		if box.cfg[name] ~= nil then
			send_graph(name, box.cfg[name], ts)
		end
	end

	if _M.options.disable_slab_stats then
		return
	end

	local item_count = 0

	local slab_stats = box.slab.stats()
	for i, slab in pairs(slab_stats) do
		local item_size = slab['item_size']
		local slab_prefix = 'slab_' .. tostring(item_size) .. '.'
		for name, stat in pairs(slab) do
			if name ~= 'item_size' then
				if name == 'item_count' then
					item_count = item_count + tonumber(stat)
				end
				send_graph(slab_prefix .. name, stat, ts)
			end
		end
	end

	send_graph('item_count', item_count, ts)
end

local function send_expirationd_stats(ts, dt)
	if not pcall(require, "expirationd") then
		return
	end

	local tasks = require("expirationd").stats()
	for task_name, task in pairs(tasks) do
		local task_prefix = 'expirationd.' .. task_name .. '.'
		for name, value in pairs(task) do
			if type(value) == "number" then
				local stat = string.gsub(name, "[.:]", "_")
				send_graph(task_prefix .. stat, value, ts)
			end
		end
	end
end

local function send_replication_stats(box_info, ts)
	local box_id = box_info.server.id or 0
	local box_lsn = box_info.server.lsn or 0
	local vclock = box_info['vclock']

	local sum = 0
	for id, clock in ipairs(vclock) do
		send_graph('vclock.' .. tostring(id), clock, ts)
		sum = sum + clock
	end

	send_graph("id", box_id, ts)
	send_graph("lsn", box_lsn, ts)
	send_graph('vclock_sum', sum, ts)

	send_graph('replication_vclock_sum', sum - box_lsn, ts)
	if box_info.replication.status == "follow" then
		send_graph("replication_idle", box_info.replication.idle, ts)
		send_graph("replication_lag", box_info.replication.lag, ts)
	end
end

local function init_stats()
	_M.add_sec_metric('select_rps_max', function() return box.stat().SELECT.rps end, _M.max)
	_M.add_sec_metric('replace_rps_max', function() return box.stat().REPLACE.rps end, _M.max)
	_M.add_sec_metric('update_rps_max', function() return box.stat().UPDATE.rps end, _M.max)
	_M.add_sec_metric('insert_rps_max', function() return box.stat().INSERT.rps end, _M.max)
	_M.add_sec_metric('upsert_rps_max', function() return box.stat().UPSERT.rps end, _M.max)
	_M.add_sec_metric('call_rps_max', function() return box.stat().CALL.rps end, _M.max)
	_M.add_sec_metric('delete_rps_max', function() return box.stat().DELETE.rps end, _M.max)
	_M.add_sec_metric('auth_rps_max', function() return box.stat().AUTH.rps end, _M.max)
	_M.add_sec_metric('error_rps_max', function() return box.stat().ERROR.rps end, _M.max)
end

local function send_stats(ostats, stats, ts, dt)
	local res = 0

	ts = math.floor(ts)
	dt = math.floor(dt)

	if dt ~= 0 then
		local box_info = box.info

		-- send global stats
		send_graph("uptime", box_info.uptime or 0, ts)

		-- send net stats
		send_net_stats(ostats.net_stat, stats.net_stat, ts, dt)

		-- send box stats
		send_box_stats(ostats.base_stat, stats.base_stat, ts, dt)

		-- send slab stats
		send_slab_stats(ts, dt)

		-- send mem stats; available only on tarantools >= 1.7
		if stats.mem_stat ~= nil then
			send_mem_stats(stats.mem_stat, ts, dt)
		end

		-- send expirationd stats
		send_expirationd_stats(ts, dt)

		-- send replication stats
		send_replication_stats(box_info, ts)

		-- send custom metrics
		send_metrics(ts, dt)
	end
end

local function collect_stats()
	for id, metric in pairs(metrics) do
		local mtype = metric[1]
		if mtype == METRIC_SEC_TIMER then
			local cnt = metric[3]
			local values = metric[5]
			local metric_fn = metric[6]

			values[cnt % 60 + 1] = metric_fn()
			metric[3] = cnt + 1
		end
	end
end

_M.stop = function()
    for k, v in pairs(fiber.info()) do
        if string.find(v.name, 'graphite_common_stat') then
            log.info("killing fiber '%s' (%d)", v.name, v.fid)
            fiber.kill(v.fid)
        end
    end
    common_stat_fiber = nil

    for k, v in pairs(fiber.info()) do
        if string.find(v.name, 'graphite_stat') then
            log.info("killing fiber '%s' (%d)", v.name, v.fid)
            fiber.kill(v.fid)
        end
    end
    stat_fiber = nil

	if sock ~= nil then
		sock:close()
		sock = nil
	end

	metrics = {}
	initialized = false
end

_M.metrics = function()
	return metrics
end

local function accumulate_stat()
	return {
		base_stat = box.stat(),
		net_stat  = box.stat.net(),
		mem_stat  = box.info.memory and box.info.memory(), -- mem info available only on tarantools >= 1.7
	}
end

_M.init = function(prefix_, host_, port_, options_)
	prefix = prefix_ or 'localhost.tarantool.'
	host = host_ or 'nerv1.i'
	port = port_ or 2003

	_M.options = {
		disable_slab_stats = true,
	}
	for k, v in pairs(options_ or {}) do
		_M.options[k] = v
	end

	_M.stop()

	init_stats()
	initialized = true

	common_stat_fiber = fiber.create(function()
		fiber.name("graphite_common_stat")

		sock = socket('AF_INET', 'SOCK_DGRAM', 'udp')

		if sock ~= nil then
			local t = fiber.time()
			while true do
				local ostat = accumulate_stat()
				local nt = fiber.time()

				local st = 60 - (nt - t)
				fiber.sleep(st)

				local stat = accumulate_stat()

				t = fiber.time()
				send_stats(ostat, stat, t, t - nt)
			end
		end
	end)

	if common_stat_fiber ~= nil then
		stat_fiber = fiber.create(function()
			fiber.name("graphite_stat")

			while true do
				collect_stats()
				fiber.sleep(1)
			end
		end
		)
	end

	log.info("Successfully initialized graphite module: %s:%s", host, port)
end

_M.sum = function(first, last, values, dt)
	local res = 0
	local i = first
	while i <= last do
		res = res + values[i % 60 + 1]
		i = i + 1
	end
	return res
end

_M.sum_per_sec = function(first, last, values, dt)
	local res = 0
	if dt ~= 0 then
		local i = first
		while i <= last do
			res = res + values[i % 60 + 1]
			i = i + 1
		end
		res = res / dt
	end
	return res
end

_M.max = function(first, last, values, dt)
	local res = nil
	local i = first
	while i <= last do
		local v = values[i % 60 + 1]
		if res == nil or v > res then
			res = v
		end
		i = i + 1
	end
	return res
end

_M.min = function(first, last, values, dt)
	local res = nil
	local i = first
	while i <= last do
		local v = values[i % 60 + 1]
		if res == nil or v < res then
			res = v
		end
		i = i + 1
	end
	return res
end

_M.last = function(first, last, values, dt)
	return values[last % 60 + 1]
end

_M.add_sec_metric = function(name, metric_fn, aggr_fn)
	local mtype = METRIC_SEC_TIMER
	local id = name .. '_' .. tostring(mtype)
	metrics[id] = { mtype, name, 0, 0, {}, metric_fn, aggr_fn }
end

_M.sum_per_min = function(name, value)
	local mtype = METRIC_SUM_PER_MIN
	local id = name .. '_' .. tostring(mtype)
	if metrics[id] == nil then
		metrics[id] = { mtype, name, value }
	else
		metrics[id][3] = metrics[id][3] + value
	end
end

_M.sum_per_sec = function(name, value)
	local mtype = METRIC_SUM_PER_SEC
	local id = name .. '_' .. tostring(mtype)
	if metrics[id] == nil then
		metrics[id] = { mtype, name, value }
	else
		metrics[id][3] = metrics[id][3] + value
	end
end

_M.add = function(name, value)
	local mtype = METRIC_VALUE
	local id = name .. '_' .. tostring(mtype)
	if metrics[id] == nil then
		metrics[id] = { mtype, name, value }
	else
		metrics[id][3] = metrics[id][3] + value
	end
end

_M.set = function(name, value)
	local mtype = METRIC_VALUE
	local id = name .. '_' .. tostring(mtype)
	metrics[id] = { mtype, name, value }
end

_M.avg_per_min = function(name, value)
	local mtype = METRIC_AVG_PER_MIN
	local id = name .. '_' .. tostring(mtype)
	if metrics[id] == nil or metrics[id][3] == nil then
		metrics[id] = { mtype, name, value, 0, 1 }
	else
		metrics[id][4] = metrics[id][4] + (value - metrics[id][3])
		metrics[id][5] = metrics[id][5] + 1
	end
end

_M.min_per_min = function(name, value)
	local mtype = METRIC_MIN_PER_MIN
	local id = name .. '_' .. tostring(mtype)
	if metrics[id] == nil or metrics[id][3] == nil then
		metrics[id] = { mtype, name, value }
	else
		if value < metrics[id][3] then
			metrics[id][3] = value
		end
	end
end

_M.max_per_min = function(name, value)
	local mtype = METRIC_MAX_PER_MIN
	local id = name .. '_' .. tostring(mtype)
	if metrics[id] == nil or metrics[id][3] == nil then
		metrics[id] = { mtype, name, value }
	else
		if value > metrics[id][3] then
			metrics[id][3] = value
		end
	end
end

_M.send = function(name, res, ts)
	send_graph(name, res, math.floor(ts))
end

_M.inc = function(name)
	_M.add(name, 1)
end

_M.callback = function(name, callback)
    if type(callback) ~= 'function' then
        log.error("callback is not a function")
        return
    end
	local mtype = METRIC_CALLBACK
	local id = name .. '_' .. tostring(mtype)
	metrics[id] = { mtype, name, callback }
end

_M.status = function()
	local status = {}

	status['initialized'] = initialized

	if initialized == true then
		status['fibers'] = {}

		if common_stat_fiber ~= nil then
			table.insert(status['fibers'], {
				name = common_stat_fiber:name(),
				status = common_stat_fiber:status()
			})
		end

		if stat_fiber ~= nil then
			table.insert(status['fibers'], {
				name = stat_fiber:name(),
				status = stat_fiber:status()
			})
		end

		status['host'] = host
		status['port'] = port
		status['prefix'] = prefix
	end

	return status
end

return _M
