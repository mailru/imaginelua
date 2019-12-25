-- imagine helper functions for tarantool 1.6+

require('strict').on()

local log      = require('log')
local pickle   = require('pickle')
local io       = require('io')
local fio      = require('fio')
local clock    = require('clock')
local digest   = require('digest')
local graphite = require('graphite')
local ffi      = require('ffi')
local fiber    = require('fiber')
local yaml     = require('yaml')

ffi.cdef[[
    int gethostname(char *name, size_t len);
]]

-- module state ----------------------------------------------------------------

local config

-- module state ^---------------------------------------------------------------

local function extend_deep(table_orig, table_by)
    for k, v in pairs(table_by) do
        if type(v) == 'table' then
            if table_orig[k] == nil then
                -- copy keys rather than refer the table
                table_orig[k] = extend_deep({}, v)
            elseif type(table_orig[k]) == 'table' then
                extend_deep(table_orig[k], v)
            else
                log.error("imagine: key types mismatch")
            end
        else
            table_orig[k] = v
        end
    end
    return table_orig
end

-- guess one level upper file from which this function was called
local function guess_caller(skip_levels)
    local i = skip_levels or 2

    while true do
        local info = debug.getinfo(i, 'S')
        if not info then
            log.error("imagine: failed to determine module name")
            return
        end
        if not info.source:match('.*imagine%.lua$') then
            return info.source:match('^@.-([^/]+%.lua)$')
        end
        i = i + 1
    end
end

local function read_config(file)
    if not fio.stat(file) then
        log.info("imagine: configuration file not found, skipping (file:'%s')", file)
        return {}
    end

    local f, msg, errno = io.open(file, 'r')
    if not f then
        error(string.format("imagine: io.open failed (file:'%s', " ..
                            "msg:'%s', errno:%d", file, msg, errno))
    end
    local content = f:read('*all')
    f:close()
    if not content then
        error(string.format("imagine: file reading failed (file:'%s')", file))
    end
    return yaml.decode(content)
end

local function print_config(config)
    local c = extend_deep({}, config)
    c.secure = c.secure and '<removed>' or nil
    log.info("imagine: using following configuration\n%s", yaml.encode(c))
end

local function get_hostname()
    local buf = ffi.new('char[256]')
    ffi.C.gethostname(buf, 256)
    return ffi.string(buf)
end

local function normalize_metric_name(name)
    return name:lower():gsub('[^%w_]', '_')
end

--[[
-- packs sequential arguments to a table and
-- sets number of elements (including nil holes)

-- wrong (all data is lost after the nil hole):
ret = {func(...)}
return unpack(ret)

-- correct:
ret = pack(func(...))
return unpack(ret.r, 1, ret.n)

]]--
local function pack(...)
    return {r = {...}, n = select('#', ...)}
end

--[[
-- helps to create atomic function with one line
-- it works like decorator
-- very useful if you have multiple returns in your function

-- usage example:
your_function = imagine.atomic(yourfunction)

]]--
local function atomic_tail(status, ...)
    if not status then
        box.rollback()
        error((...), 2)
    end

    box.commit()
    return ...
end

local function atomic(func)
    return function(...)
        box.begin()
        return atomic_tail(pcall(func, ...))
    end
end

--[[
-- init_storage works like decorator
-- it helps to init storage for setuid function calls from client

-- for example you have following init function which creates your spaces and indexes
function init_modulename()
    box.schema.create_space('modulename',
            {if_not_exists = true})
    box.space.modulename:create_index('pk',
            {type = 'hash', parts = {1, 'STR'}, if_not_exists = true})
end

-- and you have theese two functions in your module as interface for client
local function modulename.set(...)
    return box.space.modulename:replace(...)
end

local function modulename.get(...)
    return box.space.modulename:select(...)
end

-- you need to add following line to your code

-- the parameters are:
--    init_func: init function of your module which creates your spaces and indexes
--    interface: interface functions of your module
--               it will call box.schema.func.create for them
imagine.init_storage(init_modulename, {'modulename.set', 'modulename.get'})

]]--
local function init_storage(init_func, interface)
    local init_username = "imagine"
    box.schema.user.create(init_username, {if_not_exists = true})
    box.schema.user.grant(init_username, 'execute,read,write', 'universe', nil,
            {if_not_exists = true})
    box.session.su(init_username)

    init_func()

    for _, v in pairs(interface) do
        box.schema.func.create(v, {setuid = true, if_not_exists = true})
    end

    box.session.su('admin')
    box.schema.user.revoke(init_username, 'execute,read,write', 'universe')
end

--[[
-- helps to create role with 'execute' access to interface functions
-- use box.schema.user.grant('user_name', 'execute', 'role', 'role_name') to grant

-- usage example:
imagine.init_role('modulename_client', {'modulename.set', 'modulename.get'})

]]--
local function init_role(role_name, interface)
    box.schema.role.create(role_name, {if_not_exists = true})
    for _, v in pairs(interface) do
        box.schema.role.grant(role_name, 'execute', 'function', v,
                {if_not_exists = true})
    end
end

local function stat_tail(name, ...)
    return ...
end

local function stat(name, func)
    local stat_name_func_call = 'imagine.func_' .. name .. '_call'
    return function(...)
        graphite.sum_per_min(stat_name_func_call, 1)
        return stat_tail(name, func(name, ...))
    end
end

local function wrap_su_imagine(func)
    return function (...)
        local init_username, ret

        init_username = "imagine"
        box.schema.user.create(init_username, {if_not_exists = true})
        box.schema.user.grant(init_username, 'execute,read,write', 'universe', nil,
                {if_not_exists = true})
        box.session.su(init_username)

        ret = pack(func(...))

        box.session.su('admin')
        box.schema.user.revoke(init_username, 'execute,read,write', 'universe')
        return unpack(ret.r, 1, ret.n)
    end
end

local function split(str, delim)
    local parts = {}

    for part in string.gmatch(str, '([^' .. delim .. ']+)') do
        parts[#parts + 1] = part
    end
    return parts
end

local function wrap_stat_init(name, options)
    local metrics, default_metrics, prefix, names, funcs, m, f, all_funcs

    default_metrics = 'avg,min,max,rpm'

    if type(name) ~= 'string' then
        log.error("imagine: 'name' is not a string")
        return
    end

    options = options or {}
    if type(options) ~= 'table' then
        log.error("imagine: 'options' is not a table")
        return
    end

    metrics = default_metrics
    if options.metrics then
        if type(options.metrics) ~= 'string' then
            log.error("imagine: 'options.metrics' is not a string")
            return
        end
        metrics = options.metrics
    end

    prefix = ''
    if options.prefix then
        if type(options.prefix) ~= 'string' then
            log.error("imagine: 'options.prefix' is not a string")
            return
        end
        prefix = options.prefix
    end

    names = {
        rpm = prefix .. name .. '_rpm',
        avg = prefix .. name .. '_avg',
        min = prefix .. name .. '_min',
        max = prefix .. name .. '_max',
    }
    all_funcs = {
        rpm = function (name) graphite.sum_per_min(name, 1) end,
        avg = graphite.avg_per_min,
        min = graphite.min_per_min,
        max = graphite.max_per_min,
    }

    funcs = {}
    for _, m in ipairs(split(metrics, ',')) do
        if all_funcs[m] then
            funcs[m] = all_funcs[m]
        else
            log.error("imagine: unsupported metric (name:'%s', metric:'%s')",
                      name, m)
        end
    end

    return names, funcs
end

--[[
-- returns function wrapped with statistics
--
-- parameters:
--   name       string identifier of counters
--   func       function to wrap
--   options    options
--
-- options:
--   prefix     prefix for counters (default: '')
--   metrics    list of metrics (default: 'min,max,avg,rpm')
--              available metrics: min, max, avg, rpm
]]--
local function wrap_stat(name, func, options)
    local names, funcs, stat_name

    stat_name = string.gsub(name, '[%.:]', '_')
    names, funcs = wrap_stat_init(stat_name, options)
    if not names then
        return func
    end
    return function (...)
        local start, delta, ret, m, f

        start = clock.time64()
        ret = pack(func(...))
        delta = tonumber(clock.time64() - start) / 1000
        for m, f in pairs(funcs) do
            f(names[m], delta)
        end
        return unpack(ret.r, 1, ret.n)
    end
end

local wrap_stat_expirationd = (function ()
    local wrapped_funcs = {}

    return function ()
        local expirationd, task, v

        expirationd = require('expirationd')
        for _, v in pairs(expirationd.tasks()) do
            task = expirationd.task(v)
            if not wrapped_funcs[task.is_tuple_expired] then
                task.is_tuple_expired = wrap_stat(v .. '_exp',
                        task.is_tuple_expired, {prefix = 'imagine.expd.'})
                wrapped_funcs[task.is_tuple_expired] = true
            end
            if not wrapped_funcs[task.process_expired_tuple] then
                task.process_expired_tuple = wrap_stat(v .. '_proc',
                        task.process_expired_tuple, {prefix = 'imagine.expd.'})
                wrapped_funcs[task.process_expired_tuple] = true
            end
        end
    end
end)()

local function file_calc_hash(file)
    local f, err, errno, hash

    f, err, errno = io.open(file, 'rb')
    if not f then
        log.error("imagine: io.open failed (file:'%s', err:'%s', errno:%d)",
                  file, err, errno)
        return
    end
    hash = digest.crc32(f:read('*all'))
    f:close()
    return hash
end

local file_add = (function()
    local hashes = {}

    return function(file)
        local key = file:match('([^/]+)%.lua$') or file:match('([^/]+)$')
        if not key then
            return
        end

        key = normalize_metric_name(key)
        local hash = file_calc_hash(file)
        hashes[key] = hash and math.fmod(hash, 1024) or -1024
        graphite.callback('imagine.files.' .. key, function ()
            return hashes[key]
        end)
    end
end)()

local function init(options, do_re_require)

    if do_re_require ~= false then
        local function re_require(name)
            local module_prev = require(name)
            for k, _ in pairs(module_prev) do
                module_prev[k] = nil
            end
            package.loaded[name] = nil
            local module_new = require(name)
            for k, v in pairs(module_new) do
                module_prev[k] = v
            end
            return module_new
        end

        if package.loaded.expirationd ~= nil then
            local expirationd = require('expirationd')
            for _, name in pairs(expirationd.tasks()) do
                expirationd.task(name):kill()
            end

            re_require('expirationd')
        end

        graphite = re_require('graphite')

        return re_require('imagine').init(options, false)
    end

    local function init_role(name, options, all_options)
        local table, prefix, interface, func_name

        if type(name) ~= 'string' then
            log.error("imagine: 'options.roles' has non-string key")
            return
        end
        if type(options) ~= 'table' then
            log.error("imagine: 'options.roles.%s' is not a table", name)
            return
        end

        if type(options.funcs) ~= 'table' then
            log.error("imagine: 'options.roles.%s.funcs' is not specified " ..
                      "or not a table", name)
            return
        end
        for k, v in pairs(options.funcs) do
            if type(k) ~= 'string' then
                log.error("imagine: 'options.roles.%s.funcs' has " ..
                          "non-string key", name)
                return
            end
            if type(v) ~= 'function' then
                log.error("imagine: 'options.roles.%s.funcs.%s' is not a " ..
                          "function", name, k)
                return
            end
        end

        if not options.table then
            table = _G
            prefix = ''
        elseif type(options.table) == 'string' then
            table = rawget(_G, options.table)
            if not table then
                table = {}
                rawset(_G, options.table, table)
            end
            prefix = options.table .. '.'
        else
            log.error("imagine: 'options.roles.%s.table' is not a string",
                      name)
            return
        end

        interface = {}
        for k, func in pairs(options.funcs) do
            func_name = prefix .. k
            local func_table = table
            local func_name_obj = k:gsub(':', '.', 1)
            local func_name_split = split(func_name_obj, '.')
            if #func_name_split > 1 then
                k = func_name_split[#func_name_split]
                for i=1,#func_name_split - 1 do
                    local new_table = rawget(func_table, func_name_split[i])
                    if not new_table then
                        new_table = {}
                        rawset(func_table, func_name_split[i], new_table)
                    end
                    func_table = new_table
                end
            end

            if not config.stat.no_auto_wrap_funcs then
                func = wrap_stat(func_name, func, {prefix = 'imagine.funcs.'})
            end

            rawset(func_table, k, func)
            interface[#interface + 1] = func_name
        end

        if not box.cfg.read_only then
            box.schema.role.create(name, {if_not_exists = true})
            for _, v in pairs(interface) do
                box.schema.func.create(v, {setuid = true, if_not_exists = true})
                box.schema.role.grant(name, 'execute', 'function', v,
                                        {if_not_exists = true})
            end
        end
    end

    if type(options) ~= 'table' then
        log.error("imagine: 'options' is not specified or not a table")
        return
    end
    if type(options.init_func) ~= 'function' then
        log.error("imagine: 'options.init_func' is not specified or " ..
                  "not a function")
        return
    end
    if type(options.roles) ~= 'table' then
        log.error("imagine: 'options.roles' is not specified or not a table")
        return
    end

    options.name = options.name or guess_caller():match('(.+)%.lua$')

    do
        local config_graphite = config.graphite
        if options.graphite then
            if type(options.graphite) ~= 'table' then
                log.error("imagine: 'options.graphite' is not a table")
                return
            end
            config_graphite = extend_deep({}, config_graphite)
            config_graphite = extend_deep(config_graphite, options.graphite)
        end

        local prefix = string.format('%s.tnt.%s.%s.%s.',
                                     config_graphite.prefix,
                                     options.name,
                                     get_hostname():gsub('%..*', '.'),
                                     box.cfg.custom_proc_title)
        if config_graphite.raw_prefix then
            prefix = config_graphite.raw_prefix
        end
        graphite.init(prefix, config_graphite.ip, config_graphite.port)
    end

    -- XXX: options.init_func will be called only on master
    if not box.cfg.read_only then
        wrap_su_imagine(options.init_func)()
    end

    -- add size of spaces to statistics
    for _, v in box.space._space:pairs() do
        if v[1] >= 512 then
            local name = v[3]
            graphite.callback('imagine.space.' .. name, function ()
                return box.space[name]:len()
            end)
        end
    end

    -- add fibers to statistics
    graphite.callback('imagine.fiber', function ()
        local fibers = {}
        for _, v in pairs(fiber.info()) do
            fibers[#fibers + 1] = { normalize_metric_name(v.name), v.fid }
        end
        return fibers
    end)

    -- add files hash to statistics
    for fn in io.popen('ls -d -1 *.lua *.yaml 2>/dev/null'):lines() do
        file_add(fn)
    end

    -- create roles with interfaces
    if not box.cfg.read_only then
        for k, v in pairs(options.roles) do
            init_role(k, v, options)
        end
    end

    if package.loaded.expirationd ~= nil then
        wrap_stat_expirationd()
    end
end

return (function ()
    -- put all module initializations here,
    -- instead of spreading them over the file
    config = {}
    config.stat = {}
    config.graphite = {
        prefix = '__PROJECT__',
        ip     = '127.0.0.1',
        port   = 2003,
    }
    extend_deep(config, rawget(_G, 'imagine_default_config') or {})
    extend_deep(config, read_config('imagine.conf.yaml'))
    print_config(config)

    return {
        init         = init,
        init_storage = init_storage,
        init_role    = init_role,
        get_hostname = get_hostname,

        config = config,

        atomic = atomic,
    }
end)()
