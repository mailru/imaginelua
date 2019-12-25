local imagine = require('imagine')
local log = require('log')


local KEY_LIFETIME = 60

local function create(key, ...)
    box.space.key:insert({key, os.time(), ...})
end

local function delete(key)
    box.space.key:delete({key})
end

local function get(key)
    return box.space.key:select({key})
end


local function init()
    box.schema.create_space('key', {if_not_exists = true})
    box.space.key:create_index('pk', {type = 'hash', parts = {1, 'str'}, if_not_exists = true})

    require('expirationd').run_task(
        'project_expiration_task',
        'key',
        function (args, t) return t[2] + KEY_LIFETIME < os.time() end,
        function (space, args, t) delete(t[1]) end
    )

    log.info('init ok')
end

imagine.init({
    init_func = init,

    roles = {
        client_role = {
            table = 'key',
            funcs = {
                create = imagine.atomic(create),
                delete = imagine.atomic(delete),
                get    = imagine.atomic(get),
            },
        },
    },

    graphite = {
        prefix = 'project',
        ip     = '127.0.0.1',
        port   = 2003,
    },
})
