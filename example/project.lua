local imagine = require('imagine')
local fiber = require('fiber')
local log = require('log')


local function users_create(login)
    box.space.users:insert({login, fiber.time()})
end

local function users_list()
    return box.space.users:select({})
end


local function init()
    box.schema.create_space('users', {if_not_exists = true})
    box.space.users:create_index('pk', {type = 'hash', parts = {1, 'str'}, if_not_exists = true})

    log.info('init ok')
end

imagine.init({
    init_func = init,

    roles = {
        backend_role = {
            table = 'users',
            funcs = {
                create = imagine.atomic(users_create),
                list   = imagine.atomic(users_list),
            },
        },
    },
})
