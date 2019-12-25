#!/bin/bash
set -e -o pipefail


name=$1
if [[ ! "$name" =~ ^[a-z]+$ ]]; then
    echo 'USAGE: ./make.sh PROJECT_NAME' >&2
    exit 1
fi


mkdir -p dist

for f in lua/imagine.lua grafana/dashboard.json; do
    sed "s/__PROJECT__/$name/" $f > dist/$(basename $f)
done

for f in lua/expirationd.lua lua/graphite.lua; do
    cp $f dist/$(basename $f)
done

cat <<EOF > dist/$name.lua
local imagine = require('imagine')
local log = require('log')

local function TODO(...)

end

local function init()
    -- box.schema.create_space('TODO', {if_not_exists = true})
    -- box.space.TODO:create_index('TODO', {type = 'hash', parts = {1, 'str'}, if_not_exists = true})

    -- require('expirationd').run_task(
    --     'TODO',
    --     'TODO',
    --     function (args, t) return TODO end,
    --     function (space, args, t) TODO end
    -- )

    log.info('init ok')
end

imagine.init({
    init_func = init,

    roles = {
        client_role = {
            table = '$name',
            funcs = {
                todo = imagine.atomic(TODO),
            },
        },
    },

    graphite = {
        prefix = '$name',
        ip     = 'TODO',
        port   = TODO,
    },
})
EOF

echo 'Done. Saved to dist/'
ls -al dist/
