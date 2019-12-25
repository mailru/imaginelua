require('console').listen(3302)

box.cfg({
    custom_proc_title = 'project',
})

dofile('project.lua')

box.schema.user.create('client', {password = 'client', if_not_exists = true})
box.schema.user.grant('client', 'execute', 'role', 'client_role', {if_not_exists = true})
