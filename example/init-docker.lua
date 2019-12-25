require('console').listen(3302)

box.cfg({
    custom_proc_title = 'project',
})

dofile('graphite.lua')
dofile('imagine.lua')
dofile('project.lua')
