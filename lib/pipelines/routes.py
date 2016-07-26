# PIPELINE SERVER ROUTES
JOBS_GET_EXP = r'/jobs'
JOBS_CREATE_EXP = r'/jobs/create'
JOBS_EDIT_EXP = r'/jobs/edit'
JOBS_CANCEL_EXP = r'/jobs/cancel'
JOBS_RESTART_EXP = r'/jobs/restart'
CONFIG_VIEW_EXP = r'/config'
CONFIG_EDIT_EXP = r'/config/edit'
DATADISKS_GET_EXP = r'/datadisks/list'
DATADISKS_CREATE_EXP = r'/datadisks/create'
DATADISKS_DELETE_EXP = r'/datadisks/delete'
LOGS_GET_EXP = r'/logs'

# SQLITE SERVER ROUTES
SQLITE_READ_EXP = r'/read/(["jobs"|"job_dependencies"|"job_archive"|"data_disks"])'
SQLITE_INSERT_EXP = r'/insert/(["jobs"|"job_dependencies"|"job_archive"|"data_disks"])'
SQLITE_UPDATE_EXP = r'/update/(["jobs"|"job_dependencies"|"job_archive"|"data_disks"])'
SQLITE_INCR_VALUE_EXP = r'/increment/(["jobs"|"job_dependencies"|"job_archive"|"data_disks"])'

# CONFIG SERVER ROUTES
CONFIG_UPDATE_EXP = r'/update'
