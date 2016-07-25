# PIPELINE SERVER ROUTES
JOBS_GET_EXP = r'/jobs'
JOBS_CREATE_EXP = r'/jobs/create'
JOB_EDIT_EXP = r'/jobs/edit'
JOB_CANCEL_EXP = r'/jobs/cancel'
JOB_RESTART_EXP = r'/jobs/restart'
CONFIG_VIEW_EXP = r'/config'
CONFIG_EDIT_EXP = r'/config/edit'
DATADISKS_LIST_EXP = r'/datadisks/list'
DATADISKS_DESCRIBE_EXP = r'/datadisks/describe'
DATADISKS_CREATE_EXP = r'/datadisks/create'
DATADISKS_DELETE_EXP = r'/datadisks/delete'
LOG_GET_EXP = r'/logs'

# SQLITE SERVER ROUTES
SQLITE_READ_EXP = r'/read/(["jobs"|"job_dependencies"|"job_archive"|"data_disks"])'
SQLITE_INSERT_EXP = r'/insert/(["jobs"|"job_dependencies"|"job_archive"|"data_disks"])'
SQLITE_UPDATE_EXP = r'/update/(["jobs"|"job_dependencies"|"job_archive"|"data_disks"])'
SQLITE_INCR_VALUE_EXP = r'/increment/(["jobs"|"job_dependencies"|"job_archive"|"data_disks"])'

# CONFIG SERVER ROUTES
CONFIG_UPDATE_EXP = r'/update'
