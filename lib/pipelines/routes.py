# PIPELINE SERVER ROUTES
JOBS_LIST_CREATE_EXP = r'/jobs$'
JOB_DESCRIBE_EXP = r'/jobs/(\d+)$'
JOB_EDIT_EXP = r'/jobs/(\d+)/edit$'
JOB_CANCEL_EXP = r'/jobs/(\d+)/cancel$'
JOB_RESTART_EXP = r'/jobs/(\d+)/restart$'
CONFIG_VIEW_INIT_EXP = r'/config$'
CONFIG_EDIT_EXP = r'/config/edit$'
DATADISKS_LIST_CREATE_EXP = r'/datadisks$'
DATADISK_DESCRIBE_DELETE_EXP = r'/datadisks/([a-z]*([0-9]*|[a-z]*|[A-Z-]*))*$'  # TODO: check that expression is correct
LOG_STDOUT_EXP = r'/log/(\d+)/stdout$'
LOG_STDERR_EXP = r'/log/(\d+)/stderr$'

# SQLITE SERVER ROUTES
SQLITE_READ = r'/read/(["jobs"|"job_dependencies"|"job_archive"|"data_disks"])'
SQLITE_INSERT = r'/insert/(["jobs"|"job_dependencies"|"job_archive"|"data_disks"])'
SQLITE_UPDATE = r'/update/(["jobs"|"job_dependencies"|"job_archive"|"data_disks"])'
SQLITE_INCR_VALUE = r'/increment/(["jobs"|"job_dependencies"|"job_archive"|"data_disks"])'

# CONFIG SERVER ROUTES
CONFIG_INIT = r'/init'
CONFIG_UPDATE = r'/update'
