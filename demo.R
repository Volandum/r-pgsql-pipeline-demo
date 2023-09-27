## Connect to the database

source('database_tooling.R') #loads RPostgres, tidyverse, dbplyr, igraph and ggplot2 packages, plus all helper functions

endpoint = 
username = 
password =  # user password

# This user doesn't have access to anything but the assumed role does

con <- DBI::dbConnect(RPostgres::Postgres(), dbname = 'postgres',
                      host = endpoint, port = 5432, 
                      user = username, password = password)

dbSendStatement(con, 'set role analyst')

# You can run arbitrary queries but don't have access to the tables until you assume the role
# This role has READ access to schema source_tables
# and READ/WRITE access to schemas pipeline and scratch

dbGetQuery(con, 'select * from pg_tables')
dbGetQuery(con, 'select * from source_tables.sim_av_tumour limit 10')

dbGetQuery(con, 'select current_role')
dbGetQuery(con, 'select * from source_tables.sim_av_tumour limit 10')

# The tooling works with dbplyr objects, which look like this

db_pg_tables = tbl(con, 'pg_tables')
db_pg_tables
# They can also be defined from SQL

db_pg_tables_data_dictionary = tbl(con, sql("select * from pg_tables where tableowner = 'rdsadmin'"))
db_pg_tables_data_dictionary
# We can retrieve the SQL from these

remote_query(db_pg_tables_data_dictionary)

# Let's now connect to our data tables

db_colorectal_patients = tbl(con, in_schema('source_tables', 'colorectal_patients'))
show_query(db_colorectal_patients)
db_colorectal_patients
# Let's try making one of these 'buffered matviews'

colorectal_patients = make_buffered_mv(db_colorectal_patients, con, 
                                       schema = 'scratch', bmv_name = 'colorectal_patients')
check_buffered_mv(con, schema = 'scratch', bmv_name = 'colorectal_patients')
show_query(colorectal_patients)

# We can see the implementation in the dependencies

get_useful_dependencies(con)
get_useful_dependencies(con) %>% filter(dependent_schema == 'scratch')
get_useful_dependencies(con, recursive = TRUE) %>% filter(dependent_schema == 'scratch')

# We can tear things down

delete_buffered_mv(con, schema = 'scratch', bmv_name = 'colorectal_patients')


# Attempts to replace a buffered matview using the scratch schema

colorectal_patients = make_buffered_mv(db_colorectal_patients, con, 
                                       schema = 'scratch', bmv_name = 'colorectal_patients')

colorectal_patients_without_ethnicity = db_colorectal_patients %>% select(-ETHNICITY)
replace_buffered_mv(colorectal_patients_without_ethnicity, con, schema = 'scratch', bmv_name = 'colorectal_patients')
# Errors as expected - we can't safely remove columns

colorectal_patients_filtered = db_colorectal_patients %>% filter(ETHNICITY == 'A')
replace_buffered_mv(colorectal_patients_filtered, con, schema = 'scratch', bmv_name = 'colorectal_patients')

colorectal_patients_new_column = db_colorectal_patients %>% mutate(sex_text = ifelse(
  SEX == 1, 'MALE',
  if_else(SEX == 2, 'FEMALE', 'UNKNOWN')
))
replace_buffered_mv(colorectal_patients_new_column, con, schema = 'scratch', bmv_name = 'colorectal_patients')
delete_buffered_mv(con, schema = 'scratch', bmv_name = 'colorectal_patients')

# Some quick pipeline objects

db_colorectal_patients = tbl(con, in_schema('source_tables', 'colorectal_patients'))
colorectal_patients = make_buffered_mv(db_colorectal_patients, con, 
                                       schema = 'pipeline', bmv_name = 'colorectal_patients')
db_colorectal_tumours = tbl(con, in_schema('source_tables', 'colorectal_tumours'))
colorectal_tumours = make_buffered_mv(db_colorectal_tumours, con,
                                      schema = 'pipeline', bmv_name = 'colorectal_tumours')

other_tumour_count = colorectal_patients %>% inner_join(colorectal_tumours, by = 'PATIENTID') %>%
  group_by(PATIENTID) %>% 
  summarise(other_tumours = sum(ifelse(SITE_ICD10_O2_3CHAR %in% c('C18', 'C19', 'C20'), 0, 1)))

other_tumour_count = make_buffered_mv(other_tumour_count, con, 
                                      schema = 'pipeline', bmv_name = 'other_tumours_by_colorectal_pat')
other_tumour_count = tbl(con, in_schema('pipeline', 'other_tumours_by_colorectal_pat'))

other_tumour_count %>% count(other_tumours) %>% collect() %>%
  mutate(n = as.numeric(n)) %>%
  ggplot(aes(x = other_tumours, y = n)) + 
  xlab('Number of non-colorectal tumours') + ylab('Number of patients with a colorectal tumour') + 
  scale_y_log10() +
  geom_col()

dependency_graph(con, 'pipeline') %>% plot(layout = layout_as_tree)

refresh_queue(con, 'pipeline')

full_refresh(con, 'pipeline')
