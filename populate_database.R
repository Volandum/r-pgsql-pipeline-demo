source('database_tooling.R')

endpoint = 
username =  # admin username
password =  # admin password

# This user doesn't have access to anything but the assumed role does

admin_con <- DBI::dbConnect(RPostgres::Postgres(), dbname = 'postgres',
                      host = endpoint, port = 5432, 
                      user = username, password = password)

simulacrum_release_location = 

av_tumour_csv = read.csv(paste0(simulacrum_release_location, '\\data/sim_av_tumour.csv'))
av_patient_csv = read.csv(paste0(simulacrum_release_location, '\\data/sim_av_patient.csv'))
colorectal_cohort = av_tumour_csv[av_tumour_csv$SITE_ICD10_O2_3CHAR %in% c('C18', 'C19', 'C20'),]$PATIENTID
colorectal_tumours = av_tumour_csv[av_tumour_csv$PATIENTID %in% colorectal_cohort,]
colorectal_patients = av_patient_csv[av_patient_csv$PATIENTID %in% colorectal_cohort,]
dbWriteTable(admin_con, name = Id(schema = 'source_tables', table = 'colorectal_tumours'),
             colorectal_tumours)
dbWriteTable(admin_con, name = Id(schema = 'source_tables', table = 'colorectal_patients'),
             colorectal_patients)

dbWriteTable(admin_con, name = Id(schema = 'source_tables', table = 'sim_av_tumour'),
             av_tumour_csv)
dbWriteTable(admin_con, name = Id(schema = 'source_tables', table = 'sim_av_patient'),
             av_patient_csv)
