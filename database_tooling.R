### Install packages if not already installed
required_packages = c('RPostgres', 'tidyverse', 'dbplyr', 'igraph', 'ggplot2')

install_and_load = function(package_name){
  if(!require(package_name, character.only = TRUE)){
    install.packages(package_name)
    library(package_name, character.only = TRUE)
  }
}

lapply(required_packages, install_and_load)

### Notes:
## Basic design:
# t_something is a table, v_something is a view, mv_something is a materialised view,
#   something is the object intended for use
# We add buffered matviews, which go dependencies -> view -> matview -> view
#   This will be named e.g. source_schema.contacts -> pipeline_schema.v_contacts ->
#   pipeline_schema.mv_contacts -> pipeline_schema.contacts 
# This allows us to cache data and refresh it by refreshing the matview
# v_contacts holds the code, the other objects are effectively 
#   just select * (but with fixed column schema)
# If we need to make a code change to the view which does not change schema we can directly
#   CREATE OR REPLACE VIEW v_contacts
# If we need to make a code change which does change the schema then we 
#   CREATE TABLE t_contacts as select * from mv_contacts
#   CREATE OR REPLACE VIEW contacts as select * from t_contacts
#   DROP MATERIALIZED VIEW mv_contacts
#   CREATE VIEW v_contacts ...
#   CREATE MATERIALIZED VIEW mv_contacts as select * from v_contacts
#   CREATE OR REPLACE VIEW contacts as select * from mv_contacts

## Tips and tricks:
# dbplyr expressions can be defined as SQL expressions without using dplyr syntax using the
# SQL() function - this supports complex or performance-critical steps

### Helper functions
# translate(dbplyr_object) gets the SQL expression for a complex object expression
# delete_object(connection, schema, viewname, cascade) deletes the corresponding database object
# get_dependencies(connection, recursive = FALSE) checks the dependencies

translate = function(dbplyr_object){
  return(remote_query(dbplyr_object))
}

check_exists_view = function(connection, schema = defaultschema, viewname){
  present_views = dbGetQuery(connection, 'select viewname, schemaname from pg_views')
  return(viewname %in% present_views[present_views$schemaname == schema, ]$viewname)
}

check_exists_matview = function(connection, schema = defaultschema, matviewname){
  present_matviews = dbGetQuery(connection, 'select matviewname, schemaname from pg_matviews')
  return(matviewname %in% present_matviews[present_matviews$schemaname == schema, ]$matviewname)
}

check_exists_table = function(connection, schema = defaultschema, tablename){
  present_tables = dbGetQuery(connection, 'select tablename, schemaname from pg_tables')
  return(tablename %in% present_tables[present_tables$schemaname == schema, ]$tablename)
}

check_exists_object = function(connection, schema = defaultschema, objectname){
  if (check_exists_view(connection, schema, objectname)){
    return('view')
  } else if (check_exists_matview(connection, schema, objectname)){
    return('materialized view')
  } else if (check_exists_table(connection, schema, objectname)){
    return('table')
  } else {
    return(FALSE)
  }
}

delete_object = function(connection, schema = defaultschema, objectname, cascade = FALSE){
  objecttype = check_exists_object(connection, schema, objectname)
  if (objecttype == FALSE){
    stop('object does not exist')
  }
  statement = paste0('drop ', objecttype, ' ', schema, '.', objectname)
  if (cascade){
    statement = paste0(statement, ' cascade')
  }
  message(statement)
  dbExecute(connection, statement)
}

get_dependencies = function(connection, recursive = FALSE){
  if(recursive){
    recursive_query = "with recursive initial_query as (
    SELECT distinct dependent_ns.nspname as dependent_schema
, dependent_view.relname as dependent_view 
, source_ns.nspname as source_schema
, source_table.relname as source_table,
1 as depth
FROM pg_depend 
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid 
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid 
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid 
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
where not(dependent_view.oid = source_table.oid)
ORDER BY 1,2),
combination as (select * from initial_query
union all
select dependent.dependent_schema, dependent.dependent_view, source.source_schema, source.source_table,
dependent.depth + source.depth as depth
from
combination as dependent 
inner join
initial_query as source 
on dependent.source_schema = source.dependent_schema and dependent.source_table = source.dependent_view
)
select * from combination
    "
return(dbGetQuery(connection, recursive_query))
  } 
  # Query from https://stackoverflow.com/a/11773226/11732165
  dependencies_list = dbGetQuery(connection, 
                                 "SELECT distinct dependent_ns.nspname as dependent_schema
, dependent_view.relname as dependent_view 
, source_ns.nspname as source_schema
, source_table.relname as source_table
FROM pg_depend 
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid 
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid 
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid 
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
where not(dependent_view.oid = source_table.oid)
ORDER BY 1,2
")
  return(dependencies_list)
}

get_useful_dependencies = function(connection, recursive = FALSE){
  dependencies = get_dependencies(connection, recursive)
  return(dependencies[!dependencies$dependent_schema %in% c('information_schema', 'pg_catalog'),])
}

### User-facing functions
# check_buffered_mv(connection, schema, bmv_name) checks that a potential buffered matview is complete
#   and has appropriate dependencies, returns TRUE if present, FALSE if absent and errors if 
#   complicated (and prints details)
#   Also checks for reserved prefixes
# make_buffered_mv(dbplyr_object, connection, schema, bmv_name) creates the database objects 
#   corresponding to a dbplyr expression and returns a dbplyr expression
#   corresponding to the new object
#   so it can be used as a further dependency
# replace_buffered_mv(dbplyr_object, connection, schema, bmv_name) replaces objectname with dbplyr_object
# delete_buffered_mv(connection, schema, bmv_name, cascade) deletes a buffered mv, 
#   CASCADE = TRUE  
# dependency_graph(connection, schema) gets the dependencies for objects in a schema 
# refresh_queue(connection, schema) gets a linear list of refresh commands for objects in a schema 
#   due to limited time to prepare this demo the DAG option is not available
# full_refresh(connection, schema) gets the list of refresh commands and executes them in sequence

check_buffered_mv = function(connection, schema, bmv_name){
  # Check for reserved prefixes
  prefix = unlist(strsplit(bmv_name, '_'))[1]
  if (prefix %in% c('t', 'v', 'mv')){
    message('Reserved prefix:')
    message(prefix)
    stop('Invalid name for buffered matview')
  }
  initial_view_name = paste0('v_', bmv_name)
  matviewname = paste0('mv_', bmv_name)
  final_view_name = bmv_name
  exists_initial_view = check_exists_object(connection, schema, initial_view_name)
  exists_matview = check_exists_object(connection, schema, matviewname)
  exists_final_view = check_exists_object(connection, schema, final_view_name)
  if(exists_initial_view == FALSE &
     exists_matview == FALSE &
     exists_final_view == FALSE){
    return(FALSE)
  }
  if(exists_initial_view == 'view' & 
     exists_matview == 'materialized view' &
     exists_final_view == 'view'){
    # Checks dependencies
    # Bug: it only checks the dependencies, it doesn't check that 
    # the matview and final view are just select * from the previous object
    dependencies = get_dependencies(connection)
    mv_dependencies = dependencies[dependencies$dependent_view == matviewname &
                                     dependencies$dependent_schema == schema,]
    if(nrow(mv_dependencies) != 1){
      print(mv_dependencies)
      stop('dependencies incorrect')
    }
    if(mv_dependencies$source_schema != schema | mv_dependencies$source_table != initial_view_name){
      print(mv_dependencies)
      stop('dependencies_incorrect')
    }
    
    final_view_dependencies = dependencies[dependencies$dependent_view == final_view_name &
                                     dependencies$dependent_schema == schema,]
    if(nrow(final_view_dependencies) != 1){
      print(final_view_dependencies)
      stop('dependencies incorrect')
    }
    if(final_view_dependencies$source_schema != schema | final_view_dependencies$source_table != matviewname){
      print(final_view_dependencies)
      stop('dependencies_incorrect')
    }
    
    return(TRUE)
  } else {
    message(paste0(initial_view_name, ' is of type ', exists_initial_view))
    message(paste0(matviewname, ' is of type ', exists_matview))
    message(paste0(final_view_name, ' is of type ', exists_final_view))
    stop('Some objects exist but are not of correct type')
  }
}

make_buffered_mv = function(dbplyr_object, connection, schema, bmv_name){
  if(check_buffered_mv(connection, schema, bmv_name)){
    stop('A buffered mv with that name already exists')
  }
  source_sql = translate(dbplyr_object)
  
  initial_view_name = paste0('v_', bmv_name)
  matviewname = paste0('mv_', bmv_name)
  final_view_name = bmv_name
  
  initial_view_query = paste0('CREATE VIEW ', schema, '.', initial_view_name, ' as ', source_sql)
  matview_query = paste0('CREATE MATERIALIZED VIEW ', schema, '.', matviewname, ' as select * from ', schema, '.', initial_view_name)
  final_view_query = paste0('CREATE VIEW ', schema, '.', final_view_name, ' as select * from ', schema, '.', matviewname)
  
  message(initial_view_query)
  dbExecute(connection, initial_view_query)
  message(matview_query)
  dbExecute(connection, matview_query)
  message(final_view_query)
  dbExecute(connection, final_view_query)
  
  return(tbl(connection, in_schema(schema, final_view_name)))
}

delete_buffered_mv = function(connection, schema, bmv_name, cascade = FALSE){
  if(!check_buffered_mv(connection, schema, bmv_name)){
    stop('A buffered mv with that name does not exist')
  }
  
  initial_view_name = paste0('v_', bmv_name)
  matviewname = paste0('mv_', bmv_name)
  final_view_name = bmv_name
  
  delete_object(connection, schema, final_view_name, cascade)
  delete_object(connection, schema, matviewname, cascade = FALSE)
  delete_object(connection, schema, initial_view_name, cascade = FALSE)
}

replace_buffered_mv = function(dbplyr_object, connection, schema, bmv_name){
  if(!check_buffered_mv(connection, schema, bmv_name)){
    stop('A buffered mv with that name does not exist')
  }
  current_columns = colnames(tbl(connection, in_schema(schema, bmv_name)))
  new_columns = colnames(dbplyr_object)
  lost_columns = setdiff(current_columns, new_columns)
  if(length(lost_columns) > 0){
    message('Lost columns:')
    message(lost_columns)
    stop('Cannot replace as some columns are lost')
  }
  added_columns = setdiff(new_columns, current_columns)
  if(length(added_columns) == 0){
    source_sql = translate(dbplyr_object)
    initial_view_name = paste0('v_', bmv_name)
    
    initial_view_query = paste0('CREATE OR REPLACE VIEW ', schema, '.', initial_view_name, ' as ', source_sql)
    
    message(initial_view_query)
    dbExecute(connection, initial_view_query)
  } else {
    message('Added columns:')
    message(added_columns)
    
    initial_view_name = paste0('v_', bmv_name)
    matviewname = paste0('mv_', bmv_name)
    final_view_name = bmv_name
    temporary_table_name = paste0('t_', bmv_name)
    source_sql = translate(dbplyr_object)
    
    create_table_query = paste0('CREATE TABLE ', schema, '.', temporary_table_name, 
                                ' as select * from ', schema, '.', matviewname)
    replace_final_view_query = paste0('CREATE OR REPLACE VIEW ', schema, '.', final_view_name, 
                                      ' as select * from ', schema, '.', temporary_table_name)
    
    message(create_table_query)
    dbExecute(connection, create_table_query)
    message(replace_final_view_query)
    dbExecute(connection, replace_final_view_query)
    delete_object(connection, schema = schema, objectname = matviewname, cascade = FALSE)
    
    update_initial_view_query = paste0('CREATE OR REPLACE VIEW ', schema, '.', initial_view_name, ' as ', 
                                       source_sql)
    matview_query = paste0('CREATE MATERIALIZED VIEW ', schema, '.', matviewname, 
                           ' as select * from ', schema, '.', initial_view_name)
    final_view_query = paste0('CREATE OR REPLACE VIEW ', schema, '.', final_view_name, 
                              ' as select * from ', schema, '.', matviewname)
    
    message(update_initial_view_query)
    dbExecute(connection, update_initial_view_query)
    message(matview_query)
    dbExecute(connection, matview_query)
    message(final_view_query)
    dbExecute(connection, final_view_query)
    delete_object(connection, schema = schema, objectname = temporary_table_name, cascade = FALSE)
  } 
}

dependency_graph = function(connection, schema){
  dependencies = get_useful_dependencies(connection, recursive = FALSE)
  df_for_dependencies = dependencies %>%
    filter(dependent_schema == schema) %>%
    transmute(source_node = paste0(source_schema, '.', source_table),
              result_node = paste0(dependent_schema, '.', dependent_view)) %>%
    igraph::graph_from_data_frame()
  return(df_for_dependencies)
}

refresh_queue = function(connection, schema){
  dependencies = get_useful_dependencies(connection, recursive = TRUE)
  dependencies %>% group_by(dependent_schema, dependent_view) %>% summarise(max_depth = max(depth),
                                                                            .groups = 'drop') %>%
    filter(Vectorize(check_exists_matview, c('schema', 'matviewname'))(connection, dependent_schema, dependent_view)) %>%
    arrange(max_depth)
}
  
full_refresh = function(connection, schema){
  refresh_queue = refresh_queue(connection, schema) %>%
    mutate(refresh_query = paste0('REFRESH MATERIALIZED VIEW ', dependent_schema, '.', dependent_view))
  for (command in refresh_queue$refresh_query){
    message(command)
    dbExecute(connection, command)
  }
}

defaultschema = #'pipeline'

# Note all functions will default to this schema 