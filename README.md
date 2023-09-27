# r-pgsql-pipeline-demo
I built a system for enabling pipeline management in an AWS PostgreSQL database from R in 2020-2022, and gave a demo presentation of this with tooling hastily built from scratch at Genomics England on 2023-09-08.

# What it does
The tooling allows analytical users to create lazy queries (in the tidyverse/dbplyr idiom) and materialise them onto the database as combinations of views and materialised views, so that multiple analytical users can work on the same pipeline. The tooling also allows capture of the dependencies to enable automated refreshes and in-place modifications of pipeline elements (tables) without breaking downstream elements.

# Files
Weird and Wonderful - Postgres Pipelines and R.pptx contains the presentation delivered, introducing the situation and requirements that led to the design.
database_tooling.R contains all the tooling functions and associated documentation

# To run the demo
For sample data you can for instance download the Simulacrum at https://simulacrum.healthdatainsight.org.uk/using-the-simulacrum/requesting-data/
Then set up a Free Tier Relational Database Service (RDS) PostgreSQL instance at Amazon Web Services with an accessible endpoint
Connect to the endpoint and run database_setup.sql (inputting admin credentials) to create the schemas and roles used in the demo, and populate_database.R to load the data. Experimentation showed that loading the data via R was over an order of magnitude faster and significantly more robust than loading via DBeaver. 
Then users can run demo.R using user-level credentials set up in the database setup script.