from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'execution_timeout': timedelta(hours=2)
}

# Define DAG
dag = DAG(
    'northern_ireland_data_pipeline',
    default_args=default_args,
    description='Load data from S3 to Redshift for Northern Ireland crime and dwelling data',
    schedule_interval='@daily',
    catchup=False,
    tags=['northern_ireland', 'crime_data', 'dwelling_data']
)

# Start operator
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Create staging table for crime data
create_staging_crime_table = RedshiftDataOperator(
    task_id='create_staging_crime_table',
    sql="""
    DROP TABLE IF EXISTS stg_police_recorded_crime;
    CREATE TABLE IF NOT EXISTS stg_police_recorded_crime (
        calendar_year VARCHAR(4),
        month VARCHAR(3),
        policing_district VARCHAR(100),
        crime_type VARCHAR(150),
        data_measure VARCHAR(150),
        count VARCHAR(20)
    )
    DISTSTYLE EVEN
    SORTKEY (calendar_year, month);
    
    DROP TABLE IF EXISTS error_records;
    CREATE TABLE IF NOT EXISTS error_records (
        calendar_year VARCHAR(4),
        month VARCHAR(3),
        policing_district VARCHAR(100),
        crime_type VARCHAR(150),
        data_measure VARCHAR(150),
        count VARCHAR(20),
        error_message VARCHAR(1000),
        error_timestamp TIMESTAMP DEFAULT GETDATE()
    )
    DISTSTYLE EVEN
    SORTKEY (error_timestamp);
    """,
    aws_conn_id='aws_s3_conn',
    database='dev',
    cluster_identifier='dwh-redshift-dw',
    db_user='awsuser',
    dag=dag
)

# Create final crime table
create_crime_table = RedshiftDataOperator(
    task_id='create_crime_table',
    sql="""
    CREATE TABLE IF NOT EXISTS police_recorded_crime (
        calendar_year INTEGER,
        month VARCHAR(3),
        policing_district VARCHAR(100),
        crime_type VARCHAR(150),
        data_measure VARCHAR(150),
        count DECIMAL(10,2),
        loaded_at TIMESTAMP DEFAULT GETDATE()
    )
    DISTSTYLE EVEN
    SORTKEY (calendar_year, month);
    """,
    aws_conn_id='aws_s3_conn',
    database='dev',
    cluster_identifier='dwh-redshift-dw',
    db_user='awsuser',
    dag=dag
)

# Create staging table for dwelling data
create_staging_dwelling_table = RedshiftDataOperator(
    task_id='create_staging_dwelling_table',
    sql="""
    DROP TABLE IF EXISTS stg_new_dwelling_completions;
    CREATE TABLE IF NOT EXISTS stg_new_dwelling_completions (
        lgd VARCHAR(50),
        q1_2005 VARCHAR(20),
        q2_2005 VARCHAR(20),
        q3_2005 VARCHAR(20),
        q4_2005 VARCHAR(20),
        q1_2006 VARCHAR(20),
        q2_2006 VARCHAR(20),
        q3_2006 VARCHAR(20),
        q4_2006 VARCHAR(20),
        q1_2007 VARCHAR(20),
        q2_2007 VARCHAR(20),
        q3_2007 VARCHAR(20),
        q4_2007 VARCHAR(20),
        q1_2008 VARCHAR(20),
        q2_2008 VARCHAR(20),
        q3_2008 VARCHAR(20),
        q4_2008 VARCHAR(20),
        q1_2009 VARCHAR(20),
        q2_2009 VARCHAR(20),
        q3_2009 VARCHAR(20),
        q4_2009 VARCHAR(20),
        q1_2010 VARCHAR(20),
        q2_2010 VARCHAR(20),
        q3_2010 VARCHAR(20),
        q4_2010 VARCHAR(20),
        q1_2011 VARCHAR(20),
        q2_2011 VARCHAR(20),
        q3_2011 VARCHAR(20),
        q4_2011 VARCHAR(20),
        q1_2012 VARCHAR(20),
        q2_2012 VARCHAR(20),
        q3_2012 VARCHAR(20),
        q4_2012 VARCHAR(20),
        q1_2013 VARCHAR(20),
        q2_2013 VARCHAR(20),
        q3_2013 VARCHAR(20),
        q4_2013 VARCHAR(20),
        q1_2014 VARCHAR(20),
        q2_2014 VARCHAR(20),
        q3_2014 VARCHAR(20),
        q4_2014 VARCHAR(20),
        q1_2015 VARCHAR(20)
    )
    DISTSTYLE EVEN
    SORTKEY (lgd);
    
    DROP TABLE IF EXISTS dwelling_error_records;
    CREATE TABLE IF NOT EXISTS dwelling_error_records (
        lgd VARCHAR(50),
        error_message VARCHAR(1000),
        error_timestamp TIMESTAMP DEFAULT GETDATE()
    )
    DISTSTYLE EVEN
    SORTKEY (error_timestamp);
    """,
    aws_conn_id='aws_s3_conn',
    database='dev',
    cluster_identifier='dwh-redshift-dw',
    db_user='awsuser',
    dag=dag
)

# Create final dwelling table
create_dwelling_table = RedshiftDataOperator(
    task_id='create_dwelling_table',
    sql="""
    DROP TABLE IF EXISTS new_dwelling_completions;
    CREATE TABLE IF NOT EXISTS new_dwelling_completions (
        lgd VARCHAR(50),
        q1_2005 INTEGER,
        q2_2005 INTEGER,
        q3_2005 INTEGER,
        q4_2005 INTEGER,
        q1_2006 INTEGER,
        q2_2006 INTEGER,
        q3_2006 INTEGER,
        q4_2006 INTEGER,
        q1_2007 INTEGER,
        q2_2007 INTEGER,
        q3_2007 INTEGER,
        q4_2007 INTEGER,
        q1_2008 INTEGER,
        q2_2008 INTEGER,
        q3_2008 INTEGER,
        q4_2008 INTEGER,
        q1_2009 INTEGER,
        q2_2009 INTEGER,
        q3_2009 INTEGER,
        q4_2009 INTEGER,
        q1_2010 INTEGER,
        q2_2010 INTEGER,
        q3_2010 INTEGER,
        q4_2010 INTEGER,
        q1_2011 INTEGER,
        q2_2011 INTEGER,
        q3_2011 INTEGER,
        q4_2011 INTEGER,
        q1_2012 INTEGER,
        q2_2012 INTEGER,
        q3_2012 INTEGER,
        q4_2012 INTEGER,
        q1_2013 INTEGER,
        q2_2013 INTEGER,
        q3_2013 INTEGER,
        q4_2013 INTEGER,
        q1_2014 INTEGER,
        q2_2014 INTEGER,
        q3_2014 INTEGER,
        q4_2014 INTEGER,
        q1_2015 INTEGER,
        loaded_at TIMESTAMP DEFAULT GETDATE()
    )
    DISTSTYLE EVEN
    SORTKEY (lgd);
    """,
    aws_conn_id='aws_s3_conn',
    database='dev',
    cluster_identifier='dwh-redshift-dw',
    db_user='awsuser',
    dag=dag
)

# Load crime data into staging
load_staging_crime_data = S3ToRedshiftOperator(
    task_id='load_staging_crime_data',
    schema='public',
    table='stg_police_recorded_crime',
    s3_bucket='northern-ireland-crime-data',
    s3_key='police-recorded-crime-monthly-data.csv',
    copy_options=[
        "CSV",
        "IGNOREHEADER 1",
        "DATEFORMAT 'auto'",
        "MAXERROR 100000",
        "BLANKSASNULL",
        "EMPTYASNULL",
        "TRUNCATECOLUMNS",
        "COMPUPDATE OFF"
    ],
    redshift_conn_id='redshift_default',
    aws_conn_id='aws_s3_conn',
    dag=dag
)

# Load dwelling data into staging
load_staging_dwelling_data = S3ToRedshiftOperator(
    task_id='load_staging_dwelling_data',
    schema='public',
    table='stg_new_dwelling_completions',
    s3_bucket='northern-ireland-new-dwelling-data',
    s3_key='new-dwelling-completions-by-lgd-q1-2005-q1-2015.csv',
    copy_options=[
        "CSV",
        "IGNOREHEADER 1",
        "DATEFORMAT 'auto'",
        "MAXERROR 100000",
        "BLANKSASNULL",
        "EMPTYASNULL",
        "TRUNCATECOLUMNS",
        "COMPUPDATE OFF",
        "ACCEPTINVCHARS AS ' '"
    ],
    redshift_conn_id='redshift_default',
    aws_conn_id='aws_s3_conn',
    dag=dag
)

# Transform crime data
transform_crime_data = RedshiftDataOperator(
    task_id='transform_crime_data',
    sql="""
    BEGIN TRANSACTION;

    -- First, analyze the data and log any potential issues
    INSERT INTO error_records (
        calendar_year,
        month,
        policing_district,
        crime_type,
        data_measure,
        count,
        error_message
    )
    SELECT 
        calendar_year,
        month,
        policing_district,
        crime_type,
        data_measure,
        count as original_count,
        CASE
            WHEN NOT calendar_year ~ '^[0-9]+$' THEN 'Invalid year format'
            WHEN NULLIF(TRIM(count), '') IS NULL THEN 'Empty count value'
            WHEN TRIM(count) IN ('/0', 'NA', 'N/A', '-') THEN 'Special value: ' || TRIM(count)
            WHEN NOT REGEXP_REPLACE(TRIM(count), '[^0-9.]', '') ~ '^[0-9]*\.?[0-9]*$' THEN 'Invalid numeric format'
            ELSE 'Other validation error'
        END as error_message
    FROM stg_police_recorded_crime
    WHERE NOT calendar_year ~ '^[0-9]+$'
    OR NOT (
        NULLIF(TRIM(count), '') IS NULL
        OR TRIM(count) IN ('/0', 'NA', 'N/A', '-')
        OR REGEXP_REPLACE(TRIM(count), '[^0-9.]', '') ~ '^[0-9]*\.?[0-9]*$'
    );

    -- Delete existing records
    DELETE FROM police_recorded_crime 
    WHERE (calendar_year, month) IN (
        SELECT DISTINCT 
            calendar_year::INTEGER,
            month
        FROM stg_police_recorded_crime
        WHERE calendar_year ~ '^[0-9]+$'
    );

    -- Insert transformed data
    INSERT INTO police_recorded_crime (
        calendar_year,
        month,
        policing_district,
        crime_type,
        data_measure,
        count,
        loaded_at
    )
    WITH cleaned_data AS (
        SELECT 
            calendar_year::INTEGER as calendar_year,
            month,
            policing_district,
            crime_type,
            data_measure,
            CASE
                WHEN NULLIF(TRIM(count), '') IS NULL THEN NULL
                WHEN TRIM(count) IN ('/0', 'NA', 'N/A', '-') THEN NULL
                WHEN REGEXP_REPLACE(TRIM(count), '[^0-9.]', '') ~ '^[0-9]*\.?[0-9]*$' 
                    THEN TRIM(count)::DECIMAL(10,2)
                ELSE NULL
            END as count,
            GETDATE() as loaded_at
        FROM stg_police_recorded_crime
        WHERE calendar_year ~ '^[0-9]+$'
    )
    SELECT 
        calendar_year,
        month,
        policing_district,
        crime_type,
        data_measure,
        count,
        loaded_at
    FROM cleaned_data;

    -- Drop staging and error tables immediately after use
    DROP TABLE IF EXISTS stg_police_recorded_crime;
    DROP TABLE IF EXISTS error_records;

    COMMIT;
    """,
    aws_conn_id='aws_s3_conn',
    database='dev',
    cluster_identifier='dwh-redshift-dw',
    db_user='awsuser',
    dag=dag
)

# Transform dwelling data
transform_dwelling_data = RedshiftDataOperator(
    task_id='transform_dwelling_data',
    sql="""
    BEGIN TRANSACTION;

    -- First, analyze the data and log any potential issues
    INSERT INTO dwelling_error_records (lgd, error_message)
    SELECT 
        lgd,
        'Missing or invalid LGD name'
    FROM stg_new_dwelling_completions
    WHERE TRIM(lgd) IS NULL OR TRIM(lgd) = '';

    -- Clear existing data
    TRUNCATE TABLE new_dwelling_completions;

    -- Insert transformed data with validation
    INSERT INTO new_dwelling_completions
    SELECT
        TRIM(lgd) as lgd,
        NULLIF(TRIM(q1_2005), '')::INTEGER as q1_2005,
        NULLIF(TRIM(q2_2005), '')::INTEGER as q2_2005,
        NULLIF(TRIM(q3_2005), '')::INTEGER as q3_2005,
        NULLIF(TRIM(q4_2005), '')::INTEGER as q4_2005,
        NULLIF(TRIM(q1_2006), '')::INTEGER as q1_2006,
        NULLIF(TRIM(q2_2006), '')::INTEGER as q2_2006,
        NULLIF(TRIM(q3_2006), '')::INTEGER as q3_2006,
        NULLIF(TRIM(q4_2006), '')::INTEGER as q4_2006,
        NULLIF(TRIM(q1_2007), '')::INTEGER as q1_2007,
        NULLIF(TRIM(q2_2007), '')::INTEGER as q2_2007,
        NULLIF(TRIM(q3_2007), '')::INTEGER as q3_2007,
        NULLIF(TRIM(q4_2007), '')::INTEGER as q4_2007,
        NULLIF(TRIM(q1_2008), '')::INTEGER as q1_2008,
        NULLIF(TRIM(q2_2008), '')::INTEGER as q2_2008,
        NULLIF(TRIM(q3_2008), '')::INTEGER as q3_2008,
        NULLIF(TRIM(q4_2008), '')::INTEGER as q4_2008,
        NULLIF(TRIM(q1_2009), '')::INTEGER as q1_2009,
        NULLIF(TRIM(q2_2009), '')::INTEGER as q2_2009,
        NULLIF(TRIM(q3_2009), '')::INTEGER as q3_2009,
        NULLIF(TRIM(q4_2009), '')::INTEGER as q4_2009,
        NULLIF(TRIM(q1_2010), '')::INTEGER as q1_2010,
        NULLIF(TRIM(q2_2010), '')::INTEGER as q2_2010,
        NULLIF(TRIM(q3_2010), '')::INTEGER as q3_2010,
        NULLIF(TRIM(q4_2010), '')::INTEGER as q4_2010,
        NULLIF(TRIM(q1_2011), '')::INTEGER as q1_2011,
        NULLIF(TRIM(q2_2011), '')::INTEGER as q2_2011,
        NULLIF(TRIM(q3_2011), '')::INTEGER as q3_2011,
        NULLIF(TRIM(q4_2011), '')::INTEGER as q4_2011,
        NULLIF(TRIM(q1_2012), '')::INTEGER as q1_2012,
        NULLIF(TRIM(q2_2012), '')::INTEGER as q2_2012,
        NULLIF(TRIM(q3_2012), '')::INTEGER as q3_2012,
        NULLIF(TRIM(q4_2012), '')::INTEGER as q4_2012,
        NULLIF(TRIM(q1_2013), '')::INTEGER as q1_2013,
        NULLIF(TRIM(q2_2013), '')::INTEGER as q2_2013,
        NULLIF(TRIM(q3_2013), '')::INTEGER as q3_2013,
        NULLIF(TRIM(q4_2013), '')::INTEGER as q4_2013,
        NULLIF(TRIM(q1_2014), '')::INTEGER as q1_2014,
        NULLIF(TRIM(q2_2014), '')::INTEGER as q2_2014,
        NULLIF(TRIM(q3_2014), '')::INTEGER as q3_2014,
        NULLIF(TRIM(q4_2014), '')::INTEGER as q4_2014,
        NULLIF(TRIM(q1_2015), '')::INTEGER as q1_2015,
        GETDATE() as loaded_at
    FROM stg_new_dwelling_completions
    WHERE TRIM(lgd) IS NOT NULL AND TRIM(lgd) != '';

    -- Drop staging and error tables immediately after use
    DROP TABLE IF EXISTS stg_new_dwelling_completions;
    DROP TABLE IF EXISTS dwelling_error_records;

    COMMIT;
    """,
    aws_conn_id='aws_s3_conn',
    database='dev',
    cluster_identifier='dwh-redshift-dw',
    db_user='awsuser',
    dag=dag
)

# Data quality check for crime data
data_quality_check = RedshiftDataOperator(
    task_id='data_quality_check',
    sql="""
    -- Collect metrics
    WITH metrics AS (
        SELECT 
            'A. Total Records' as metric,
            COUNT(*) as value,
            'Total number of crime records loaded' as description
        FROM police_recorded_crime
        
        UNION ALL
        
        SELECT 
            'D. Records by Year - ' || calendar_year::VARCHAR,
            COUNT(*),
            'Number of records per year'
        FROM police_recorded_crime
        GROUP BY calendar_year
    )
    SELECT 
        SUBSTRING(metric, POSITION('. ' in metric) + 2) as metric,
        value,
        description
    FROM metrics
    ORDER BY metric;

    -- Drop archive tables
    DROP TABLE IF EXISTS error_records_archive;

    -- Analyze final table
    ANALYZE police_recorded_crime;
    """,
    aws_conn_id='aws_s3_conn',
    database='dev',
    cluster_identifier='dwh-redshift-dw',
    db_user='awsuser',
    dag=dag
)

# Data quality check for dwelling data
dwelling_data_quality_check = RedshiftDataOperator(
    task_id='dwelling_data_quality_check',
    sql="""
    -- Collect metrics
    WITH metrics AS (
        SELECT 
            'A. Total Dwelling Records' as metric,
            COUNT(*) as value,
            'Total number of dwelling records loaded' as description
        FROM new_dwelling_completions
        
        UNION ALL
        
        SELECT 
            'C. Null Values by Quarter',
            SUM(
                CASE WHEN q1_2015 IS NULL THEN 1 ELSE 0 END +
                CASE WHEN q4_2014 IS NULL THEN 1 ELSE 0 END +
                CASE WHEN q3_2014 IS NULL THEN 1 ELSE 0 END
            ),
            'Count of null values in recent quarters'
        FROM new_dwelling_completions
    )
    SELECT 
        SUBSTRING(metric, POSITION('. ' in metric) + 2) as metric,
        value,
        description
    FROM metrics
    ORDER BY metric;

    -- Drop archive tables
    DROP TABLE IF EXISTS dwelling_error_records_archive;

    -- Analyze final table
    ANALYZE new_dwelling_completions;
    """,
    aws_conn_id='aws_s3_conn',
    database='dev',
    cluster_identifier='dwh-redshift-dw',
    db_user='awsuser',
    dag=dag
)

# End operator
end_pipeline = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Set up task dependencies
start_pipeline >> [create_staging_crime_table, create_crime_table, create_staging_dwelling_table, create_dwelling_table]

# Crime data path
create_staging_crime_table >> load_staging_crime_data
create_crime_table >> load_staging_crime_data
load_staging_crime_data >> transform_crime_data >> data_quality_check

# Dwelling data path
create_staging_dwelling_table >> load_staging_dwelling_data
create_dwelling_table >> load_staging_dwelling_data
load_staging_dwelling_data >> transform_dwelling_data >> dwelling_data_quality_check

# Join paths at the end
[data_quality_check, dwelling_data_quality_check] >> end_pipeline