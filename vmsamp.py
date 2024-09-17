import psycopg2
import logging
from pystackql import StackQL
import time
import os
import json
import requests
import schedule

def connect_to_db():
    """Connect to the database"""

    global connection, pgcur
    connection = psycopg2.connect(host="db1",
                                  database = os.environ['POSTGRES_DB'],
                                  user = os.environ['POSTGRES_USER'],
                                  password = os.environ['POSTGRES_PASSWORD'])
    pgcur = connection.cursor()
    connection.autocommit = True
    if connection:
        logging.debug('pg connected')
        pgcur.execute("SET TIME ZONE %s",(utc_timezone,))
        pgcur.execute("SHOW TIMEZONE")
        logging.debug('pg timezone AFTER - %s',pgcur.fetchone())
    else:
        logging.critical('postgres unavailable, start the database to get going')


def create_table_availabilitysets():
    """Create table for the resource type availability sets"""

    create_availability_sets_table_query = """CREATE TABLE IF NOT EXISTS availabilitySets (id SERIAL PRIMARY KEY,
        apiVersions TEXT DEFAULT NULL,
        MaximumPlatformFaultDomainCount TEXT,
        capacity TEXT,
        costs TEXT, 
        family TEXT, 
        kind TEXT,
        locationsInfo JSONB,
        locations TEXT,
        name TEXT,
        resourceType TEXT,
        restrictions JSONB,
        size TEXT,
        tier TEXT,
        run_timestamp TIMESTAMP);"""
    try:
        pgcur.execute(create_availability_sets_table_query)
        logging.info("Table created : availabilitySets")
    except psycopg2.Error as e:
        logging.error("Error in creating table availabilitySets:",e)

def truncate_table_availabilitysets():
    """Truncate table availabilitysets"""

    query = """TRUNCATE TABLE availabilitySets;"""
    try:
        pgcur.execute(query)
        logging.info("Truncated table : availabilitysets")
    except psycopg2.Error as e:
        logging.critical(f"Error in truncating availabilitysets table:{e}")

def fetch_data_availabilitysets():
    """Fetch the data of the availability sets using StackQL"""

    try:
        query = """select * from azure.compute.resource_skus
               where subscriptionId = '%s' and resourceType = 'availabilitySets'
                ;""" % (subscription_id)
        res = stackql.execute(query)
        json_data = json.dumps(res)
        logging.info("Fetched the availabilitysets data from StackQL")
        return json_data
    except psycopg2.Error as e:
        logging.error(f"Error in Fetching availability sets data from StackQL:{e}")


def insert_into_availabilitysets(a_data):
    """Insert data into availabilitySets"""

    query = """
                WITH split_data AS (
                    SELECT jsonb_array_elements(%s::jsonb) AS item 
                )
                INSERT INTO availabilitySets (
                    apiVersions,
                    maximumPlatformFaultDomainCount,
                    capacity,
                    costs,
                    family,
                    kind,
                    locationsInfo,
                    locations,
                    name,
                    resourceType,
                    restrictions,
                    size,
                    tier,
                    run_timestamp
                )
                SELECT 
                    (CASE
                        WHEN item->>'apiVersions' = 'null' THEN NULL
                        ELSE item->>'apiVersions'
                    END) AS api_versions,
                    (
                        SELECT 
                            (CASE
                                WHEN elem->>'value' = 'null' THEN NULL
                                ELSE (elem->>'value')::INT
                            END)
                        FROM jsonb_array_elements(
                            CASE 
                                WHEN jsonb_typeof((item->>'capabilities')::jsonb) = 'array'
                                THEN (item->>'capabilities')::jsonb 
                                ELSE '[]'::jsonb 
                            END
                        ) AS elem
                        WHERE elem->>'name' = 'MaximumPlatformFaultDomainCount'
                    ) AS maximum_platform_fault_domain_count,
                    (CASE
                        WHEN item->>'capacity' = 'null' THEN NULL
                        ELSE item->>'capacity'
                    END) AS capacity,
                    (CASE
                        WHEN item->>'costs' = 'null' THEN NULL
                        ELSE item->>'costs'
                    END) AS costs,
                    (CASE
                        WHEN item->>'family' = 'null' THEN NULL
                        ELSE item->>'family'
                    END) AS family,
                    (CASE
                        WHEN item->>'kind' = 'null' THEN NULL
                        ELSE item->>'kind'
                    END) AS kind,
                    (CASE
                        WHEN item->'locationInfo' IS NULL THEN NULL
                        ELSE item->'locationInfo'
                    END) AS location_info,
                    (CASE
                        WHEN item->>'locations' = 'null' THEN NULL
                        ELSE item->>'locations'
                    END) AS locations,
                    (CASE
                        WHEN item->>'name' = 'null' THEN NULL
                        ELSE item->>'name'
                    END) AS name,
                    (CASE
                        WHEN item->>'resourceType' = 'null' THEN NULL
                        ELSE item->>'resourceType'
                    END) AS resource_type,
                    (CASE
                        WHEN item->'restrictions' = '"[]"' THEN NULL
                        ELSE item->'restrictions'
                    END) AS restrictions,
                    (CASE
                        WHEN item->>'size' = 'null' THEN NULL
                        ELSE item->>'size'
                    END) AS size,
                    (CASE
                        WHEN item->>'tier' = 'null' THEN NULL
                        ELSE item->>'tier'
                    END) AS tier,
                    CURRENT_TIMESTAMP AS run_timestamp
                FROM split_data;


        """
    try:
        pgcur.execute(query,(a_data,))
        logging.info("Data inserted into the table availabiltysets")
    except psycopg2.Error as e:
        logging.error(f"Error in Inserting into availability sets table :{e}")

def create_table_snapshots():
    """Create table for the resource type snapshots"""
    
    create_snapshots_query = """CREATE TABLE IF NOT EXISTS snapshots (
        id SERIAL PRIMARY KEY,
        apiVersions TEXT DEFAULT NULL, 
        capabilities TEXT DEFAULT NULL, 
        capacity TEXT, costs TEXT, 
        family TEXT,
        kind TEXT,
        locationInfo JSONB, 
        locations TEXT, 
        name TEXT,
        resourceType TEXT,
        restrictions JSONB,
        size TEXT,
        tier TEXT,
        run_timestamp TIMESTAMP);"""
    try:
        pgcur.execute(create_snapshots_query)
        logging.info("Table created : snapshots")
    except psycopg2.Error as e:
        logging.error(f"Error in creating table snapshots:{e}")

def truncate_table_snapshots():
    """Truncate table snapshots"""

    query = """TRUNCATE TABLE snapshots;"""
    try:
        pgcur.execute(query)
        logging.info("Truncated table :snapshots")
    except psycopg2.Error as e:
        logging.critical(f"Error in truncating the table snapshots:{e}")

        
def fetch_data_snapshots():
    """Fetch the data of the snapshots using StackQL"""
    
    try:
        query = """select * from azure.compute.resource_skus
                where subscriptionId = '%s' and resourceType = 'snapshots'
                    ;""" % (subscription_id)
        res = stackql.execute(query)
        json_data_str = json.dumps(res)
        logging.info("Fetched Snapshots data from StackQL")
        return json_data_str
    except psycopg2.Error as e:
        logging.error(f"Error in Fetching snapshots data from StackQL:{e}")

def insert_into_snapshots(s_data):
    """Insert data into Snapshots table"""

    query = """
            WITH split_data AS (
                SELECT jsonb_array_elements(%s::jsonb) AS item
                )
            INSERT INTO snapshots (
                apiVersions,
                capabilities,
                capacity,
                costs,
                family,
                kind,
                locationInfo,
                locations,
                name,
                resourceType,
                restrictions,
                size,
                tier,
                run_timestamp
            )
            SELECT 
                    (CASE
                        WHEN item->>'apiVersions' = 'null' THEN NULL
                        ELSE item->>'apiVersions'
                    END) AS api_versions,
                    (CASE 
                        WHEN item->>'capabilities' = 'null' THEN NULL
                        ELSE item->>'capabilities'
                    END) AS capabilities,
                    (CASE
                        WHEN item->>'capacity' = 'null' THEN NULL
                        ELSE item->>'capacity'
                    END) AS capacity,
                    (CASE
                        WHEN item->>'costs' = 'null' THEN NULL
                        ELSE item->>'costs'
                    END) AS costs,
                    (CASE
                        WHEN item->>'family' = 'null' THEN NULL
                        ELSE item->>'family'
                    END) AS family,
                    (CASE
                        WHEN item->>'kind' = 'null' THEN NULL
                        ELSE item->>'kind'
                    END) AS kind,
                    (CASE
                        WHEN item->'locationInfo' IS NULL THEN NULL
                        ELSE item->'locationInfo'
                    END) AS location_info,
                    (CASE
                        WHEN item->>'locations' = 'null' THEN NULL
                        ELSE item->>'locations'
                    END) AS locations,
                    (CASE
                        WHEN item->>'name' = 'null' THEN NULL
                        ELSE item->>'name'
                    END) AS name,
                    (CASE
                        WHEN item->>'resourceType' = 'null' THEN NULL
                        ELSE item->>'resourceType'
                    END) AS resource_type,
                    (CASE
                        WHEN item->'restrictions' = '"[]"' THEN NULL
                        ELSE item->'restrictions'
                    END) AS restrictions,
                    (CASE
                        WHEN item->>'size' = 'null' THEN NULL
                        ELSE item->>'size'
                    END) AS size,
                    (CASE
                        WHEN item->>'tier' = 'null' THEN NULL
                        ELSE item->>'tier'
                    END) AS tier,
                    CURRENT_TIMESTAMP AS run_timestamp
                FROM split_data;
        """
    try:
        pgcur.execute(query,(s_data,))
        logging.info("Data inserted into the table snapshots")
    except psycopg2.Error as e:
        logging.error(f"Error in Inserting into the snapshots table:{e}")

def create_table_disks():
    """Create table disks"""

    create_disks_query = """CREATE TABLE IF NOT EXISTS disks (
            id SERIAL PRIMARY KEY,
            apiVersions TEXT DEFAULT NULL,
            MinIopsPerGiBReadOnly INT,
            MaxIOpsReadWrite INT,
            MaxZonalFaultDomainCount INT, 
            BurstCreditBucketSizeInIO INT,
            MaxBurstBandwidthMBps INT,
            MaxSizeGiB INT,
            MaxIopsPerGiBReadWrite INT,
            MinIopsReadOnly INT,
            MaxIOps INT, 
            MinIOpsReadWrite INT,
            MinBandwidthMBps INT, 
            MaxBandwidthMBpsReadOnly INT,
            MinIopsPerGiBReadWrite INT,
            MinIOSizeKiBps INT,
            MaxBandwidthMBpsPerformancePlus INT,
            MinBandwidthMBpsReadOnly INT, 
            MaxValueOfMaxShares INT,
            MaxBurstDurationInMin INT, 
            BurstCreditBucketSizeInGiB INT, 
            MinBandwidthMBpsReadWrite INT,
            MaxBandwidthMBpsReadWrite INT,
            PlatformFaultDomainCount INT, 
            MinIOps INT, 
            MaxIOpsPerformancePlus INT, 
            BillingPartitionSizes TEXT, 
            MaxBurstIops INT,
            MinSizeGiB INT, 
            MaxIopsPerGiBReadOnly INT,
            MaxBandwidthMBps INT, 
            MaxIOSizeKiBps INT,
            MaxIopsReadOnly INT, 
            capacity TEXT, 
            costs TEXT,
            family TEXT, 
            kind TEXT, 
            locationInfo JSONB,
            locations TEXT,
            name TEXT,
            resourceType TEXT, 
            restrictions JSONB,
            size TEXT,
            tier TEXT, 
            run_timestamp TIMESTAMP);"""   
    try:
        pgcur.execute(create_disks_query)
        logging.info("Table created : disks")
    except psycopg2.Error as e:
        logging.error(f"Error in creating table disks:{e}")

def truncate_table_disks():
    """Truncate the table disks"""
    try:
        pgcur.execute("""TRUNCATE TABLE disks;""")
        logging.info("Truncated table : disks ")
    except psycopg2.Error as e:
        logging.Error(f"Error in truncating table disks:{e}")

def fetch_data_disks():
    """Fetch the data of the disks using StackQL"""
    try:
        query = """select * from azure.compute.resource_skus
                where subscriptionId = '%s' and resourceType = 'disks'
                    ;""" % (subscription_id)
        res = stackql.execute(query)
        json_data_str = json.dumps(res)
        logging.info("Fetched Disks data from StackQL")
        return json_data_str
    
    except psycopg2.Error as e:
        logging.error(f"Error in Fetching disks data from StackQL:{e}")

def insert_into_disks(d_data):
    query = """
            WITH split_data AS (
                SELECT jsonb_array_elements(%s::jsonb) AS item
            )
            INSERT INTO disks (
                apiVersions,
                MinIopsPerGiBReadOnly,
                MaxIOpsReadWrite,
                MaxZonalFaultDomainCount,
                BurstCreditBucketSizeInIO,
                MaxBurstBandwidthMBps,
                MaxSizeGiB,
                MaxIopsPerGiBReadWrite,
                MinIopsReadOnly,
                MaxIOps,
                MinIOpsReadWrite,
                MinBandwidthMBps,
                MaxBandwidthMBpsReadOnly,
                MinIopsPerGiBReadWrite,
                MinIOSizeKiBps,
                MaxBandwidthMBpsPerformancePlus,
                MinBandwidthMBpsReadOnly,
                MaxValueOfMaxShares,
                MaxBurstDurationInMin,
                BurstCreditBucketSizeInGiB,
                MinBandwidthMBpsReadWrite,
                MaxBandwidthMBpsReadWrite,
                PlatformFaultDomainCount,
                MinIOps,
                MaxIOpsPerformancePlus,
                BillingPartitionSizes,
                MaxBurstIops,
                MinSizeGiB,
                MaxIopsPerGiBReadOnly,
                MaxBandwidthMBps,
                MaxIOSizeKiBps,
                MaxIopsReadOnly,
                capacity,
                costs,
                family,
                kind,
                locationInfo,
                locations,
                name,
                resourceType,
                restrictions,
                size,
                tier,
                run_timestamp
            )
            
            SELECT 
                (CASE
                    WHEN item->>'apiVersions' = 'null' THEN NULL
                    ELSE item->>'apiVersions'
                END) AS api_versions,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxIopsPerGiBReadOnly'
                ) AS MaxIopsPerGiBReadOnly,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MinIopsPerGiBReadOnly'
                ) AS MinIopsPerGiBReadOnly,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxIOpsReadWrite'
                ) AS MaxIOpsReadWrite,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxZonalFaultDomainCount'
                ) AS MaxZonalFaultDomainCount,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'BurstCreditBucketSizeInIO'
                ) AS BurstCreditBucketSizeInIO,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxBurstBandwidthMBps'
                ) AS MaxBurstBandwidthMBps,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxSizeGiB'
                ) AS MaxSizeGiB,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxIopsPerGiBReadWrite'
                ) AS MaxIopsPerGiBReadWrite,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MinIopsReadOnly'
                ) AS MinIopsReadOnly,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxIOps'
                ) AS MaxIOps,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MinIOpsReadWrite'
                ) AS MinIOpsReadWrite,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MinBandwidthMBps'
                ) AS MinBandwidthMBps,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxBandwidthMBpsReadOnly'
                ) AS MaxBandwidthMBpsReadOnly,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MinIopsPerGiBReadWrite'
                ) AS MinIopsPerGiBReadWrite,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxBandwidthMBpsPerformancePlus'
                ) AS MaxBandwidthMBpsPerformancePlus,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MinIOSizeKiBps'
                ) AS MinIOSizeKiBps,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MinBandwidthMBpsReadOnly'
                ) AS MinBandwidthMBpsReadOnly,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxBurstDurationInMin'
                ) AS MaxBurstDurationInMin,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'BurstCreditBucketSizeInGiB'
                ) AS BurstCreditBucketSizeInGiB,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MinBandwidthMBpsReadWrite'
                ) AS MinBandwidthMBpsReadWrite,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxBandwidthMBpsReadWrite'
                ) AS MaxBandwidthMBpsReadWrite,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'PlatformFaultDomainCount'
                ) AS PlatformFaultDomainCount,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MinIOps'
                ) AS MinIOps,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxIOpsPerformancePlus'
                ) AS MaxIOpsPerformancePlus,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::text
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'BillingPartitionSizes'
                ) AS BillingPartitionSizes,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxBurstIops'
                ) AS MaxBurstIops,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MinSizeGiB'
                ) AS MinSizeGiB,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxIopsPerGiBReadOnly'
                ) AS MaxIopsPerGiBReadOnly,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxBandwidthMBps'
                ) AS MaxBandwidthMBps,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxIOSizeKiBps'
                ) AS MaxIOSizeKiBps,
                (
                    SELECT 
                        (CASE
                            WHEN elem->>'value' = 'null' THEN NULL
                            ELSE (elem->>'value')::INT
                        END)
                    FROM jsonb_array_elements(
                        (item->>'capabilities')::jsonb
                    ) AS elem
                    WHERE elem->>'name' = 'MaxIopsReadOnly'
                ) AS MaxIopsReadOnly,
                (
                    CASE
                        WHEN item->>'capacity' = 'null' THEN NULL
                        ELSE item->>'capacity'
                    END
                ) AS capacity,
                (
                    CASE
                        WHEN item->>'costs' = 'null' THEN NULL
                        ELSE item->>'costs'
                    END
                ) AS costs,
                (
                    CASE
                        WHEN item->>'family' = 'null' THEN NULL
                        ELSE item->>'family'
                    END
                ) AS family,
                (
                    CASE
                        WHEN item->>'kind' = 'null' THEN NULL
                        ELSE item->>'kind'
                    END
                ) AS kind,
                (
                    CASE
                        WHEN item->'locationInfo' IS NULL THEN NULL
                        ELSE item->'locationInfo'
                    END
                ) AS location_info,
                (
                    CASE
                        WHEN item->>'locations' = 'null' THEN NULL
                        ELSE item->>'locations'
                    END
                ) AS locations,
                (
                    CASE
                        WHEN item->>'name' = 'null' THEN NULL
                        ELSE item->>'name'
                    END
                ) AS name,
                (
                    CASE
                        WHEN item->>'resourceType' = 'null' THEN NULL
                        ELSE item->>'resourceType'
                    END
                ) AS resource_type,
                (
                    CASE
                        WHEN item->'restrictions' = '"[]"' THEN NULL
                        ELSE item->'restrictions'
                    END
                ) AS restrictions,
                (
                    CASE
                        WHEN item->>'size' = 'null' THEN NULL
                        ELSE item->>'size'
                    END
                ) AS size,
                (
                    CASE
                        WHEN item->>'tier' = 'null' THEN NULL
                        ELSE item->>'tier'
                    END
                ) AS tier,
                CURRENT_TIMESTAMP AS run_timestamp
            FROM split_data;

        """
    try:
        pgcur.execute(query,(d_data,))
        logging.info("Data inserted into the table disks")
    except psycopg2.Error as e:
        logging.error(f"Error in inserting data into the disks table :{e}")

def create_table_hostgroups():
    """Create table for the reource type hostgroups/hosts"""

    create_hostgroups_query = """
            CREATE TABLE IF NOT EXISTS hostGroups_hosts (
            id SERIAL PRIMARY KEY, 
            apiVersions TEXT DEFAULT NULL,
            Cores INT,
            vCPUsPerCore INT,
            vCPUs INT,
            SupportsAutoplacement boolean,
            capacity TEXT,
            costs TEXT,
            family TEXT, 
            kind TEXT,
            locationInfo JSONB,
            locations TEXT,
            name TEXT,
            resourceType TEXT,
            restrictions JSONB,
            size TEXT, 
            tier TEXT,
            run_timestamp TIMESTAMP);
        
        """
    try:
        pgcur.execute(create_hostgroups_query)
        logging.info("Table created : hostgroups_hosts")
    except psycopg2.Error as e:
        logging.error(f"Error in creating table for hostgroups/hosts:{e}")

def truncate_table_hostgroups():
    """Truncate the table hostgroups_hosts"""

    query = "TRUNCATE TABLE hostGroups_hosts"
    try:
        pgcur.execute(query)
        logging.info("Truncated table: hostGroups_hosts")
    except psycopg2.Error as e:
        logging.critical(f"Error in truncating the table hostGroups_hosts:{e}")

def fetch_data_hostgroups():
    """Fetch the data of the hostGroups/hosts using StackQL"""
        
    try:
        query = """select * from azure.compute.resource_skus
                where subscriptionId = '%s' and resourceType = 'hostGroups/hosts'
                    ;""" % (subscription_id)
        res = stackql.execute(query)
        json_data_str = json.dumps(res)
        logging.info("Fetched hostGroups/hosts data from StackQL")
        return json_data_str
    except psycopg2.Error as e:
        logging.critical("Error in Fetching hostgroups/hosts data from StackQL")

def insert_into_hostgroup(h_data):
    """Insert data into hostGroups_hosts table"""

    query = """
            WITH split_data AS (
                            SELECT jsonb_array_elements(%s::jsonb) AS item 
                        )
                        INSERT INTO hostgroups_hosts (
                            apiVersions,
                            Cores ,
                            vCPUsPerCore ,
                            vCPUs ,
                            SupportsAutoplacement ,
                            capacity,
                            costs,
                            family,
                            kind,
                            locationInfo,
                            locations,
                            name,
                            resourceType,
                            restrictions,
                            size,
                            tier,
                            run_timestamp
                        )
                        
                        SELECT 
                            (CASE
                                WHEN item->>'apiVersions' = 'null' THEN NULL
                                ELSE item->>'apiVersions'
                            END) AS api_versions,
                            (
                                SELECT 
                                    (CASE
                                        WHEN elem->>'value' = 'null' THEN NULL
                                        ELSE (elem->>'value')::INT
                                    END)
                                FROM jsonb_array_elements(
                                    (item->>'capabilities')::jsonb
                                ) AS elem
                                WHERE elem->>'name' = 'Cores'
                            ) AS Cores,
                            (
                                SELECT 
                                    (CASE
                                        WHEN elem->>'value' = 'null' THEN NULL
                                        ELSE (elem->>'value')::INT
                                    END)
                                FROM jsonb_array_elements(
                                    (item->>'capabilities')::jsonb
                                ) AS elem
                                WHERE elem->>'name' = 'vCPUsPerCore'
                            ) AS vCPUsPerCore,
                            (
                                SELECT 
                                    (CASE
                                        WHEN elem->>'value' = 'null' THEN NULL
                                        ELSE (elem->>'value')::INT
                                    END)
                                FROM jsonb_array_elements(
                                    (item->>'capabilities')::jsonb
                                ) AS elem
                                WHERE elem->>'name' = 'vCPUs'
                            ) AS vCPUs,
                            (
                                SELECT 
                                    (CASE
                                        WHEN elem->>'value' = 'null' THEN NULL
                                        ELSE (elem->>'value')::boolean
                                    END)
                                FROM jsonb_array_elements(
                                    (item->>'capabilities')::jsonb
                                ) AS elem
                                WHERE elem->>'name' = 'SupportsAutoplacement'
                            ) AS SupportsAutoplacement,
                        
                            (
                                CASE
                                    WHEN item->>'capacity' = 'null' THEN NULL
                                    ELSE item->>'capacity'
                                END
                            ) AS capacity,
                            (
                                CASE
                                    WHEN item->>'costs' = 'null' THEN NULL
                                    ELSE item->>'costs'
                                END
                            ) AS costs,
                            (
                                CASE
                                    WHEN item->>'family' = 'null' THEN NULL
                                    ELSE item->>'family'
                                END
                            ) AS family,
                            (
                                CASE
                                    WHEN item->>'kind' = 'null' THEN NULL
                                    ELSE item->>'kind'
                                END
                            ) AS kind,
                            (
                                CASE
                                    WHEN item->'locationInfo' IS NULL THEN NULL
                                    ELSE item->'locationInfo'
                                END
                            ) AS location_info,
                            (
                                CASE
                                    WHEN item->>'locations' = 'null' THEN NULL
                                    ELSE item->>'locations'
                                END
                            ) AS locations,
                            (
                                CASE
                                    WHEN item->>'name' = 'null' THEN NULL
                                    ELSE item->>'name'
                                END
                            ) AS name,
                            (
                                CASE
                                    WHEN item->>'resourceType' = 'null' THEN NULL
                                    ELSE item->>'resourceType'
                                END
                            ) AS resource_type,
                            (
                                CASE
                                    WHEN item->'restrictions' = '"[]"' THEN NULL
                                    ELSE item->'restrictions'
                                END
                            ) AS restrictions,
                            (
                                CASE
                                    WHEN item->>'size' = 'null' THEN NULL
                                    ELSE item->>'size'
                                END
                            ) AS size,
                            (
                                CASE
                                    WHEN item->>'tier' = 'null' THEN NULL
                                    ELSE item->>'tier'
                                END
                            ) AS tier,
                            CURRENT_TIMESTAMP AS run_timestamp
                        FROM split_data;

            """
    try:
        pgcur.execute(query,(h_data,))
        logging.info("Data inserted into the table hostgroups_hosts")
    except psycopg2.Error as e:
        logging.critical(f"Error in inserting data into the hostGroups_hosts table:{e}")

    

def create_table_vms():
    """Create Table for the resource type virtual machines"""

    create_virtual_machines_query = """CREATE TABLE IF NOT EXISTS virtualMachines (
            id SERIAL PRIMARY KEY,
            apiVersions TEXT DEFAULT NULL,
            LowPriorityCapable BOOLEAN,
            MemoryGB NUMERIC, 
            vCPUsAvailable INTEGER, 
            CapacityReservationSupported BOOLEAN, 
            CachedDiskBytes NUMERIC,
            HyperVGenerations TEXT,
            vCPUsPerCore INTEGER,
            MaxDataDiskCount INTEGER, 
            RdmaEnabled BOOLEAN,
            CombinedTempDiskAndCachedIOPS INTEGER,
            UltraSSDAvailable BOOLEAN, 
            VMDeploymentTypes TEXT,
            CombinedTempDiskAndCachedReadBytesPerSecond NUMERIC, 
            RetirementDateUtc DATE,
            OSVhdSizeMB INTEGER,
            MaxResourceVolumeMB INTEGER,
            TrustedLaunchDisabled BOOLEAN, 
            MaxWriteAcceleratorDisksAllowed INTEGER,
            ParentSize TEXT, 
            NvmeSizePerDiskInMiB INTEGER,
            NvmeDiskSizeInMiB INTEGER, 
            CpuArchitectureType TEXT,
            UncachedDiskIOPS INTEGER,
            vCPUs INTEGER, 
            PremiumIO BOOLEAN, 
            SupportedEphemeralOSDiskPlacements TEXT,
            ConfidentialComputingType TEXT, 
            DiskControllerTypes TEXT,
            ACUs INTEGER, 
            MemoryPreservingMaintenanceSupported BOOLEAN, 
            EncryptionAtHostSupported BOOLEAN, 
            MaxNetworkInterfaces INTEGER, 
            HibernationSupported BOOLEAN, 
            UncachedDiskBytesPerSecond NUMERIC, 
            EphemeralOSDiskSupported BOOLEAN,
            GPUs INTEGER,
            AcceleratedNetworkingEnabled BOOLEAN,
            CombinedTempDiskAndCachedWriteBytesPerSecond NUMERIC, 
            capacity TEXT,
            costs TEXT,
            family TEXT, 
            kind TEXT,
            locationInfo JSONB,
            locations TEXT, 
            name TEXT,
            resourceType TEXT,
            restrictions JSONB, 
            size TEXT, 
            tier TEXT,
            run_timestamp TIMESTAMP);"""
    try:
        pgcur.execute(create_virtual_machines_query)
        logging.info("Table created : virtualMachines")
    except psycopg2.Error as e:
        logging.error(f"Error in creating table virtualMachines:{e}")

def truncate_table_vms():
    """Truncate the table virtualMachines"""

    try:
        pgcur.execute("""TRUNCATE TABLE virtualMachines""")
        logging.info("Truncated table : virtualMachines")
    except psycopg2.Error as e:
        logging.Error(f"Error in truncating table virtualMachines:{e}")

def fetch_data_vms():
    """Fetch the data of the virtual machines using StackQL"""

    try:
        query = """select * from azure.compute.resource_skus
               where subscriptionId = '%s' and resourceType = 'virtualMachines'
                ;""" % (subscription_id)
        res = stackql.execute(query)
        json_data = json.dumps(res)
        logging.info("Fetched virtualmachines data from StackQL")
        return json_data
    except psycopg2.Error as e:
        logging.error(f"Error in fetching virtual machines data from StackQL:{e} ")

def insert_into_vms(v_data):
    """Insert data into the table virtualMachines"""

    query ="""
            WITH split_data AS (SELECT jsonb_array_elements(%s::jsonb) AS item )

            INSERT INTO virtualMachines (
                apiVersions,
                LowPriorityCapable,
                MemoryGB,
                vCPUsAvailable,
                CapacityReservationSupported,
                CachedDiskBytes,
                HyperVGenerations,
                vCPUsPerCore,
                MaxDataDiskCount,
                RdmaEnabled,
                CombinedTempDiskAndCachedIOPS,
                UltraSSDAvailable,
                VMDeploymentTypes,
                CombinedTempDiskAndCachedReadBytesPerSecond,
                RetirementDateUtc,
                OSVhdSizeMB,
                MaxResourceVolumeMB,
                TrustedLaunchDisabled,
                MaxWriteAcceleratorDisksAllowed,
                ParentSize,
                NvmeSizePerDiskInMiB,
                NvmeDiskSizeInMiB,
                CpuArchitectureType,
                UncachedDiskIOPS,
                vCPUs,
                PremiumIO,
                SupportedEphemeralOSDiskPlacements,
                ConfidentialComputingType,
                DiskControllerTypes,
                ACUs,
                MemoryPreservingMaintenanceSupported,
                EncryptionAtHostSupported,
                MaxNetworkInterfaces,
                HibernationSupported,
                UncachedDiskBytesPerSecond,
                EphemeralOSDiskSupported,
                GPUs,
                AcceleratedNetworkingEnabled,
                CombinedTempDiskAndCachedWriteBytesPerSecond,
                capacity,
                costs,
                family,
                kind,
                locationInfo,
                locations,
                name,
                resourceType,
                restrictions,
                size,
                tier,
                run_timestamp
            )

                        
                        
            SELECT 
                (CASE
                    WHEN item->>'apiVersions' = 'null' THEN NULL
                    ELSE item->>'apiVersions'
                END) AS api_versions,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::boolean
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) AS elem
                WHERE elem->>'name' = 'LowPriorityCapable') 
                AS LowPriorityCapable,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::NUMERIC
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb)
                AS elem
                WHERE elem->>'name' = 'MemoryGB') 
                AS MemoryGB,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::INT
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'vCPUsAvailable')
                AS vCPUsAvailable,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::BOOLEAN
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'CapacityReservationSupported') 
                AS CapacityReservationSupported,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::NUMERIC
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'CachedDiskBytes') 
                AS CachedDiskBytes,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::TEXT
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'HyperVGenerations') 
                AS HyperVGenerations,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::INT
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'vCPUsPerCore') 
                AS vCPUsPerCore,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::INT
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'MaxDataDiskCount') 
                AS MaxDataDiskCount,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::BOOLEAN
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'RdmaEnabled') 
                AS RdmaEnabled,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::INT
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'CombinedTempDiskAndCachedIOPS') 
                AS CombinedTempDiskAndCachedIOPS,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::BOOLEAN
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'UltraSSDAvailable') 
                AS UltraSSDAvailable,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::TEXT
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) AS elem
                WHERE elem->>'name' = 'VMDeploymentTypes') 
                AS VMDeploymentTypes,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::NUMERIC
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'CombinedTempDiskAndCachedReadBytesPerSecond') 
                AS CombinedTempDiskAndCachedReadBytesPerSecond,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::DATE
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'RetirementDateUtc'
                ) AS RetirementDateUtc,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::INT
                    END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'OSVhdSizeMB') 
                AS OSVhdSizeMB,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::INT
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'MaxResourceVolumeMB') 
                AS MaxResourceVolumeMB,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::BOOLEAN
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'TrustedLaunchDisabled'
                ) AS TrustedLaunchDisabled,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::INT
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) AS elem
                WHERE elem->>'name' = 'MaxWriteAcceleratorDisksAllowed') 
                AS MaxWriteAcceleratorDisksAllowed,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::TEXT
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'ParentSize') 
                AS ParentSize,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::INT
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'NvmeSizePerDiskInMiB') 
                AS  NvmeSizePerDiskInMiB,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::INT
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'NvmeDiskSizeInMiB'
                ) AS  NvmeDiskSizeInMiB,
                (SELECT 
                    (CASE
                    WHEN elem->>'value' = 'null' THEN NULL
                    ELSE (elem->>'value')::TEXT
                    END)
                FROM jsonb_array_elements((item->>'capabilities')::jsonb) 
                AS elem
                WHERE elem->>'name' = 'CpuArchitectureType') 
                AS  CpuArchitectureType,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::INT
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'UncachedDiskIOPS'
                ) AS  UncachedDiskIOPS,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::INT
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'vCPUs'
                ) AS  vCPUs,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::BOOLEAN
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'PremiumIO'
                ) AS  PremiumIO,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::TEXT
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'SupportedEphemeralOSDiskPlacements'
                ) AS  SupportedEphemeralOSDiskPlacements,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::TEXT
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'ConfidentialComputingType'
                ) AS  ConfidentialComputingType,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::TEXT
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'DiskControllerTypes'
                ) AS  DiskControllerTypes,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::INT
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'ACUs'
                ) AS  ACUs,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::BOOLEAN
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'MemoryPreservingMaintenanceSupported'
                ) AS  MemoryPreservingMaintenanceSupported,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::BOOLEAN
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'EncryptionAtHostSupported'
                ) AS  EncryptionAtHostSupported,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::INT
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'MaxNetworkInterfaceS'
                ) AS  MaxNetworkInterfaceS,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::BOOLEAN
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'HibernationSupported'
                ) AS  HibernationSupported,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::NUMERIC
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'UncachedDiskBytesPerSecond'
                ) AS  UncachedDiskBytesPerSecond,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::BOOLEAN
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'EphemeralOSDiskSupported'
                ) AS  EphemeralOSDiskSupported,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::INT
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'GPUs'
                ) AS  GPUs,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::BOOLEAN
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'AcceleratedNetworkingEnabled'
                ) AS  AcceleratedNetworkingEnabled,
                (
                SELECT 
                (CASE
                WHEN elem->>'value' = 'null' THEN NULL
                ELSE (elem->>'value')::NUMERIC
                END)
                FROM jsonb_array_elements(
                (item->>'capabilities')::jsonb
                ) AS elem
                WHERE elem->>'name' = 'CombinedTempDiskAndCachedWriteBytesPerSecond'
                ) AS CombinedTempDiskAndCachedWriteBytesPerSecond,



                (
                CASE
                WHEN item->>'capacity' = 'null' THEN NULL
                ELSE item->>'capacity'
                END
                ) AS capacity,
                (
                CASE
                WHEN item->>'costs' = 'null' THEN NULL
                ELSE item->>'costs'
                END
                ) AS costs,
                (
                CASE
                WHEN item->>'family' = 'null' THEN NULL
                ELSE item->>'family'
                END
                ) AS family,
                (
                CASE
                WHEN item->>'kind' = 'null' THEN NULL
                ELSE item->>'kind'
                END
                ) AS kind,
                (
                CASE
                WHEN item->'locationInfo' IS NULL THEN NULL
                ELSE item->'locationInfo'
                END
                ) AS location_info,
                (SELECT jsonb_array_elements_text((item->>'locations')::jsonb)) AS locations,
            
                (
                CASE
                WHEN item->>'name' = 'null' THEN NULL
                ELSE item->>'name'
                END
                ) AS name,
                (
                CASE
                WHEN item->>'resourceType' = 'null' THEN NULL
                ELSE item->>'resourceType'
                END
                ) AS resource_type,
                (
                CASE
                WHEN item->'restrictions' = '"[]"' THEN NULL
                ELSE item->'restrictions'
                END
                ) AS restrictions,
                (
                CASE
                WHEN item->>'size' = 'null' THEN NULL
                ELSE item->>'size'
                END
                ) AS size,
                (
                CASE
                WHEN item->>'tier' = 'null' THEN NULL
                ELSE item->>'tier'
                END
                ) AS tier,
                CURRENT_TIMESTAMP AS run_timestamp
                FROM split_data ;
                        """
    try:
        pgcur.execute(query,(v_data,))
        logging.info("Data inserted into the table virtualMachines")
    except psycopg2.Error as e:
        logging.error(f"Error in Inserting into virtualMachines table :{e}")


def create_table_azure_rates():
    
    """creates table to store the azure products' prices"""
    create_vm_rates_table_query = """
        CREATE TABLE IF NOT EXISTS azure_rates(
            id SERIAL PRIMARY KEY,
            currency_code VARCHAR(10),
            tier_minimum_units NUMERIC,
            reservation_term VARCHAR(50),
            retail_price NUMERIC,
            unit_price NUMERIC,
            arm_region_name VARCHAR(100),
            location VARCHAR(100),
            effective_start_date TIMESTAMP,
            meter_id VARCHAR(100),
            meter_name VARCHAR(100),
            product_id VARCHAR(100),
            sku_id VARCHAR(100),
            product_name VARCHAR(100),
            sku_name VARCHAR(100),
            service_name VARCHAR(100),
            service_id VARCHAR(100),
            service_family VARCHAR(100),
            unit_of_measure VARCHAR(50),
            type VARCHAR(50),
            is_primary_meter_region BOOLEAN,
            arm_sku_name VARCHAR(100),
            savings_plan_unit_price_3y NUMERIC,
            savings_plan_retail_price_3y NUMERIC,
            savings_plan_term_3y VARCHAR(50),
            savings_plan_unit_price_1y NUMERIC,
            savings_plan_retail_price_1y NUMERIC,
            savings_plan_term_1y VARCHAR(50),
            run_timestamp TIMESTAMP
        );
        """ 
    try:
        pgcur.execute(create_vm_rates_table_query)
        logging.info("Table created : azure_rates")
    except psycopg2.Error as e:
        logging.error(f"Error in creating table azure_rates :{e}")

def truncate_table_azure_rates():
    """Truncate table azure_rates"""
    query = """TRUNCATE TABLE azure_rates;"""
    try:
        pgcur.execute(query)
        logging.info("Truncated table : azure_rates")
    except psycopg2.Error as e:
        logging.critical(f"Error in truncating azure_rates table:{e}")

def fetch_data_rates(api_url):
    """Fetch Azure rates data"""

    try:
        response = requests.get(api_url)
        response.raise_for_status()
        json_data = response.json()
        vm_rates = json_data.get("Items", [])   
        processed_data = json.dumps(vm_rates) 
        api_url = json_data.get("NextPageLink")
        return api_url,processed_data
    
    except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching Azure VM rates : {e}")
            logging.info(f"Retrying in 5 seconds...")
            time.sleep(5)
def insert_into_azure_rates(r_data):
    """Insert data into azure_rates table"""

    query ="""
            WITH split_data AS (
                    SELECT jsonb_array_elements(%s::jsonb) AS item 
                )
                INSERT INTO azure_rates (
                    currency_code, 
                    tier_minimum_units, 
                    reservation_term, 
                    retail_price, 
                    unit_price,
                    arm_region_name, 
                    location, 
                    effective_start_date, 
                    meter_id, 
                    meter_name,
                    product_id, 
                    sku_id, 
                    product_name, 
                    sku_name, 
                    service_name,
                    service_id, 
                    service_family, 
                    unit_of_measure, 
                    type, 
                    is_primary_meter_region,
                    arm_sku_name, 
                    savings_plan_unit_price_3y, 
                    savings_plan_retail_price_3y, 
                    savings_plan_term_3y, 
                    savings_plan_unit_price_1y, 
                    savings_plan_retail_price_1y, 
                    savings_plan_term_1y, 
                    run_timestamp
                )  
                SELECT 
                    item->>'currencyCode' AS currencyCode,
                    (item->>'tierMinimumUnits')::NUMERIC AS tierMinimumUnits ,
                    item->>'reservationTerm' AS reservationTerm,
                    (item->>'retailPrice')::NUMERIC As retailPrice,
                    (item->>'unitPrice')::NUMERIC AS unitPrice,
                    item->>'armRegionName' AS armRegionName,
                    item->>'location' AS location,
                    (item->>'effectiveStartDate')::DATE AS effectiveStartDate,
                    item->>'meterId' AS meterId,
                    item->>'meterName' AS meterName,
                    item->>'productId' AS productId,
                    item->>'skuId' AS skuId,
                    item->>'productName' AS productName,
                    item->>'skuName' AS skuName,
                    item->>'serviceName' AS serviceName,
                    item->>'serviceId' AS serviceId,
                    item->>'serviceFamily' AS serviceFamily,
                    item->>'unitOfMeasure' AS unitOfMeasure,
                    item->>'type' AS type,
                    (item->>'isPrimaryMeterRegion')::BOOLEAN AS isPrimaryMeterRegion,
                    item->>'armSkuName' AS armSkuName,
                    (item->'savingsPlan'->0->>'unitPrice')::NUMERIC AS savings_plan_unit_price_3y,
                    (item->'savingsPlan'->0->>'retailPrice')::NUMERIC AS savings_plan_retail_price_3y,
                    item->'savingsPlan'->0->>'term' AS savings_plan_term_3y,
                    (item->'savingsPlan'->1->>'unitPrice')::NUMERIC AS savings_plan_unit_price_1y,
                    (item->'savingsPlan'->1->>'retailPrice')::NUMERIC AS savings_plan_retail_price_1y,
                    item->'savingsPlan'->1->>'term' AS savings_plan_term_1y,
                    CURRENT_TIMESTAMP AS run_timestamp
                    FROM split_data
                    """
    try:
        pgcur.execute(query,(r_data,))
        logging.info("Inserted data into table: Azure rates")
    except psycopg2.Error as e:
        logging.error(f"Error in inserting data into azure_rates table:{e}")

def create_table_vm_pricing():
        """Create table vm_pricing to store virtual machines and cost and characteristics"""

        try:
            pgcur.execute("""
            CREATE TABLE IF NOT EXISTS vm_pricing(
                    Name VARCHAR,
                    Location VARCHAR,
                    Instance_memory NUMERIC,
                    vCPUs INT,
                    Instance_Storage NUMERIC,
                    Linux_On_Demand_Cost NUMERIC,
                    Linux_Savings_Price_1_Year NUMERIC,
                    Linux_Savings_Price_3_Years NUMERIC,
                    Linux_Reservation_1_Year NUMERIC,
                    Linux_Reservation_3_Years NUMERIC,
                    Linux_Spot_Cost NUMERIC,
                    Windows_On_Demand_Cost NUMERIC,
                    Windows_Savings_Price_1_Year NUMERIC,
                    Windows_Savings_Price_3_Years NUMERIC,
                    Windows_Reservation_1_Year NUMERIC,
                    Windows_Reservation_3_Years  NUMERIC,
                    Windows_Spot_Cost NUMERIC)
                """)
            logging.info("Table Created: vm_pricing")
        except psycopg2.Error as e:
            logging.error(f"Error creating table vm_pricing:{e}")

def truncate_table_vm_pricing():
    """Truncate table azure_rates"""
    query = """TRUNCATE TABLE vm_pricing;"""
    try:
        pgcur.execute(query)
        logging.info("Truncated table : vm_pricing")
    except psycopg2.Error as e:
        logging.critical(f"Error in truncating vm_pricing table:{e}")

def insert_into_vm_pricing_join_operation():
    """Insert into the vm_pricing table"""

    query = """
            INSERT INTO vm_pricing 
            SELECT 
                vm.name AS Name,
                vm.locations AS Location,
                vm.memorygb AS Instance_Memory,
                vm.vcpus AS vCPUs,
                CAST(vm.gpus AS INTEGER) AS Instance_Storage,
                MAX(CASE 
                        WHEN (ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(rates.sku_name, '\\s+'), 1) = 1 OR
                            (rates.sku_name NOT LIKE '%Spot' AND ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(rates.sku_name, '\\s+'), 1) = 2))
                            AND rates.type = 'Consumption' 
                            AND (rates.product_name LIKE '%Series' OR rates.product_name LIKE '%Basic')
                        THEN rates.unit_price 
                    END) AS Linux_On_Demand_Cost,
                MAX(CASE 
                        WHEN rates.type = 'Consumption'
                            AND (rates.product_name LIKE '%Series' OR rates.product_name LIKE '%Basic')
                            AND rates.savings_plan_term_1y = '1 Year'
                        THEN rates.savings_plan_unit_price_1y
                    END) AS Linux_Savings_Price_1_Year,
                MAX(CASE 
                        WHEN rates.type = 'Consumption' 
                            AND (rates.product_name LIKE '%Series' OR rates.product_name LIKE '%Basic')
                            AND rates.savings_plan_term_3y= '3 Years'
                        THEN rates.savings_plan_unit_price_3y
                    END) AS Linux_Savings_Price_3_Years,
                MAX(CASE 
                        WHEN ((rates.reservation_term = '1 Year') 
                            AND rates.type = 'Reservation') 
                            AND (rates.product_name LIKE '%Series' OR rates.product_name LIKE '%Basic')
                        THEN rates.unit_price  / 8760
                    END) AS Linux_Reservation_1_Year,
                MAX(CASE 
                        WHEN ((rates.reservation_term = '3 Years') 
                            AND rates.type = 'Reservation') 
                            AND (rates.product_name LIKE '%Series' OR rates.product_name LIKE '%Basic')
                        THEN rates.unit_price / (3 * 8760)
                    END) AS Linux_Reservation_3_Years,
                MAX(CASE 
                        WHEN ((rates.sku_name LIKE '%Spot' OR ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(rates.sku_name, '\\s+'), 1) = 3) 
                            AND rates.type = 'Consumption') 
                            AND (rates.product_name LIKE '%Series' OR rates.product_name LIKE '%Basic')
                        THEN rates.unit_price 
                    END) AS Linux_Spot_Cost,
                MAX(CASE 
                        WHEN (ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(rates.sku_name, '\\s+'), 1) = 1 OR
                            (rates.sku_name NOT LIKE '%Spot' AND ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(rates.sku_name, '\\s+'), 1) = 2))
                            AND rates.type = 'Consumption' 
                            AND rates.product_name LIKE '%Windows' 
                        THEN rates.unit_price
                    END) AS Windows_On_Demand_Cost,
                MAX(CASE 
                        WHEN rates.type = 'Consumption' 
                            AND (rates.product_name LIKE '%Windows')
                            AND rates.savings_plan_term_1y = '1 Year'
                        THEN rates.savings_plan_unit_price_1y
                    END) AS Windows_Savings_Price_1_Year,
                MAX(CASE 
                        WHEN rates.type = 'Consumption' 
                            AND (rates.product_name LIKE '%Windows')
                            AND rates.savings_plan_term_3y ='3 Years'
                        THEN rates.savings_plan_unit_price_3y
                    END) AS Windows_Savings_Price_3_Years,
                MAX(CASE 
                        WHEN ((rates.reservation_term = '1 Year') 
                            AND rates.type = 'Reservation') 
                            AND (rates.product_name LIKE '%Windows')
                        THEN rates.unit_price / 8760
                    END) AS Windows_Reservation_1_Year,
                MAX(CASE 
                        WHEN ((rates.reservation_term = '3 Years') 
                            AND rates.type = 'Reservation') 
                            AND (rates.product_name LIKE '%Windows')
                        THEN rates.unit_price / (3 * 8760)
                    END) AS Windows_Reservation_3_Years,
                MAX(CASE 
                        WHEN ((rates.sku_name LIKE '%Spot' OR ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(rates.sku_name, '\\s+'), 1) = 3) 
                            AND rates.type = 'Consumption') 
                            AND rates.product_name LIKE '%Windows' 
                        THEN rates.unit_price
                    END) AS Windows_Spot_Cost
            FROM 
                azure_rates rates
            INNER JOIN 
                virtualmachines vm 
            ON 
                vm.name = rates.arm_sku_name 
                AND vm.locations = rates.arm_region_name
            GROUP BY 
                vm.name, vm.locations, vm.memorygb, vm.vcpus, vm.gpus
            ORDER BY 
                vm.locations ASC, vm.name ASC;
        """
    try:
        pgcur.execute(query)
        logging.info("Data inserted into vm_pricing table")
    except psycopg2.Error as e:
        logging.critical(f"Error in inserting data into vm_pricing table:{e}")

def create_table_vm_pricing_history():
    """Create vm_pricing_history table for storing historical data"""

    query = """
        CREATE TABLE IF NOT EXISTS vm_pricing_history(
                    Name VARCHAR,
                    Location VARCHAR,
                    Instance_memory NUMERIC,
                    vCPUs INT,
                    Instance_Storage NUMERIC,
                    Linux_On_Demand_Cost NUMERIC,
                    Linux_Savings_Price_1_Year NUMERIC,
                    Linux_Savings_Price_3_Years NUMERIC,
                    Linux_Reservation_1_Year NUMERIC,
                    Linux_Reservation_3_Years NUMERIC,
                    Linux_Spot_Cost NUMERIC,
                    Windows_On_Demand_Cost NUMERIC,
                    Windows_Savings_Price_1_Year NUMERIC,
                    Windows_Savings_Price_3_Years NUMERIC,
                    Windows_Reservation_1_Year NUMERIC,
                    Windows_Reservation_3_Years  NUMERIC,
                    Windows_Spot_Cost NUMERIC,
                    run_timestamp TIMESTAMP)
            """
    try:
        pgcur.execute(query)
        logging.info("Table Created: vm_pricing_history")
    except psycopg2.Error as e:
        logging.error(f"Error creating table vm_pricing_history:{e}")

def insert_into_vm_pricing_history():
    """Insert vm_pricing table data along eith timesatmp to store the historical data"""

    query = "Insert into vm_pricing_history select *,CURRENT_TIMESTAMP from vm_pricing ;"
    try:
        pgcur.execute(query)
        logging.info("Data inserted into vm_pricing_history table")
    except psycopg2.Error as e:
        logging.critical(f"Error in inserting data into vm_pricing_history table:{e}")


def close_db_connection():
    """ Close the cursor and connection of the database """
    try:
        pgcur.close()
        connection.close()
        logging.info("PG Database connection closed")
    except psycopg2.Error as e:
        logging.Error(f"Error in closing database connection:{e}")

    



def mainflow():
    #Connect to database
    connect_to_db() 
    
    #Process availability sets data
    create_table_availabilitysets()
    truncate_table_availabilitysets()
    a_data = fetch_data_availabilitysets()
    insert_into_availabilitysets(a_data)

    #Process snapshots data
    create_table_snapshots()
    truncate_table_snapshots()
    s_data = fetch_data_snapshots()
    insert_into_snapshots(s_data)
    

    #Process disks data
    create_table_disks()
    truncate_table_disks()
    d_data = fetch_data_disks()
    insert_into_disks(d_data)

    #Process hostGroups/hosts data
    create_table_hostgroups()
    truncate_table_hostgroups()
    h_data = fetch_data_hostgroups()
    insert_into_hostgroup(h_data)

    #Process virtual machines data
    create_table_vms()
    truncate_table_vms()
    v_data = fetch_data_vms()
    insert_into_vms(v_data)

    #Process azure rates
    create_table_azure_rates()
    truncate_table_azure_rates()
    """API url for the first page of Azure prices"""
    api_url = "https://prices.azure.com/api/retail/prices?api-version=2023-01-01-preview"

    while api_url:
        try:
            api_url,r_data = fetch_data_rates(api_url)
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching Azure VM rates : {e}")
            logging.info(f"Retrying in 5 seconds...")
            time.sleep(5)
            api_url,r_data = fetch_data_rates(api_url)


        insert_into_azure_rates(r_data)
    
    #Process the vm info and rates details
    create_table_vm_pricing()
    truncate_table_vm_pricing()
    insert_into_vm_pricing_join_operation()
    create_table_vm_pricing_history()
    insert_into_vm_pricing_history()



    #close database connection
    close_db_connection()


if __name__ == '__main__':
  stackql = StackQL()
  stackql_query = "REGISTRY PULL azure"
  result = stackql.executeStmt(stackql_query)

  subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]

  # Set UTC Timezone in Python
  utc_timezone = 'UTC'
  os.environ['TZ'] = utc_timezone
  time.tzset()

  # Set logging config
  LOGLEVEL = 'DEBUG'
  logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                encoding='utf-8',
                datefmt='%m/%d/%Y %I:%M:%S %p %Z',
                level=LOGLEVEL,
                force=True)
  schedule.every().day.at("00:00").do(mainflow)
  schedule.run_all()
  while True:
        schedule.run_pending()
        time.sleep(1)
