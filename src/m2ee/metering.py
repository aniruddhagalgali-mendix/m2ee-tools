#
# Copyright (C) 2021 Mendix. All rights reserved.
#
import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def metering_run_pg_query(config, query):
    try:
        env = os.environ.copy()
        env.update(config.get_pg_environment())
        cmd = (
            config.get_psql_binary(), "-c",
            query
        )
        logger.debug("Running command \n" + str(cmd))
        output = subprocess.check_output(cmd, env=env)
        logger.debug("Output from command \n" + output)
        return output
    except Exception as e:
        logger.error(e)


def metering_get_user_specialization_tables(config):
    try:
        output = metering_run_pg_query(config, "SELECT DISTINCT submetaobjectname from system$user")
        user_specialization_tables = str()
        for num, row in enumerate(output.split('\n')):
            # ignore the first two rows which print the table name and ----
            if num > 1:
                # ignore the System.User table and the last row which prints no of rows
                if row.find("System.User") == -1 & row.find("rows") == -1:
                    row = row.strip().lower().replace('.', '$')
                    # ignore empty rows
                    if len(row) > 0:
                        row = "'" + row + "'"
                        user_specialization_tables += row + ","

        # remove the last ,
        user_specialization_tables = user_specialization_tables[:-1]
        logger.debug("User specialization tables are: <" + user_specialization_tables + ">")
        return user_specialization_tables
    except Exception as e:
        logger.error(e)


def metering_guess_email_columns(config):
    try:
        table_email_column = dict()
        query = "SELECT table_name,column_name FROM information_schema.columns WHERE table_name in (" \
                + metering_get_user_specialization_tables(config) + ") and column_name like '%%mail%%'"
        output = metering_run_pg_query(config, query)
        for num, row in enumerate(output.split('\n')):
            # ignore the first two rows which print the table name and ----
            if num > 1:
                if row.find("rows") == -1:
                    row = row.strip().lower()
                    if row != "":
                        table_email_column[row.split('|')[0].strip()] = row.split('|')[1].strip()
        logger.debug("Probable tables and columns that may have an email address are:")
        logger.debug(table_email_column)
        return table_email_column
    except Exception as e:
        logger.error(e)


def metering_query_usage(config):
    try:
        # the base query
        query = "SELECT u.name, u.lastlogin, u.webserviceuser, u.blocked, u.active, u.isanonymous, ur.usertype, "
        # check for email address
        table_email_column = metering_guess_email_columns(config)
        if table_email_column:
            projection = "CONCAT("
            joins = list()
            # iterate over the table_email_column to form the CONCAT and JOIN part of the query
            for i, (k, v) in enumerate(table_email_column.items()):
                projection += "mailfield_" + str(i) + "." + v + ","
                joins.append("LEFT JOIN " + k + " mailfield_" + str(i) + " on mailfield_" + str(i) + ".id = u.id ")
            # remove the last ,
            projection = projection[:-1]
            projection += ") as email "
            query += projection
        query += "FROM system$user u LEFT JOIN system$userreportinfo_user ur_u on u.id = ur_u.system$userid LEFT JOIN " \
                 "system$userreportinfo ur on ur.id = ur_u.system$userreportinfoid "
        # append the JOIN to the query
        if table_email_column:
            for join in joins:
                query += join
        logger.debug("Constructed query: <" + query + ">")
        output = metering_run_pg_query(config,query)
        logger.debug("Output from the query:\n" + output)
        return output
    except Exception as e:
        logger.error(e)


def metering_export_usage_metrics(config):
    logger.info("Begin exporting usage metrics")
    logger.info(metering_query_usage(config))
    logger.info("End exporting usage metrics")
