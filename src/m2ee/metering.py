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


def metering_export_usage_metrics(config):
    logger.info("Begin exporting usage metrics")
    logger.info(metering_guess_email_columns(config))
    logger.info("End exporting usage metrics")
