#
# Copyright (C) 2021 Mendix. All rights reserved.
#
import datetime
import hashlib
import json
import logging
import os
import subprocess
import time

from m2ee.client import M2EEAdminNotAvailable

logger = logging.getLogger(__name__)
usage_metrics_schema_version = "1.1"
output_json_file = "mendix_usage_metrics.json"


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
        user_specialization_tables = metering_get_user_specialization_tables(config)
        if len(user_specialization_tables) > 0:
            query = "SELECT table_name,column_name FROM information_schema.columns WHERE table_name in (" \
                    + user_specialization_tables + ") and column_name like '%%mail%%'"
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
        query = "SELECT u.name, u.lastlogin, u.webserviceuser, u.blocked, u.active, u.isanonymous as is_anonymous, " \
                "ur.usertype, "
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
            projection += ") as email"
            query += projection
        else:
            # remove the trailing , from the query
            query = query [:-2]
        query += " FROM system$user u LEFT JOIN system$userreportinfo_user ur_u on u.id = ur_u.system$userid LEFT JOIN " \
                 "system$userreportinfo ur on ur.id = ur_u.system$userreportinfoid "
        # append the JOIN to the query
        if table_email_column:
            for join in joins:
                query += join
        logger.debug("Constructed query: <" + query + ">")
        output = metering_run_pg_query(config, query)
        logger.debug("Output from the query:\n" + output)
        return output
    except Exception as e:
        logger.error(e)


def metering_encrypt(name):
    salt = [53, 14, 215, 17, 147, 90, 22, 81, 48, 249, 140, 146, 201, 247, 182, 18, 218, 242, 114, 5, 255, 202, 227,
            242, 126, 235, 162, 38, 52, 150, 95, 193]
    salt_byte_array = bytes(salt)
    encoded_name = name.encode()
    byte_array = bytearray(encoded_name)
    h = hashlib.sha256()
    h.update(salt_byte_array)
    h.update(byte_array)
    return h.hexdigest()


def metering_extract_and_hash_domain_from_email(email):
    if not isinstance(email, str):
        return ""
    if email == "":
        return ""
    domain = ""
    if email.find("@") != -1:
        domain = str(email).split("@")[1]
    if len(domain) >= 2:
        return metering_encrypt(domain)
    else:
        return ""


def metering_massage_and_encrypt_data(object_dict):
    email_name_processed = False
    for col_name, value in object_dict.items():
        if col_name == "active":
            object_dict[col_name] = "true" if value == "t" else "false"
        if col_name == "blocked":
            object_dict[col_name] = "true" if value == "t" else "false"
        if col_name == "email" or col_name == "name":
            if not email_name_processed:
                name = object_dict["name"]
                email = object_dict["email"]
                # prefer email in name over email field
                hashed_email_domain = metering_extract_and_hash_domain_from_email(name)
                if hashed_email_domain == "":
                    hashed_email_domain = metering_extract_and_hash_domain_from_email(email)
                object_dict["email_domain"] = hashed_email_domain
                del object_dict["email"]
                object_dict["name"] = metering_encrypt("name")
                email_name_processed = True
        # isAnonymous needs to be kept empty if empty
        if col_name == "is_anonymous":
            if value == "":
                object_dict[col_name] = ""
            else:
                object_dict[col_name] = "true" if value == "t" else "false"
        if col_name == "lastlogin":
            # convert to epoch
            if not value == "":
                object_dict[col_name] = (int(time.mktime(datetime.datetime.strptime(
                    value, "%Y-%m-%d %H:%M:%S.%f").timetuple())))
        if col_name == "webserviceuser":
            object_dict[col_name] = "true" if value == "t" else "false"


def get_server_id(client):
    try:
        return client.get_license_information()["license_id"]
    except M2EEAdminNotAvailable as e:
        raise Exception("The application process is not running.")


def metering_get_usage_metrics_json(m2ee):
    try:
        query_output = metering_query_usage(m2ee.config)
        usage_metrics_list = []
        fields = []
        user_usage_metrics_dict = {}
        for row_num, row in enumerate(query_output.split('\n')):
            if row_num == 0:
                for field in row.split('|'):
                    fields.append(field.strip())
            else:
                # ignore the empty rows and the one that prints no of rows
                if len(row.split('|')) == 8:
                    for col_num, value in enumerate(row.split('|')):
                        user_usage_metrics_dict[fields[col_num]] = value.strip()
                    timestamp = datetime.datetime.now()
                    user_usage_metrics_dict["created_at"] = str(timestamp)
                    user_usage_metrics_dict["schema_version"] = usage_metrics_schema_version
                    user_usage_metrics_dict["server_id"] = get_server_id(m2ee.client)
                    metering_massage_and_encrypt_data(user_usage_metrics_dict)
                    usage_metrics_list.append(user_usage_metrics_dict.copy())
                    user_usage_metrics_dict.clear()
        json_dictionary = json.dumps(usage_metrics_list, indent=4, sort_keys=True)
        logger.debug("BEGIN Usage JSON")
        logger.debug(json_dictionary)
        logger.debug("END Usage JSON")
        return json_dictionary
    except Exception as e:
        logger.error(e)


def metering_export_usage_metrics(m2ee):
    try:
        logger.info("Begin exporting usage metrics")
        usage_metrics_json = metering_get_usage_metrics_json(m2ee)
        if not usage_metrics_json is None:
            with open(output_json_file, "w") as outfile:
                outfile.write(usage_metrics_json)
            logger.info("Usage metrics exported to " + output_json_file)
    except Exception as e:
        logger.error(e)
