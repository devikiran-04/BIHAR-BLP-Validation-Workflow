import psycopg2
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# =========================================================
# PostgreSQL Connection
# =========================================================
DB_CONFIG = {
    'host': 'wfms-psql-db1.crooayy8e4dr.ap-south-1.rds.amazonaws.com',
    'port': '5432',
    'database': 'nbpdcl_db',
    'user': 'admin_swamy',
    'password': 'SwM@!2025#RtXy9'
}

# =========================================================
# Create DB Connection
# =========================================================
def get_connection():

    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )

# =========================================================
# Truncate Tables
# =========================================================
def truncate_tables(day_num):

    conn = None

    try:

        conn = get_connection()

        conn.autocommit = False

        cur = conn.cursor()

        tables = [
            f"bfd.day{day_num}_hes_blp",
            f"bfd.day{day_num}_mdm_blp",
            f"bfd.blp_day{day_num}"
        ]

        for tbl in tables:

            print(f"Truncating : {tbl}")

            cur.execute(f"TRUNCATE TABLE {tbl};")

        conn.commit()

        print(f"Completed Truncate for DAY {day_num}")

        cur.close()

    except Exception as e:

        if conn:
            conn.rollback()

        print(f"ERROR truncate_tables DAY {day_num} : {e}")

        raise

    finally:

        if conn:
            conn.close()

# =========================================================
# Insert HES Data
# =========================================================
def insert_hes(current_date):

    conn = None

    try:

        day_num = current_date.day

        table_name = f"bfd.day{day_num}_hes_blp"

        start_ts = current_date.strftime('%Y-%m-%d') + " 00:00:00"
        end_ts = (current_date + timedelta(days=1)).strftime('%Y-%m-%d') + " 00:00:00"

        sql = f"""
        INSERT INTO {table_name}

        SELECT
            mtr_number,
            COUNT(1) AS hes_cnt

        FROM
        (
            SELECT DISTINCT
                meter_number AS mtr_number,
                meter_time

            FROM fep.fep_csv_lp

            WHERE meter_time >= '{start_ts}'
              AND meter_time <  '{end_ts}'

              AND status = 'Success'
              AND status_message = 'Successfully Completed'

              AND meter_number IN
              (
                  SELECT mtr_number
                  FROM cdb.meter_master
                  WHERE record_status = 1
                    AND mtr_installed_date IS NOT NULL
                    AND mtr_entry_type = '7547484b-28cb-4f5e-92d0-729c18c540b9'
              )

        ) t

        GROUP BY mtr_number;
        """

        conn = get_connection()

        conn.autocommit = False

        cur = conn.cursor()

        print(f"Started HES Insert : {table_name}")

        cur.execute(sql)

        inserted_rows = cur.rowcount

        conn.commit()

        print(f"Completed HES Insert : {table_name} | Rows : {inserted_rows}")

        cur.close()

    except Exception as e:

        if conn:
            conn.rollback()

        print(f"ERROR insert_hes {current_date.strftime('%Y-%m-%d')} : {e}")

        raise

    finally:

        if conn:
            conn.close()

# =========================================================
# Insert MDM Data
# =========================================================
def insert_mdm(current_date):

    conn = None

    try:

        day_num = current_date.day

        table_name = f"bfd.day{day_num}_mdm_blp"

        lp_date = current_date.strftime('%Y%m%d')

        sql = f"""
        INSERT INTO {table_name}

        SELECT
            mtr_number,
            COUNT(1) AS blp_cnt

        FROM
        (
            SELECT DISTINCT
                mtr_number,
                lp_time

            FROM mdms.mdm_loadprofile_data

            WHERE lp_date = '{lp_date}'

              AND mtr_number IN
              (
                  SELECT mtr_number
                  FROM cdb.meter_master
                  WHERE record_status = 1
                    AND mtr_installed_date IS NOT NULL
                    AND mtr_entry_type = '7547484b-28cb-4f5e-92d0-729c18c540b9'
              )

        ) t

        GROUP BY mtr_number;
        """

        conn = get_connection()

        conn.autocommit = False

        cur = conn.cursor()

        print(f"Started MDM Insert : {table_name}")

        cur.execute(sql)

        inserted_rows = cur.rowcount

        conn.commit()

        print(f"Completed MDM Insert : {table_name} | Rows : {inserted_rows}")

        cur.close()

    except Exception as e:

        if conn:
            conn.rollback()

        print(f"ERROR insert_mdm {current_date.strftime('%Y-%m-%d')} : {e}")

        raise

    finally:

        if conn:
            conn.close()

# =========================================================
# Insert BLP Comparison
# =========================================================
def insert_blp(current_date):

    conn = None

    try:

        day_num = current_date.day

        target_table = f"bfd.blp_day{day_num}"

        hes_table = f"bfd.day{day_num}_hes_blp"

        mdm_table = f"bfd.day{day_num}_mdm_blp"

        sql = f"""
        INSERT INTO {target_table}

        SELECT
            mtr_number,
            ROW_NUMBER() OVER(ORDER BY mtr_number) AS rn

        FROM
        (
            SELECT mtr_number

            FROM {hes_table}

            WHERE mtr_number IN
            (
                SELECT mtr_number

                FROM {mdm_table}

                WHERE blp_cnt < 48
                   OR blp_cnt BETWEEN 49 AND 95
            )

        ) t;
        """

        conn = get_connection()

        conn.autocommit = False

        cur = conn.cursor()

        print(f"Started BLP Insert : {target_table}")

        cur.execute(sql)

        inserted_rows = cur.rowcount

        conn.commit()

        print(f"Completed BLP Insert : {target_table} | Rows : {inserted_rows}")

        cur.close()

    except Exception as e:

        if conn:
            conn.rollback()

        print(f"ERROR insert_blp {current_date.strftime('%Y-%m-%d')} : {e}")

        raise

    finally:

        if conn:
            conn.close()

# =========================================================
# Process Single Date
# =========================================================
def process_date(current_date):

    try:

        print("\n================================================")
        print(f"PROCESSING DATE : {current_date.strftime('%Y-%m-%d')}")
        print("================================================")

        day_num = current_date.day

        # STEP 1
        truncate_tables(day_num)

        # STEP 2 (PARALLEL)
        with ThreadPoolExecutor(max_workers=2) as executor:

            futures = [
                executor.submit(insert_hes, current_date),
                executor.submit(insert_mdm, current_date)
            ]

            # IMPORTANT
            for future in futures:
                future.result()

        # STEP 3
        insert_blp(current_date)

        print(f"SUCCESS : {current_date.strftime('%Y-%m-%d')}")

    except Exception as e:

        print(f"FAILED DATE : {current_date.strftime('%Y-%m-%d')} | ERROR : {e}")

# =========================================================
# Main Execution
# =========================================================
if __name__ == "__main__":

    START_DATE = "20260517"
    END_DATE   = "20260524"

    MAX_PARALLEL_DATES = 3

    start_date = datetime.strptime(START_DATE, "%Y%m%d")

    end_date = datetime.strptime(END_DATE, "%Y%m%d")

    dates = []

    current_date = start_date

    while current_date <= end_date:

        dates.append(current_date)

        current_date += timedelta(days=1)

    print("\nSTARTING WORKFLOW...\n")

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_DATES) as executor:

        # IMPORTANT FIX
        list(executor.map(process_date, dates))

    print("\n====================================")
    print("ALL WORKFLOWS COMPLETED")
    print("====================================")