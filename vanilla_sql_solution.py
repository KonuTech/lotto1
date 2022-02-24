import os
import json
import pandas as pd
import pandasql as ps

CURRENT_WORKING_DIRECTORY = os.getcwd()
INPUTS = CURRENT_WORKING_DIRECTORY + "\\inputs"
OUTPUTS = CURRENT_WORKING_DIRECTORY + "\\outputs"

CONFIG_FILE = "config.json"
with open(CURRENT_WORKING_DIRECTORY + "\\" + CONFIG_FILE, encoding='utf-8') as f:
    CONFIG = json.load(f)

TICKETS = CONFIG["TICKETS"]["FILE_NAME"]
TICKETS_DATES = CONFIG["TICKETS"]["DATES"]
TICKETS_SEPARATOR = CONFIG["TICKETS"]["SEPARATOR"]
TICKETS_DTYPE = CONFIG["TICKETS"]["DTYPE"]

TICKETS_LINES = CONFIG["TICKETS_LINES"]["FILE_NAME"]
TICKETS_LINES_DATES = CONFIG["TICKETS_LINES"]["DATES"]
TICKETS_LINES_SEPARATOR = CONFIG["TICKETS_LINES"]["SEPARATOR"]
TICKETS_LINES_DTYPE = CONFIG["TICKETS_LINES"]["DTYPE"]

WINNING_NUMBERS = CONFIG["WINNING_NUMBERS"]

tickets = pd.read_excel(
    INPUTS + "\\" + TICKETS,
    sheet_name="Sheet1",
    header=0,
    names=None,
    index_col=None,
    usecols=None,
    squeeze=None,
    dtype=None,
    engine="openpyxl",
    converters=None,
    true_values=None,
    false_values=None,
    skiprows=None,
    nrows=None,
    na_values=None,
    keep_default_na=True,
    na_filter=True,
    verbose=True,
    parse_dates=False,
    date_parser=None,
    thousands=None,
    #   decimal='.',
    comment=None,
    skipfooter=0,
    convert_float=None,
    mangle_dupe_cols=True,
    storage_options=None
)

tickets_lines = pd.read_excel(
    INPUTS + "\\" + TICKETS_LINES,
    sheet_name="Sheet1",
    header=0,
    names=None,
    index_col=None,
    usecols=None,
    squeeze=None,
    dtype=None,
    engine="openpyxl",
    converters=None,
    true_values=None,
    false_values=None,
    skiprows=None,
    nrows=None,
    na_values=None,
    keep_default_na=True,
    na_filter=True,
    verbose=True,
    parse_dates=False,
    date_parser=None,
    thousands=None,
    #   decimal='.',
    comment=None,
    skipfooter=0,
    convert_float=None,
    mangle_dupe_cols=True,
    storage_options=None
)

query_01 = """
    WITH
    
        v_tickets_lines AS (
            SELECT
                tickets_id,
                drawing_id,
                line_id,
                bet_type,
                numbers,
                
                MAX(CASE WHEN "numbers" LIKE ("%11%") THEN 1 ELSE 0 END) AS "matched_numbers_11",
                MAX(CASE WHEN "numbers" LIKE ("%19%") THEN 1 ELSE 0 END) AS "matched_numbers_19",
                MAX(CASE WHEN "numbers" LIKE ("%21%") THEN 1 ELSE 0 END) AS "matched_numbers_21",
                MAX(CASE WHEN "numbers" LIKE ("%33%") THEN 1 ELSE 0 END) AS "matched_numbers_33",
                MAX(CASE WHEN "numbers" LIKE ("%42%") THEN 1 ELSE 0 END) AS "matched_numbers_42",
    
                MAX(CASE WHEN "numbers" LIKE ("%11%") THEN 1 ELSE 0 END) +
                MAX(CASE WHEN "numbers" LIKE ("%19%") THEN 1 ELSE 0 END) +
                MAX(CASE WHEN "numbers" LIKE ("%21%") THEN 1 ELSE 0 END) +
                MAX(CASE WHEN "numbers" LIKE ("%33%") THEN 1 ELSE 0 END) +
                MAX(CASE WHEN "numbers" LIKE ("%42%") THEN 1 ELSE 0 END)
                AS matched_numbers
    
            FROM tickets_lines
            
            GROUP BY
                tickets_id,
                drawing_id,
                line_id,
                bet_type,
                numbers
            ),
            
         v_tickets AS (
            SELECT DISTINCT
                tickets_id,
                fraction
    
            FROM tickets
            )
            
    SELECT
    v_prizes.*,
    vt.fraction,
    vt.fraction * v_prizes.prize AS prize_x_fraction
    
    FROM (
    
            SELECT
                tickets_id,
                drawing_id,
                line_id,
                bet_type,
                numbers,
                matched_numbers AS Tier,
    
                --no prize
                CASE
                    WHEN bet_type="NORMAL" AND matched_numbers < 3 THEN (0 * 90000 + 0 * 200 + 0 * 5)
    
                --NORMAL - Tier 1 (match 5 numbers) = 90000 EUR
                    WHEN bet_type="NORMAL" AND matched_numbers=5 THEN (0 * 90000 + 1 * 200 + 0 * 5)
    
                --NORMAL - Tier 2 (match 4 numbers) = 200 EUR
                    WHEN bet_type="NORMAL" AND matched_numbers=4 THEN (0 * 90000 + 1 * 200 + 0 * 5)
    
                --NORMAL - Tier 3 (match 3 numbers) = 5 EUR
                    WHEN bet_type="NORMAL" AND matched_numbers=3 THEN (0 * 90000 + 0 * 200 + 1 * 5)
    
                --S0600 - Tier 1 (match 5 numbers) = 90000 EUR and more
                    WHEN bet_type="S0600" AND matched_numbers=5 THEN (1 * 90000 + 5 * 200 + 0 * 5)
    
                --S0600 - Tier 2 (match 4 numbers) = 200 EUR and more
                    WHEN bet_type="S0600" AND matched_numbers=4 THEN (0 * 90000 + 2 * 200 + 4 * 5)
    
                --S0600 - Tier 3 (match 3 numbers) = 5 EUR and more
                    WHEN bet_type="S0600" AND matched_numbers=3 THEN (0 * 90000 + 0 * 200 + 3 * 5)
    
                --S0700 - Tier 1 (match 5 numbers) = 90000 EUR and more
                    WHEN bet_type="S0700" AND matched_numbers=5 THEN (1 * 90000 + 10 * 200 + 10 * 5)
    
                --S0700 - Tier 2 (match 4 numbers) = 200 EUR and more
                    WHEN bet_type="S0700" AND matched_numbers=4 THEN (0 * 90000 + 3 * 200 + 12 * 5)
    
                --S0700 - Tier 3 (match 3 numbers) = 5 EUR and more
                    WHEN bet_type="S0700" AND matched_numbers=3 THEN (0 * 90000 + 0 * 200 + 6 * 5)
    
                --S0800 - Tier 1 (match 5 numbers) = 90000 EUR and more
                    WHEN bet_type="S0800" AND matched_numbers=5 THEN (1 * 90000 + 15 * 200 + 30 * 5)
    
                --S0800 - Tier 2 (match 4 numbers) = 200 EUR and more
                    WHEN bet_type="S0800" AND matched_numbers=4 THEN (0 * 90000 + 4 * 200 + 24 * 5)
    
                --S0800 - Tier 3 (match 3 numbers) = 5 EUR and more
                    WHEN bet_type="S0800" AND matched_numbers=3 THEN (0 * 90000 + 0 * 200 + 10 * 5)
    
                --S0900 - Tier 1 (match 5 numbers) = 90000 EUR and more
                    WHEN bet_type="S0900" AND matched_numbers=5 THEN (1 * 90000 + 20 * 200 + 60 * 5)
    
                --S0900 - Tier 2 (match 4 numbers) = 200 EUR and more
                    WHEN bet_type="S0900" AND matched_numbers=4 THEN (0 * 90000 + 5 * 200 + 40 * 5)
    
                --S0900 - Tier 3 (match 3 numbers) = 5 EUR and more
                    WHEN bet_type="S0900" AND matched_numbers=3 THEN (0 * 90000 + 0 * 200 + 15 * 5)
    
                --S1000 - Tier 1 (match 5 numbers) = 90000 EUR and more
                    WHEN bet_type="S1000" AND matched_numbers=5 THEN (1 * 90000 + 25 * 200 + 100 * 5)
    
                --S1000 - Tier 2 (match 4 numbers) = 200 EUR and more
                    WHEN bet_type="S1000" AND matched_numbers=4 THEN (0 * 90000 + 6 * 200 + 60 * 5)
    
                --S1000 - Tier 3 (match 3 numbers) = 5 EUR and more
                    WHEN bet_type="S1000" AND matched_numbers=3 THEN (0 * 90000 + 0 * 200 + 21 * 5)
    
                --S1100 - Tier 1 (match 5 numbers) = 90000 EUR and more
                    WHEN bet_type="S1100" AND matched_numbers=5 THEN (1 * 90000 + 30 * 200 + 150 * 5)
    
                --S1100 - Tier 2 (match 4 numbers) = 200 EUR and more
                    WHEN bet_type="S1100" AND matched_numbers=4 THEN (0 * 90000 + 7 * 200 + 84 * 5)
    
                --S1100 - Tier 3 (match 3 numbers) = 5 EUR and more
                    WHEN bet_type="S1100" AND matched_numbers=3 THEN (0 * 90000 + 0 * 200 + 28 * 5)
    
                --S1200 - Tier 1 (match 5 numbers) = 90000 EUR and more
                    WHEN bet_type="S1200" AND matched_numbers=5 THEN (1 * 90000 + 35 * 200 + 210 * 5)
    
                --S1200 - Tier 2 (match 4 numbers) = 200 EUR and more
                    WHEN bet_type="S1200" AND matched_numbers=4 THEN (0 * 90000 + 8 * 200 + 112 * 5)
    
                --S1200 - Tier 3 (match 3 numbers) = 5 EUR and more
                    WHEN bet_type="S1200" AND matched_numbers=3 THEN (0 * 90000 + 0 * 200 + 36 * 5)
    
                    ELSE 0
    
                END AS prize
    
    
            FROM v_tickets_lines as vtl
    
            GROUP BY
                tickets_id,
                drawing_id,
                line_id,
                bet_type,
                numbers,
                matched_numbers
    
        ) AS v_prizes
    
    LEFT JOIN v_tickets as vt
    
    ON
    v_prizes.tickets_id = vt.tickets_id
        
    ORDER BY
    prize DESC
"""

sql_01 = ps.sqldf(query_01, locals())

print("Query processed")

with pd.ExcelWriter(OUTPUTS + '\\vanilla_sql_solution.xlsx') as writer:
    sql_01.to_excel(writer, sheet_name='vanilla_sql_solution')

print("Output saved")
