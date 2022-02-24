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

tickets_lines["numbers"] = tickets_lines["numbers"].str.replace(' ', '')
tickets_lines['numbers_list'] = tickets_lines["numbers"].apply(lambda x: x.strip('()').split(','))

cols = [
    "tickets_id",
    "drawing_id",
    "line_id",
    "bet_type",
    "numbers",
    "numbers_list",
    'matched_numbers',
    'matched_numbers_count'
]

df_matched_numbers = pd.DataFrame(columns = cols)
for i in range(tickets_lines.shape[0]):
    print("Processing: ")
    print(tickets_lines["numbers_list"][int(i)])
    matched_numbers = [X for X in WINNING_NUMBERS if(X in tickets_lines["numbers_list"][int(i)])]
    print("Matched numbers: ")
    print(matched_numbers)
    df_matched_numbers = df_matched_numbers.append(
        {
            "tickets_id": tickets_lines["tickets_id"][int(i)],
            "drawing_id": tickets_lines["drawing_id"][int(i)],
            "line_id": tickets_lines["line_id"][int(i)],
            "bet_type": tickets_lines["bet_type"][int(i)],
            "numbers": tickets_lines["numbers"][int(i)],
            "numbers_list": tickets_lines["numbers_list"][int(i)],
            'matched_numbers': matched_numbers,
            'matched_numbers_count': len(matched_numbers)
        },
        ignore_index=True
    )

### no prize
df_matched_numbers['prize'] = 0

### NORMAL - Tier 1 (match 5 numbers) = 90000 EUR
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["NORMAL"])) & (df_matched_numbers['matched_numbers_count'].isin([5]))), 'prize'
] = 1 * 90000 + 0 * 200 + 0 * 5

### NORMAL - Tier 2 (match 4 numbers) = 200 EUR
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["NORMAL"])) & (df_matched_numbers['matched_numbers_count'].isin([4]))), 'prize'
] = 0 * 90000 + 1 * 200 + 0 * 5

### NORMAL - Tier 3 (match 3 numbers) = 5 EUR
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["NORMAL"])) & (df_matched_numbers['matched_numbers_count'].isin([3]))), 'prize'
] = 0 * 90000 + 0 * 200 + 1 * 5

### S0600 - Tier 1 (match 5 numbers) = 90000 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S0600"])) & (df_matched_numbers['matched_numbers_count'].isin([5]))), 'prize'
] = 1 * 90000 + 5 * 200 + 0 * 5

### S0600 - Tier 2 (match 4 numbers) = 200 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S0600"])) & (df_matched_numbers['matched_numbers_count'].isin([4]))), 'prize'
] = 0 * 90000 + 2 * 200 + 4 * 5

### S0600 - Tier 3 (match 3 numbers) = 5 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S0600"])) & (df_matched_numbers['matched_numbers_count'].isin([3]))), 'prize'
] = 0 * 90000 + 0 * 200 + 3 * 5

### S0700 - Tier 1 (match 5 numbers) = 90000 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S0700"])) & (df_matched_numbers['matched_numbers_count'].isin([5]))), 'prize'
] = 1 * 90000 + 10 * 200 + 10 * 5

### S0700 - Tier 2 (match 4 numbers) = 200 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S0700"])) & (df_matched_numbers['matched_numbers_count'].isin([4]))), 'prize'
] = 0 * 90000 + 3 * 200 + 12 * 5

### S0700 - Tier 3 (match 3 numbers) = 5 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S0700"])) & (df_matched_numbers['matched_numbers_count'].isin([3]))), 'prize'
] = 0 * 90000 + 0 * 200 + 6 * 5

### S0800 - Tier 1 (match 5 numbers) = 90000 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S0800"])) & (df_matched_numbers['matched_numbers_count'].isin([5]))), 'prize'
] = 1 * 90000 + 15 * 200 + 30 * 5

### S0800 - Tier 2 (match 4 numbers) = 200 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S0800"])) & (df_matched_numbers['matched_numbers_count'].isin([4]))), 'prize'
] = 0 * 90000 + 4 * 200 + 24 * 5

### S0800 - Tier 3 (match 3 numbers) = 5 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S0800"])) & (df_matched_numbers['matched_numbers_count'].isin([3]))), 'prize'
] = 0 * 90000 + 0 * 200 + 10 * 5

### S0900 - Tier 1 (match 5 numbers) = 90000 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S0900"])) & (df_matched_numbers['matched_numbers_count'].isin([5]))), 'prize'
] = 1 * 90000 + 20 * 200 + 60 * 5

### S0900 - Tier 2 (match 4 numbers) = 200 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S0900"])) & (df_matched_numbers['matched_numbers_count'].isin([4]))), 'prize'
] = 0 * 90000 + 5 * 200 + 40 * 5

### S0900 - Tier 3 (match 3 numbers) = 5 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S0900"])) & (df_matched_numbers['matched_numbers_count'].isin([3]))), 'prize'
] = 0 * 90000 + 0 * 200 + 15 * 5

### S1000 - Tier 1 (match 5 numbers) = 90000 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S1000"])) & (df_matched_numbers['matched_numbers_count'].isin([5]))), 'prize'
] = 1 * 90000 + 25 * 200 + 100 * 5

### S1000 - Tier 2 (match 4 numbers) = 200 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S1000"])) & (df_matched_numbers['matched_numbers_count'].isin([4]))), 'prize'
] = 0 * 90000 + 6 * 200 + 60 * 5

### S1000 - Tier 3 (match 3 numbers) = 5 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S1000"])) & (df_matched_numbers['matched_numbers_count'].isin([3]))), 'prize'
] = 0 * 90000 + 0 * 200 + 21 * 5

### S1100 - Tier 1 (match 5 numbers) = 90000 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S1100"])) & (df_matched_numbers['matched_numbers_count'].isin([5]))), 'prize'
] = 1 * 90000 + 30 * 200 + 150 * 5

### S1100 - Tier 2 (match 4 numbers) = 200 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S1100"])) & (df_matched_numbers['matched_numbers_count'].isin([4]))), 'prize'
] = 0 * 90000 + 7 * 200 + 84 * 5

### S1100 - Tier 3 (match 3 numbers) = 5 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S1100"])) & (df_matched_numbers['matched_numbers_count'].isin([3]))), 'prize'
] = 0 * 90000 + 0 * 200 + 28 * 5

### S1200 - Tier 1 (match 5 numbers) = 90000 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S1200"])) & (df_matched_numbers['matched_numbers_count'].isin([5]))), 'prize'
] = 1 * 90000 + 35 * 200 + 210 * 5

### S1200 - Tier 2 (match 4 numbers) = 200 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S1200"])) & (df_matched_numbers['matched_numbers_count'].isin([4]))), 'prize'
] = 0 * 90000 + 8 * 200 + 112 * 5

### S1200 - Tier 3 (match 3 numbers) = 5 EUR and more
df_matched_numbers.loc[
    ((df_matched_numbers['bet_type'].isin(["S1200"])) & (df_matched_numbers['matched_numbers_count'].isin([3]))), 'prize'
] = 0 * 90000 + 0 * 200 + 36 * 5

prizes = df_matched_numbers[["tickets_id", "drawing_id", "line_id", "bet_type", "numbers", "matched_numbers_count", "prize"]]

query_01 = """
    WITH
    
        v_prizes AS (
            SELECT
                tickets_id,
                drawing_id,
                line_id,
                bet_type,
                numbers,
                matched_numbers_count as Tier,
                prize
    
            FROM prizes
            ),
    
        v_tickets AS (
            SELECT DISTINCT
                tickets_id,
                fraction
    
            FROM tickets
            )
    
    
    SELECT
        vp.*,
        vt.fraction,
        vt.fraction * vp.prize AS prize_x_fraction
    
    
    FROM v_prizes as vp
    LEFT JOIN v_tickets as vt
    
    ON
    vp.tickets_id = vt.tickets_id
    
    ORDER BY
    prize DESC
"""

sql_01 = ps.sqldf(query_01, locals())

print("Query processed")

with pd.ExcelWriter(OUTPUTS + '\\python_and_sql_solution.xlsx') as writer:
    sql_01.to_excel(writer, sheet_name='python_and_sql_solution')

print("Output saved")
