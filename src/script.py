# This script computes KPIs from a file with data of taxi trips and saves the values in a JSON file. The computed KPIs are:
#       - The average price per mile traveled by the customers of taxis
#       - The distribution of payment types
#       - The following custom indicator: (amount of tip + extra payment) / trip distance
#  After that, it writes in a log that the KPIs for this data have been computed.
#  
# Arguments:
#       - 1st argument: path to the folder where the file with data of taxi trips is located.
#       - 2nd argument: path to the folder where the JSON output file will be saved.
#       - 3rd argument (optional): date of the taxi trips data that will be computed. It must be in YYY-MM-DD format. If not given, the current date is taken.


# Imported libraries 
from datetime import datetime
import json
import sys
import os
import re
import math
import pyarrow.parquet as pq

# If less than 2 arguments are given, an error is returned
if len(sys.argv) < 3 :
    print('At least 2 arguments are needed')
    exit()

# Store the paths to the input and output folders
input_folder = sys.argv[1]
output_folder = sys.argv[2]

# If no date is given, the current date is stored
if len(sys.argv) == 3 :
    date = datetime.now().strftime('%Y-%m-%d')
# If the given date fulfills the YYYY-MM-DD format, it is stored
elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", sys.argv[3]):
    date = sys.argv[3]
# If the given date does not fulfill the YYYY-MM-DD format, an error is returned
else :
    print('Error in the date: must be in YYYY-MM-DD format')
    exit()


# Read the file that corresponds to the date from the input folder
trips = pq.read_table(os.path.join(input_folder, 'yellow_tripdata_' + date + '.parquet'))
trips = trips.to_pandas()

# List of measures
price_per_mile_sum = 0    # Sum of prices per mile
counter_PPM = 0           # Number of trips to compute the average price per mile
credit_card_freq = 0      # Number of trips payed with credit card
cash_freq = 0             # Number of trips payed with cash
no_charge_freq = 0        # Number of trips that were not charged
dispute_freq = 0          # Number of disputed trips
unknown_freq = 0          # Number of trips with unknown payment  
voided_trip_freq = 0      # Number of voided trips
custom_indicator_sum = 0  # Sum of custom indicators
counter_CI = 0            # Number of trips to compute the average custom indicator

# Note: 2 distinct counters are used, one for the prices per mile and one for the custom indicator , since some trip records have fields with a value of 0 or null

# Iterate over the rows of the pandas object trips
for row in trips.itertuples():
    
    # If the fare amount is not null and the trip distance is greater than 0, add up the price per mile
    if (not math.isnan(row.fare_amount)) & (row.trip_distance > 0):
        price_per_mile_sum += row.fare_amount / row.trip_distance
        counter_PPM += 1

    # Add 1 to the correspondant payment type counter
    if row.payment_type == 1:
        credit_card_freq += 1
    if row.payment_type == 2:
        cash_freq += 1
    if row.payment_type == 3:
        no_charge_freq += 1
    if row.payment_type == 4:
        dispute_freq += 1
    if row.payment_type == 5:
        unknown_freq += 1
    if row.payment_type == 6:
        voided_trip_freq += 1

    # If the tip amount and the extra payment are not null and the trip distance is greater than 0, add up the custom indicator
    if (not math.isnan(row.tip_amount)) & (not math.isnan(row.extra)) & (row.trip_distance > 0):
        custom_indicator_sum += (row.tip_amount + row.extra) / row.trip_distance
        counter_CI += 1

# Compute the average price per mile and the average custom indicator
average_price_per_mile = price_per_mile_sum / counter_PPM
average_custom_indicator = custom_indicator_sum / counter_CI

# Indicators to be saved
indicators = {
    "average_price_per_mile": average_price_per_mile,
    "credit_card_freq": credit_card_freq,
    "cash_freq": cash_freq,
    "no_charge_freq": no_charge_freq,
    "dispute_freq": dispute_freq,
    "unknown_freq": unknown_freq,
    "voided_trip_freq": voided_trip_freq,
    "average_custom_indicator": average_custom_indicator
}

# Transform indicators to a JSON object
indicators_json = json.dumps(indicators, indent = 4)

# Write the JSON object into a JSON file
with open(os.path.join(output_folder, date.replace('-', '') + '_yellow_taxi_kpis.json'), "w") as outfile:
    outfile.write(indicators_json)

# Write in the log that the data has been computed
with open(os.path.join(output_folder, 'log.txt'), "a") as outfile:
    outfile.write(date + ': The KPIs for the file yellow_tripdata_' + date + '.parquet have been computed\n')

print('KPIs succesfully computed and saved')