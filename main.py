__author__ = 'Luke Roy'

import numpy as np
import time
import pandas as pd


# checks pond tuple values against normal range values and creates an errors array
# 1 is added to errors array if there is an abnormal variable value detected
# 0 is added to errors array if the variable value is within acceptable value range
def abnormality_check(row):
    errors = []

    # thresholds
    nitrate_min = 0
    nitrate_max = 60
    PH_min = 6.0
    PH_max = 9.0
    ammonia_min = 0
    ammonia_max = 0.25
    temp_min = 18
    temp_max = 24
    DO_min = 6.0
    DO_max = 12.0
    turbidity_min = 1
    turbidity_max = 30
    manganese_min = 0
    manganese_max = 0.05

    # tuple values for each pond variable tracked
    nitrate = row[2]
    PH = row[3]
    ammonia = row[4]
    temp = row[5]
    DO = row[6]
    turbidity = row[7]
    manganese = row[8]

    # compare tuple values to acceptable range of normal values. If outside of normal range a 1 is added to errors array
    if nitrate > nitrate_max or nitrate < nitrate_min:
        errors.append(1)
    else:
        errors.append(0)

    if PH > PH_max or PH < PH_min:
        errors.append(1)
    else:
        errors.append(0)

    if ammonia > ammonia_max or ammonia < ammonia_min:
        errors.append(1)
    else:
        errors.append(0)

    if temp > temp_max or temp < temp_min:
        errors.append(1)
    else:
        errors.append(0)

    if DO > DO_max or DO < DO_min:
        errors.append(1)
    else:
        errors.append(0)

    if turbidity > turbidity_max or turbidity < turbidity_min:
        errors.append(1)
    else:
        errors.append(0)

    if manganese > manganese_max or manganese < manganese_min:
        errors.append(1)
    else:
        errors.append(0)

    return np.array(errors)


# sums up all 1 values in the errors array for each column.
# If this sum is greater than the specified error_rate, then an error is added to the abnormality report.
# errors_present: flag used by station_manager function to output no abnormalities report
def consistency_check(report, errors, error_rate, window):
    errors_present = False
    column = 2  # index of first variable in report

    while column < len(report.axes[1]):
        if errors.sum()[column - 2] > error_rate:
            # reports average value of pond variable by summing each tuple value and dividing by window size
            print(f"Error: {report.columns[column]} abnormal with reading value {round(report.sum()[column] / window, 2)}")
        column += 1
        errors_present = True

    return errors_present


# takes a dataframe of pond tuples within the window specified and passes each tuple to be checked for abnormalities.
# after the abnormalities are found, creates a report specifying which pond variables are abnormal
def station_manager(report, window, error_rate):
    # specify schema for abnormalities
    df = pd.DataFrame(columns=["NITRATE(PPM)", "PH", "AMMONIA(mg/l)", "TEMP", "DO", "TURBIDITY", "MANGANESE(mg/l)"])
    index = 0

    # check each tuple for abnormalities and adds row of abnormalities to dataframe
    while index < window:
        df.loc[len(df)] = abnormality_check(report.iloc[index])
        index += 1

    # report back which pond variables are not within normal value ranges or reports no abnormalities detected
    print(f"{report.iloc[0][0]} Report between window times {report.iloc[0][1]} and {report.iloc[window - 1][1]}")
    if not consistency_check(report, df, error_rate, window):
        print("No abnormalities detected \n")
    else:
        print()


# takes in pond csv file and treats it like a datastream as if the three pond stations are sending data all at once
# index: used to track which temporal tuple is being sent
# window: how large of a window the datastream will process. window of 3 will process 3 tuples at a time
# error_rate: how many tuples in a window need to be above or below the variable threshold for an error to be reported
# loop_delay: how many seconds the datastream will wait before passing data through to be processed
def main():
    index = 0
    window = 5
    error_rate = 3
    loop_delay = 5

    # read in csv file and split into dataframes for each station
    df = pd.read_csv("Ponds.csv")
    df_split = df.groupby("Station")
    station_1 = df_split.get_group("station1")
    station_2 = df_split.get_group("station2")
    station_3 = df_split.get_group("station3")

    # pass dataframes of size window to station_manager for abnormality report
    while 1:
        station_manager(station_1.iloc[index:index+window], window, error_rate)
        station_manager(station_2.iloc[index:index+window], window, error_rate)
        station_manager(station_3.iloc[index:index+window], window, error_rate)
        index += 1
        time.sleep(loop_delay)


if __name__ == "__main__":
    main()
