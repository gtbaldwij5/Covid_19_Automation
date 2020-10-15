###Author: Jack Baldwin


import arcgis
from arcgis.gis import GIS
import pandas as pd
from arcgis.mapping import WebMap
from arcgis.features import FeatureLayerCollection
import os, tempfile, requests, csv, arcpy



arcpy.env.workspace = r"C:\Users\US19520\Documents\ArcGIS\Projects\covid_19\covid_19.gdb"
gdb = r"C:\Users\US19520\Documents\ArcGIS\Projects\covid_19\covid_19.gdb"
project_folder = r"C:\Users\US19520\Documents\ArcGIS\Projects\covid_19"
aprx_global = r"C:\Users\US19520\Documents\ArcGIS\Projects\covid_19\covid_19.aprx"
print(arcpy.env.workspace)
data_url_atlantic = r'https://covidtracking.com/data/download/all-states-history.csv'
data_url_nyt = r'https://github.com/nytimes/covid-19-data/raw/master/us-counties.csv'

def data_request_nyt(url):

    print('Beginning data request....')

    temp_dir = tempfile.mkdtemp()
    filename = os.path.join(temp_dir, 'covid19_data_nyt.csv')
    print(filename)
    response = requests.get(url)
    print('The covid 19 data request was successful:',response.ok)
    data_content = response.content
    data_file = open(filename,'wb')
    data_file.write(data_content)

    with open(filename,'r') as data_file:
        csv_reader = csv.reader(data_file)
        headers = next(csv_reader)

    ### Read csv and pull out last day data
    df_running_total = pd.read_csv(filename)
    df_running_total.to_csv(os.path.join(arcpy.env.workspace, 'county_covid19_cases.csv'),index=True, header = True)

    print(df_running_total.columns)

    df_running_total['Cases'] = 'Cases'
    del temp_dir





def data_request_atlantic(url):

    print('Beginning data request....')

    temp_dir = tempfile.mkdtemp()
    filename = os.path.join(temp_dir, 'covid19_data_atlantic.csv')
    print(filename)
    response = requests.get(url)
    print('The covid 19 data request was successful:',response.ok)
    data_content = response.content
    data_file = open(filename,'wb')
    data_file.write(data_content)

    with open(filename,'r') as data_file:
        csv_reader = csv.reader(data_file)
        headers = next(csv_reader)

    ### Read csv and pull out last day data
    df_running_total = pd.read_csv(filename)
    df_running_total.to_csv(os.path.join(arcpy.env.workspace, 'state_covid19_total.csv'),index=True, header = True)

    print(df_running_total.columns)

    df_death = df_running_total[['date', 'state', 'death', 'deathConfirmed',
       'deathIncrease', 'deathProbable']]

    df_hospitalized = df_running_total[['date', 'state', 'deathProbable', 'hospitalized',
       'hospitalizedCumulative', 'hospitalizedCurrently',
       'hospitalizedIncrease']]

    df_positive = df_running_total[['date', 'state', 'positive', 'positiveCasesViral', 'positiveIncrease', 'positiveScore']]

    df_test = df_running_total[['date', 'state', 'totalTestEncountersViral',
       'totalTestEncountersViralIncrease', 'totalTestResults',
       'totalTestResultsIncrease', 'totalTestsAntibody', 'totalTestsAntigen',
       'totalTestsPeopleAntibody', 'totalTestsPeopleAntigen',
       'totalTestsPeopleViral', 'totalTestsPeopleViralIncrease',
       'totalTestsViral', 'totalTestsViralIncrease', 'positiveIncrease']]


    del temp_dir

    print('Data request complete!')

    return df_death, df_hospitalized, df_positive, df_test


def death_csv():

    os.remove(gdb + r'\death_by_date.csv')
    os.remove( gdb + r'\death_by_state.csv')
    os.remove( gdb + r'\death_by_state_date.csv')
    os.remove( gdb + r'\rolling_death.csv')
    #os.remove(gdb + r'\indicator_rolling_death.csv')
    if arcpy.Exists('rolling_death'):
        arcpy.Delete_management('rolling_death')

    #Death by Date
    date_death_groupby = df_death.groupby(['date'])
    date_death_groupby_final = date_death_groupby['deathIncrease'].sum()
    df_date_death_groupby_final = pd.DataFrame(date_death_groupby_final)
    df_date_death_groupby_final['deathIncrease_avg'] = df_date_death_groupby_final.rolling(14, min_periods=1)['deathIncrease'].mean()
    df_date_death_groupby_final.to_csv(os.path.join(arcpy.env.workspace, 'death_by_date.csv'),index=True, header = True)

    state_death_groupby = df_death.groupby(['state'])
    state_death_groupby_final = state_death_groupby['deathIncrease'].sum()
    df_state_death_groupby_final = pd.DataFrame(state_death_groupby_final)
    df_state_death_groupby_final.to_csv(os.path.join(arcpy.env.workspace, 'death_by_state.csv'),index=True, header = True)

    state_date_death_groupby = df_death.groupby(['state', 'date'])
    state_date_death_groupby_final = state_date_death_groupby['deathIncrease'].sum()

    df_state_date_death_groupby_final = pd.DataFrame(state_date_death_groupby_final)
    df_state_date_death_groupby_final.to_csv(os.path.join(arcpy.env.workspace, 'death_by_state.csv'), index=True,
                                        header=True)
    df_state_date_death_groupby_final.to_csv(os.path.join(arcpy.env.workspace, 'death_by_state_date.csv'),index=True, header = True)

    s = df_death.groupby("state").rolling(14, min_periods=1)["deathIncrease"].mean()
    df_death["14DayAvg_death"] = s.reset_index().set_index("level_1")["deathIncrease"]
    df_death["14DayAvg_death"] = df_death["14DayAvg_death"].round(0)
    df_death.to_csv(os.path.join(arcpy.env.workspace, 'rolling_death.csv'), index=True, header=True)

    df_death['Date'] = pd.to_datetime(df_death['date'])
    recent_date = df_death['Date'].max()
    #print(recent_date)
    df_recent_date = df_death.iloc[:56]
    df_recent_date = df_recent_date[['date', 'state', "14DayAvg_death"]]
    #print(df_recent_date)
    df_recent_date.to_csv(os.path.join(arcpy.env.workspace, 'indicator_rolling_death.csv'), index=True, header=True)


    #Add to map
    arcpy.TableToTable_conversion(gdb + r"\rolling_death.csv", arcpy.env.workspace, "rolling_death")
    arcpy.TableToTable_conversion(gdb + r"\indicator_rolling_death.csv", arcpy.env.workspace, "indicator_rolling_death")
    # aprx = arcpy.mp.ArcGISProject(aprx_global)
    # addTab = arcpy.mp.Table( gdb + r'\rolling_death')
    # m = aprx.listMaps()[0]
    # m.addTable(addTab)
    # aprx.saveACopy(aprx_global)


def hospitalized_csv():

    os.remove(gdb + r'\hospitalized_by_date.csv')
    os.remove(gdb + r'\hospitalized_by_state.csv')
    os.remove(gdb + r'\hospitalized_by_state_date.csv')
    os.remove(gdb + r'\rolling_hospitalized.csv')
    os.remove(gdb + r'\indicator_rolling_hospitalized.csv')
    if arcpy.Exists( gdb + r'\rolling_hospitalized'):
        arcpy.Delete_management( gdb + r'\rolling_hospitalized')

    date_hospitalized_groupby = df_hospitalized.groupby(['date'])
    date_hospitalized_groupby_final = date_hospitalized_groupby['hospitalizedCurrently'].sum()
    date_hospitalized_groupby_final.to_csv(os.path.join(arcpy.env.workspace, 'hospitalized_by_date.csv'),index=True, header = True)

    state_hospitalized_groupby = df_hospitalized.groupby(['state'])
    state_hospitalized_groupby_final = state_hospitalized_groupby['hospitalizedCurrently'].sum()
    state_hospitalized_groupby_final.to_csv(os.path.join(arcpy.env.workspace, 'hospitalized_by_state.csv'),index=True, header = True)

    state_date_hospitalized_groupby = df_hospitalized.groupby(['state', 'date'])
    state_date_hospitalized_final = state_date_hospitalized_groupby['hospitalizedCurrently'].sum()
    state_date_hospitalized_final.to_csv(os.path.join(arcpy.env.workspace, 'hospitalized_by_state_date.csv'),index=True, header = True)

    s = df_hospitalized.groupby("state").rolling(14, min_periods=1)["hospitalizedCurrently"].mean()
    df_hospitalized["14DayAvg_hospitalized"] = s.reset_index().set_index("level_1")["hospitalizedCurrently"]
    df_hospitalized["14DayAvg_hospitalized"] = df_hospitalized["14DayAvg_hospitalized"].round(0)
    df_hospitalized.to_csv(os.path.join(arcpy.env.workspace, 'rolling_hospitalized.csv'), index=True, header=True)

    df_hospitalized['Date'] = pd.to_datetime(df_hospitalized['date'])
    recent_date = df_hospitalized['Date'].max()
    #print(recent_date)
    df_recent_date = df_hospitalized.iloc[:56]
    df_recent_date = df_recent_date[['date', 'state', "14DayAvg_hospitalized"]]
    print(df_recent_date)
    df_recent_date.to_csv(os.path.join(arcpy.env.workspace, 'indicator_rolling_hospitalized.csv'), index=True, header=True)



    #Add to map
    arcpy.TableToTable_conversion(gdb + r"\rolling_hospitalized.csv", arcpy.env.workspace, "rolling_hospitalized")
    arcpy.TableToTable_conversion(gdb + r"\indicator_rolling_hospitalized.csv", arcpy.env.workspace, "indicator_rolling_hospitalized")
    # aprx = arcpy.mp.ArcGISProject(aprx_global)
    # addTab = arcpy.mp.Table( gdb + r'\rolling_hospitalized')
    # m = aprx.listMaps()[0]
    # m.addTable(addTab)
    # aprx.saveACopy(aprx_global)

def positive_test_csv():

    os.remove(gdb + r'\positive_by_date.csv')
    os.remove(gdb + r'\positive_by_state.csv')
    os.remove(gdb + r'\positive_by_state_date.csv')
    os.remove(gdb + r'\rolling_positive.csv')
    os.remove(gdb + r'\indicator_rolling_positive.csv')
    if arcpy.Exists( gdb + r'\rolling_positive'):
        arcpy.Delete_management( gdb + r'\rolling_positive')

    date_positive_groupby = df_positive.groupby(['date'])
    date_positive_groupby_final = date_positive_groupby['positiveIncrease'].sum()
    date_positive_groupby_final.to_csv(os.path.join(arcpy.env.workspace, 'positive_by_date.csv'),index=True, header = True)

    state_positive_groupby = df_positive.groupby(['state'])
    state_positive_groupby_final = state_positive_groupby['positiveIncrease'].sum()
    state_positive_groupby_final.to_csv(os.path.join(arcpy.env.workspace, 'positive_by_state.csv'),index=True, header = True)

    state_date_positive_groupby = df_positive.groupby(['state', 'date'])
    state_date_positive_final = state_date_positive_groupby['positiveIncrease'].sum()
    state_date_positive_final.to_csv(os.path.join(arcpy.env.workspace, 'positive_by_state_date.csv'),index=True, header = True)

    s = df_positive.groupby("state").rolling(14, min_periods=1)["positiveIncrease"].mean()
    df_positive["14DayAvg_positive"] = s.reset_index().set_index("level_1")["positiveIncrease"]
    df_positive["14DayAvg_positive"] = df_positive["14DayAvg_positive"].round(0)
    df_positive.to_csv(os.path.join(arcpy.env.workspace, 'rolling_positive.csv'), index=True, header=True)

    df_positive['Date'] = pd.to_datetime(df_positive['date'])
    recent_date = df_positive['Date'].max()
    # print(recent_date)
    df_recent_date = df_positive.iloc[:56]
    df_recent_date = df_recent_date[['date', 'state', "14DayAvg_positive"]]
    print(df_recent_date)
    df_recent_date.to_csv(os.path.join(arcpy.env.workspace, 'indicator_rolling_positive.csv'), index=True, header=True)

    #Add to map
    arcpy.TableToTable_conversion(gdb + r"\rolling_positive.csv", arcpy.env.workspace, "rolling_positive")
    arcpy.TableToTable_conversion(gdb + r"\indicator_rolling_positive.csv", arcpy.env.workspace, "indicator_rolling_positive")
    # aprx = arcpy.mp.ArcGISProject(aprx_global)
    # addTab = arcpy.mp.Table( gdb + r'\rolling_positive')
    # m = aprx.listMaps()[0]
    # m.addTable(addTab)
    # aprx.saveACopy(aprx_global)


def test_csv():

    os.remove(gdb + r'\test_by_date.csv')
    os.remove(gdb + r'\test_by_state.csv')
    os.remove(gdb + r'\test_by_state_date.csv')
    os.remove(gdb + r'\rolling_test_results.csv')
    # os.remove(gdb + r'\indicator_rolling_test_results.csv')
    os.remove(gdb + r'\rolling_test_results_final.csv')
    os.remove(gdb + r'\rolling_positive_final.csv')
    if arcpy.Exists( gdb + r'\rolling_test_results'):
        arcpy.Delete_management( gdb + r'\rolling_test_results')

    date_test_groupby = df_test.groupby(['date'])
    date_test_groupby_final = date_test_groupby['totalTestResultsIncrease'].sum()
    date_test_groupby_final.to_csv(os.path.join(arcpy.env.workspace, 'test_by_date.csv'),index=True, header = True)

    state_test_groupby = df_test.groupby(['state'])
    state_test_groupby_final = state_test_groupby['totalTestResultsIncrease'].sum()
    state_test_groupby_final.to_csv(os.path.join(arcpy.env.workspace, 'test_by_state.csv'),index=True, header = True)

    state_date_test_groupby = df_test.groupby(['state', 'date'])
    state_date_test_final = state_date_test_groupby['totalTestResultsIncrease'].sum()
    state_date_test_final.to_csv(os.path.join(arcpy.env.workspace, 'test_by_state_date.csv'),index=True, header = True)

    s = df_test.groupby("state").rolling(14, min_periods=1)["totalTestResultsIncrease"].mean()
    df_test["14DayAvg_test"] = s.reset_index().set_index("level_1")["totalTestResultsIncrease"]
    df_test["14DayAvg_test"] = df_test["14DayAvg_test"].round(0)
    df_test["14DayAvg"] = df_test["14DayAvg_test"]
    df_test.to_csv(os.path.join(arcpy.env.workspace, 'rolling_test_results.csv'), index=True, header=True)

    df_test['Variable'] = 'Tests'
    df_test_final = df_test[['state', 'date', 'Variable', '14DayAvg']]
    df_test_final.to_csv(os.path.join(arcpy.env.workspace, 'rolling_test_results_final.csv'), index=True, header=True)

    p = df_positive.groupby("state").rolling(14, min_periods=1)["positiveIncrease"].mean()
    df_positive["14DayAvg_positive"] = p.reset_index().set_index("level_1")["positiveIncrease"]
    df_positive["14DayAvg_positive"] = df_positive["14DayAvg_positive"].round(0)
    df_positive["14DayAvg"] = df_positive["14DayAvg_positive"]
    df_positive['Variable'] = 'Cases'
    df_positive_final = df_positive[['state', 'date', 'Variable', '14DayAvg']]
    df_positive_final.to_csv(os.path.join(arcpy.env.workspace, 'rolling_positive_final.csv'), index=True, header=True)

    df_positive_test_final = df_test_final.append(df_positive_final)
    df_positive_test_final.to_csv(os.path.join(arcpy.env.workspace, 'rolling_positive_test_final.csv'), index=True, header=True)

    df_test['Date'] = pd.to_datetime(df_test['date'])
    recent_date = df_test['Date'].max()
    # print(recent_date)
    df_recent_date = df_test.iloc[:56]
    df_recent_date = df_recent_date[['date', 'state', "14DayAvg_test"]]
    print(df_recent_date)
    df_recent_date.to_csv(os.path.join(arcpy.env.workspace, 'indicator_rolling_test.csv'), index=True, header=True)

    #Add to map
    arcpy.TableToTable_conversion(gdb + r"\rolling_test_results.csv", arcpy.env.workspace, "rolling_test_results")
    arcpy.TableToTable_conversion(gdb + r"\indicator_rolling_test.csv", arcpy.env.workspace, "indicator_rolling_test_results")
    arcpy.TableToTable_conversion(gdb + r"\rolling_positive_test_final.csv", arcpy.env.workspace, "rolling_positive_test")
    # aprx = arcpy.mp.ArcGISProject(aprx_global)
    # addTab = arcpy.mp.Table( gdb + r'\rolling_test_results')
    # m = aprx.listMaps()[0]
    # m.addTable(addTab)
    # aprx.saveACopy(aprx_global)




aprx = arcpy.mp.ArcGISProject(aprx_global)
m = aprx.listMaps("Covid 19 Cases per Capita (States)")[0]
tables = arcpy.ListTables()
for table in tables:
    print(table)
    arcpy.Delete_management(table)
aprx.saveACopy(aprx_global)



data_request_nyt(data_url_nyt)
df_death, df_hospitalized, df_positive, df_test = data_request_atlantic(data_url_atlantic)
death_csv()
hospitalized_csv()
positive_test_csv()
test_csv()
