#Author: Jack Baldwin

import os, tempfile, requests, csv, arcpy
import pandas as pd

### Set Global Parameters
arcpy.env.workspace = # r"Path/To/Geodatabase.gdb"
gdb =  # r"Path/To/Geodatabase.gdb"
project_folder =  # r"Path/To/Project"
aprx_global = # r"Path/To/Project.aprx"
print(arcpy.env.workspace)
print(arcpy.env.workspace)
data_url_atlantic = r'https://covidtracking.com/data/download/all-states-history.csv'
data_url_nyt = r'https://github.com/nytimes/covid-19-data/raw/master/us-counties.csv'

os.remove( gdb + r'\map_state_indicator_rolling_positive.lyrx')
if arcpy.Exists("indicator_rolling_positive"):
    arcpy.Delete_management("indicator_rolling_positive")
if arcpy.Exists("rolling_positive"):
    arcpy.Delete_management("rolling_positive")
if arcpy.Exists("states_indicator_rolling_positive"):
    arcpy.Delete_management("states_indicator_rolling_positive")
if arcpy.Exists("states_rolling_positive"):
    arcpy.Delete_management("states_rolling_positive")




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

def positive_test_csv():

    if arcpy.Exists( gdb + r'\rolling_positive'):
        arcpy.Delete_management( gdb + r'\rolling_positive')

    state_date_positive_groupby = df_positive.groupby(['state', 'date'])
    state_date_positive_final = state_date_positive_groupby['positiveIncrease'].sum()
    state_date_positive_final.to_csv(os.path.join(arcpy.env.workspace, 'positive_by_state_date.csv'),index=True, header = True)

    s = df_positive.groupby("state").rolling(14, min_periods=1)["positive"].mean()
    df_positive["14DayAvg_positive"] = s.reset_index().set_index("level_1")["positive"]
    df_positive["14DayAvg_positive"] = df_positive["14DayAvg_positive"].round(0)
    df_positive.to_csv(os.path.join(arcpy.env.workspace, 'rolling_positive.csv'), index=True, header=True)

    df_positive['Date'] = pd.to_datetime(df_positive['date'])
    df_recent_date = df_positive.iloc[:56]
    df_recent_date = df_recent_date[['date', 'state', "14DayAvg_positive"]]
    print(df_recent_date)
    df_recent_date.to_csv(os.path.join(arcpy.env.workspace, 'indicator_rolling_positive.csv'), index=True, header=True)

    #Add to map
    arcpy.TableToTable_conversion(gdb + r"\rolling_positive.csv", arcpy.env.workspace, "rolling_positive")
    arcpy.TableToTable_conversion(gdb + r"\indicator_rolling_positive.csv", arcpy.env.workspace, "indicator_rolling_positive")

def attribute_join():
    # Set local variables
    in_features = "usa_states2"
    in_field = "STATE_ABBR"
    join_table = "rolling_positive"
    join_field = "state"
    out_feature = "states_rolling_positive"

    print('Beginning first state attribute join...')

    state_joined_table = arcpy.AddJoin_management(in_features, in_field, join_table,
                                                  join_field)

    arcpy.CopyFeatures_management(state_joined_table, out_feature)

    in_features = "usa_states2"
    in_field = "STATE_ABBR"
    join_table = "indicator_rolling_positive"
    join_field = "state"
    out_feature = "states_indicator_rolling_positive"

    print('Beginning second state attribute join...')

    state_joined_table = arcpy.AddJoin_management(in_features, in_field, join_table,
                                                  join_field)

    arcpy.CopyFeatures_management(state_joined_table, out_feature)

    arcpy.AddField_management(out_feature, "cases_per_capita", "DOUBLE", 9, "", "", "cases_per_capita", "NULLABLE", "REQUIRED")

    with arcpy.da.UpdateCursor(out_feature, ['indicator_rolling_positive_F14DayAvg_positive', 'usa_states2_POPULATION', 'cases_per_capita']) as cursor:
        for x in cursor:
            x[2] = x[0] / x[1]
            cursor.updateRow(x)


def set_map_symbology():

    print('Adding data to map...')

    ### Add to map
    in_features = gdb + "\states_indicator_rolling_positive"
    in_layer = "statesLyr"
    out_layer_file = "last_state.lyr"

    # Execute MakeFeatureLayer
    arcpy.MakeFeatureLayer_management(in_features, in_layer)

    # Execute SaveToLayerFile
    arcpy.SaveToLayerFile_management(in_layer, gdb +'\map_state_indicator_rolling_positive')
    aprx = arcpy.mp.ArcGISProject(project_folder + "\esriTeamDemo.aprx")
    aprx.defaultGeodatabase = gdb
    aprx.save()

    insertLyr = arcpy.mp.LayerFile(gdb + "\map_state_indicator_rolling_positive.lyrx")
    m = aprx.listMaps('covid19_cases')[0]
    for lyr in m.listLayers():
        if lyr.name == 'statesLyr':
            m.removeLayer(lyr)

    aprx.saveACopy(aprx_global)
    refLyr = m.listLayers("Light Gray Base")[0]
    m.insertLayer(refLyr, insertLyr, "BEFORE")
    aprx.saveACopy(aprx_global)

    print('Updating symbology...')

    ### Add Symbology
    lyr = m.listLayers('statesLyr')[0]
    sym = lyr.symbology

    sym.updateRenderer('GraduatedColorsRenderer')
    colorRamp = aprx.listColorRamps("Purples (Continuous)")[0]
    sym.renderer.colorRamp = colorRamp
    sym.renderer.classificationField = 'cases_per_capita'
    sym.renderer.breakCount = 10
    lyr.symbology = sym
    aprx.saveACopy(aprx_global)

    print('Successfully updated symbology!')

data_request_nyt(data_url_nyt)
df_death, df_hospitalized, df_positive, df_test = data_request_atlantic(data_url_atlantic)
positive_test_csv()
attribute_join()
set_map_symbology()
