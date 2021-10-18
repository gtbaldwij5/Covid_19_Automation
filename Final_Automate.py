#Author: Jack Baldwin


#   Requirements:
#       1) ArcGis Pro project open named 'covid_19'
#       2.a) Downloaded states feature class from ESRI: https://www.arcgis.com/home/item.html?id=1a6cae723af14f9cae228b133aebc620
#           2.b) states feature class is labelled 'usa_states'
#       3) A base map called 'Light Gray Base' is applied to the map
#       4) Script is run through ArcGIS python interpreter on PyCharm IDE

#########################################################################################
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
import os, tempfile, requests, csv, arcpy
import pandas as pd
from arcgis.gis import GIS

### Set Global Parameters
arcpy.env.workspace = r"C:\Users\US19520\Documents\ArcGIS\Projects\covid_19\covid_19.gdb"
gdb = r"C:\Users\US19520\Documents\ArcGIS\Projects\covid_19\covid_19.gdb"
project_folder = r"C:\Users\US19520\Documents\ArcGIS\Projects\covid_19"
aprx_global = r"C:\Users\US19520\Documents\ArcGIS\Projects\covid_19\covid_19.aprx"
print(arcpy.env.workspace)
data_url = 'https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_confirmed_usafacts.csv'
data_url_atlantic = r'https://covidtracking.com/data/download/all-states-history.csv'
data_url_nyt = r'https://github.com/nytimes/covid-19-data/raw/master/us-counties.csv'
prjPath = r"C:\Users\US19520\Documents\ArcGIS\Projects\covid_19\covid_19.aprx"


def remove_previous_files():
    print('Removing previous files...')

    # os.remove( gdb + r'\last_state.lyrx')
    # os.remove( gdb + r'\last_county.lyrx')
    # os.remove( gdb + r'\last_office.lyrx')
    # os.remove( gdb + r'\covid19_last_day.csv')
    # os.remove( gdb + r'\county_covid19_total.csv')
    # os.remove( gdb + r'\county_covid19_last_day.csv')
    # os.remove( gdb + r'\county_covid19_last_weeks.csv')
    if arcpy.Exists("county_last_day"):
        arcpy.Delete_management("county_last_day")
    if arcpy.Exists("county_total"):
        arcpy.Delete_management("county_total")
    if arcpy.Exists("state_last_day"):
        arcpy.Delete_management("state_last_day")
    if arcpy.Exists("states_last_day"):
        arcpy.Delete_management("states_last_day")
    if arcpy.Exists("statesLyr"):
        arcpy.removeLayer_management("statesLyr")
    if arcpy.Exists("countiesLyr"):
        arcpy.removeLayer_management("countiesLyr")
    if arcpy.Exists("counties_last_day"):
        arcpy.Delete_management("counties_last_day")
    if arcpy.Exists("county_last_weeks"):
        arcpy.Delete_management("county_last_weeks")
    if arcpy.Exists("offices_last_day"):
        arcpy.Delete_management("offices_last_day")
    if arcpy.Exists("counties_last_weeks"):
        arcpy.Delete_management("counties_last_weeks")
    if arcpy.Exists("offices_last_weeks"):
        arcpy.Delete_management("offices_last_weeks")

    print('Previous files successfully removed!')


def data_request(url):

    print('Beginning data request....')

    temp_dir = tempfile.mkdtemp()
    filename = os.path.join(temp_dir, 'covid19_data.csv')
    print(filename)
    response = requests.get(url)
    print('The covid 19 data request was successful:',response.ok)
    data_content = response.content
    data_file = open(filename,'wb')
    data_file.write(data_content)

    with open(filename,'r') as data_file:
        csv_reader = csv.reader(data_file)
        headers = next(csv_reader)

    ### Slice for last day and convert to csv
    dates = headers[4:len(headers)]
    last_day = dates[-1]
    print(last_day)
    last_weeks = dates[-16:-1]
    print(last_weeks)

    ### Read csv and pull out last day data
    df_running_total = pd.read_csv(filename)
    df_running_total.to_csv(os.path.join(arcpy.env.workspace, 'county_covid19_total.csv'),index=True, header = True)

    del temp_dir

    print('Data request complete!')

    return df_running_total, last_day, last_weeks





#arcpy.management.Delete(r" 'county_last_day.csv'; 'county_total.csv'; 'state_last_day.csv';'states_last_day.csv'; 'last_state.lyrx'")



def add_data_to_map():

    print('Beginning to add state level data to geodatabase...')

    arcpy.TableToTable_conversion("county_covid19_total.csv", arcpy.env.workspace, "county_total", field_mapping = "FIRST")

    for col in df_running_total.columns:
        print(col)

    df_last_day = df_running_total[['countyFIPS', 'County Name', 'State', 'StateFIPS', last_day]]

    df_last_day.to_csv(os.path.join(arcpy.env.workspace, 'county_covid19_last_day.csv'),index=True, header = True)

    arcpy.TableToTable_conversion("county_covid19_last_day.csv", arcpy.env.workspace, "county_last_day",
                                  field_mapping='FIRST')

    base = last_weeks[0]
    day_1 = last_weeks[1]
    day_2 = last_weeks[2]
    day_3 = last_weeks[3]
    day_4 = last_weeks[4]
    day_5 = last_weeks[5]
    day_6 = last_weeks[6]
    day_7 = last_weeks[7]
    day_8 = last_weeks[8]
    day_9 = last_weeks[9]
    day_10 = last_weeks[10]
    day_11 = last_weeks[11]
    day_12 = last_weeks[12]
    day_13 = last_weeks[13]

    df_last_weeks = df_running_total[['countyFIPS', 'County Name', 'State', 'StateFIPS', base, day_1, day_2, day_3, day_4,
                                      day_5, day_6, day_7, day_8, day_9, day_10, day_11, day_12, day_13, last_day]]

    df_last_weeks[last_day] = df_last_weeks[last_day] - df_last_weeks[day_13]
    df_last_weeks[day_13] = df_last_weeks[day_13] - df_last_weeks[day_12]
    df_last_weeks[day_12] = df_last_weeks[day_12] - df_last_weeks[day_11]
    df_last_weeks[day_11] = df_last_weeks[day_11] - df_last_weeks[day_10]
    df_last_weeks[day_10] = df_last_weeks[day_10] - df_last_weeks[day_9]
    df_last_weeks[day_9] = df_last_weeks[day_9] - df_last_weeks[day_8]
    df_last_weeks[day_8] = df_last_weeks[day_8] - df_last_weeks[day_7]
    df_last_weeks[day_7] = df_last_weeks[day_7] - df_last_weeks[day_6]
    df_last_weeks[day_6] = df_last_weeks[day_6] - df_last_weeks[day_5]
    df_last_weeks[day_5] = df_last_weeks[day_5] - df_last_weeks[day_4]
    df_last_weeks[day_4] = df_last_weeks[day_4] - df_last_weeks[day_3]
    df_last_weeks[day_3] = df_last_weeks[day_3] - df_last_weeks[day_2]
    df_last_weeks[day_2] = df_last_weeks[day_2] - df_last_weeks[day_1]
    df_last_weeks[day_1] = df_last_weeks[day_1] - df_last_weeks[base]

    df_last_weeks['total_weeks'] = df_last_weeks[last_day] + df_last_weeks[day_13] + df_last_weeks[day_12] + df_last_weeks[day_11] + df_last_weeks[day_10] + df_last_weeks[day_9] + df_last_weeks[day_8] + df_last_weeks[day_7] + df_last_weeks[day_6] + df_last_weeks[day_5] + df_last_weeks[day_4] + df_last_weeks[day_3] + df_last_weeks[day_2] + df_last_weeks[day_1]

    df_last_weeks.to_csv(os.path.join(arcpy.env.workspace, 'county_covid19_last_weeks.csv'), index=True, header=True)

    arcpy.TableToTable_conversion(gdb + r"/county_covid19_last_weeks.csv", arcpy.env.workspace, "county_last_weeks")

    ###  Last day groupby state
    df_last_weeks_day = df_last_weeks[['countyFIPS', 'County Name', 'State', 'StateFIPS', 'total_weeks']]

    last_day_groupby = df_last_weeks_day.groupby(['State'])
    last_day_final = last_day_groupby['total_weeks'].sum()

    last_day_final.to_csv(os.path.join(arcpy.env.workspace, 'covid19_last_day.csv'),index=True, header = True)

    arcpy.TableToTable_conversion( gdb + r"\covid19_last_day.csv", arcpy.env.workspace, "state_last_day")
    #arcpy.AlterField_management(r'state_last_day', 'Field1', 'state', 'state')
    arcpy.AlterField_management(r'state_last_day', 'Field2', 'current_cases', 'current_cases')

    # Set local variables
    in_features = "usa_states"
    in_field = "STATE_ABBR"
    join_table = "state_last_day"
    join_field = "state"
    out_feature = "states_last_day"

    print('Beginning state attribute join...')

    state_joined_table = arcpy.AddJoin_management(in_features, in_field, join_table,
                                                  join_field)

    arcpy.CopyFeatures_management(state_joined_table, out_feature)

    ### Add numeric field
    arcpy.AddField_management(out_feature, "current_cases_double", "DOUBLE", 9, "", "", "current_cases_double", "NULLABLE", "REQUIRED")
    arcpy.AddField_management(out_feature, "updated_cases_per_capita", "DOUBLE", 9, "", "", "updated_cases_per_capita", "NULLABLE", "REQUIRED")
    arcpy.AddField_management(out_feature, "updated_cases_per_capita_text", "TEXT", 5, "", "", "updated_cases_per_capita_text", "NULLABLE", "REQUIRED")

    with arcpy.da.UpdateCursor(r'states_last_day', ['state_last_day_current_cases','current_cases_double']) as cursor:
        for x in cursor:
            x[1] = x[0]
            cursor.updateRow(x)

    with arcpy.da.UpdateCursor(r'states_last_day', ['current_cases_double','usa_states_POPULATION', 'updated_cases_per_capita']) as cursor:
        for x in cursor:
            x[2] = x[0]/x[1]*100
            cursor.updateRow(x)

    with arcpy.da.UpdateCursor(r'states_last_day', ['updated_cases_per_capita', 'updated_cases_per_capita_text']) as cursor:
        for x in cursor:
            x[1] = x[0]
            x[1] = round(float(x[1]), 3)
            x[1] = str(x[1])
            x[1] = x[1].lstrip('0')
            cursor.updateRow(x)

    print('State data successfully added to geodatabase!')


def county_add():
    print('Beginning to add county level data to geodatabase...')

    arcpy.AddField_management('county_last_weeks', "state_fixed", "TEXT", 9, "", "", "state_fixed", "NULLABLE", "REQUIRED")
    arcpy.AddField_management('usa_counties', "GEOID", "TEXT", 9, "", "", "GEOID", "NULLABLE", "REQUIRED")
    arcpy.AddField_management('county_last_weeks', "GEOID", "TEXT", 9, "", "", "GEOID", "NULLABLE", "REQUIRED")


    with arcpy.da.UpdateCursor(r'county_last_weeks', ['Field5', 'state_fixed']) as cursor:
        for x in cursor:
            if len(x[0]) == 1:
                x[1] = '0' + x[0]
            else:
                x[1] = x[0]
            cursor.updateRow(x)

    with arcpy.da.UpdateCursor(r'county_last_weeks', ['Field2', 'GEOID']) as cursor:
        for x in cursor:
            if len(x[0]) == 4:
                x[1] = '0' + x[0]
            else:
                x[1] = x[0]
            cursor.updateRow(x)

    with arcpy.da.UpdateCursor(r'usa_counties', ['STATE_FIPS', 'CNTY_FIPS', 'GEOID']) as cursor:
        for x in cursor:
            x[2] = x[0] + x[1]
            cursor.updateRow(x)

    print('Beginning county attribute join...')

    in_features = "usa_counties"
    in_field = "GEOID"
    join_table = "county_last_weeks"
    join_field = "GEOID"
    out_feature = "counties_last_weeks"


    county_joined_table = arcpy.AddJoin_management(in_features, in_field, join_table,
                                                  join_field)

    arcpy.CopyFeatures_management(county_joined_table, out_feature)

    ###Add Numeric Fields
    arcpy.AddField_management(out_feature, "current_cases_double", "DOUBLE", 9, "", "", "current_cases_double", "NULLABLE", "REQUIRED")
    arcpy.AddField_management(out_feature, "updated_cases_per_capita", "DOUBLE", 9, "", "", "updated_cases_per_capita", "NULLABLE", "REQUIRED")

    with arcpy.da.UpdateCursor(r'counties_last_weeks', ['county_last_weeks_Field21','current_cases_double']) as cursor:
        for x in cursor:
            x[1] = x[0]
            cursor.updateRow(x)

    with arcpy.da.UpdateCursor(r'counties_last_weeks', ['current_cases_double','usa_counties_POPULATION', 'updated_cases_per_capita']) as cursor:
        for x in cursor:
            if x[0] == None:
                x[0] = 0
            #print(x[0])
            x[2] = x[0]/x[1] * 100
            cursor.updateRow(x)

    in_features = gdb + "\counties_last_weeks"
    in_layer = "countiesLyr"
    out_layer_file = "last_county.lyr"

    # Execute MakeFeatureLayer
    arcpy.MakeFeatureLayer_management(in_features, in_layer)

    # Execute SaveToLayerFile
    arcpy.SaveToLayerFile_management(in_layer, gdb + '\last_county')

    aprx = arcpy.mp.ArcGISProject(project_folder + "\covid_19.aprx")

    aprx.defaultGeodatabase = gdb

    aprx.save()

    insertLyr = arcpy.mp.LayerFile(gdb + "\last_county.lyrx")

    m = aprx.listMaps('Covid 19 Cases per Capita (Counties)')[0]

    for lyr in m.listLayers():
        if lyr.name == 'countiesLyr':
            m.removeLayer(lyr)

    aprx.saveACopy(aprx_global)

    refLyr = m.listLayers("Light Gray Base")[0]

    m.insertLayer(refLyr, insertLyr, "BEFORE")

    aprx.saveACopy(aprx_global)

    print('Updating symbology...')

    ### Add Symbology
    lyr = m.listLayers('countiesLyr')[0]
    sym = lyr.symbology

    sym.updateRenderer('GraduatedColorsRenderer')
    colorRamp = aprx.listColorRamps("Purples (Continuous)")[0]
    sym.renderer.colorRamp = colorRamp
    sym.renderer.classificationField = 'updated_cases_per_capita'
    sym.renderer.breakCount = 10


    lyr.symbology = sym

    aprx.saveACopy(aprx_global)

    print('County data successfully added to geodatabase!')

def office_function():
    print('evaluating offices...')

    in_features = "gt_office_point"
    in_field = "gt_offic_6"
    join_table = "county_last_weeks"
    join_field = "GEOID"
    out_feature = "offices_last_weeks"

    office_joined_table = arcpy.AddJoin_management(in_features, in_field, join_table,
                                                   join_field)

    arcpy.CopyFeatures_management(office_joined_table, out_feature)

    arcpy.AddField_management(out_feature, "current_cases_double", "DOUBLE", 9, "", "", "current_cases_double",
                              "NULLABLE", "REQUIRED")
    arcpy.AddField_management(out_feature, "updated_cases_per_capita", "DOUBLE", 9, "", "", "updated_cases_per_capita",
                              "NULLABLE", "REQUIRED")

    with arcpy.da.UpdateCursor(r'offices_last_weeks', ['county_last_weeks_Field21','current_cases_double']) as cursor:
        for x in cursor:
            x[1] = x[0]
            cursor.updateRow(x)

    with arcpy.da.UpdateCursor(r'offices_last_weeks', ['current_cases_double','gt_office_point_usa_coun_5', 'updated_cases_per_capita']) as cursor:
        for x in cursor:
            x[2] = x[0]/x[1] * 100
            cursor.updateRow(x)

    in_features = gdb + "\offices_last_weeks"
    in_layer = "officesLyr"
    out_layer_file = "last_office.lyr"

    # Execute MakeFeatureLayer
    arcpy.MakeFeatureLayer_management(in_features, in_layer)

    # Execute SaveToLayerFile
    arcpy.SaveToLayerFile_management(in_layer, gdb + '\last_office')

    aprx = arcpy.mp.ArcGISProject(project_folder + "\covid_19.aprx")

    aprx.defaultGeodatabase = gdb

    aprx.save()

    insertLyr = arcpy.mp.LayerFile(gdb + "\last_office.lyrx")

    m = aprx.listMaps('Grant Thornton Offices')[0]

    for lyr in m.listLayers():
        if lyr.name == 'officesLyr':
            m.removeLayer(lyr)

    aprx.saveACopy(aprx_global)

    refLyr = m.listLayers("Light Gray Base")[0]

    m.insertLayer(refLyr, insertLyr, "BEFORE")

    aprx.saveACopy(aprx_global)

    print('Updating symbology...')

    ### Add Symbology
    lyr = m.listLayers('officesLyr')[0]
    sym = lyr.symbology

    if sym.renderer.type != 'SimpleRenderer':
        sym.updateRenderer('SimpleRenderer')
    sym.updateRenderer('GraduatedSymbolsRenderer')
    sym.renderer.classificationField = 'updated_cases_per_capita'
    sym.renderer.breakCount = 4

    points_labels = ['0% - 0.5%', '> 0.5% - 1.0%', '> 1.0% - 1.5%', '> 2.0%']
    points_upperBounds = [0.5, 1.0, 1.5, 100.0]
    points_sizes = [6, 16.50, 27, 37.50]
    layers_colors = [ {'RGB': [203, 201, 226, 80]}, {'RGB': [158, 154, 200, 80]},
                      {'RGB': [117, 107, 177, 80]}, {'RGB': [84, 39, 143, 80]},]

    for i in range(4):
        item = sym.renderer.classBreaks[i]
        item.symbol.applySymbolFromGallery('Circle', 1)
        item.label = points_labels[i]
        item.upperBound = points_upperBounds[i]
        item.symbol.size = points_sizes[i]
        item.symbol.color = layers_colors[i]

    lyr.symbology = sym

    aprx.saveACopy(aprx_global)

    print('Office data successfully added to geodatabase!')




def set_map_symbology():

    print('Adding data to map...')

    ### Add to map
    in_features = gdb + "\states_last_day"
    in_layer = "statesLyr"
    out_layer_file = "last_state.lyr"

    # Execute MakeFeatureLayer
    arcpy.MakeFeatureLayer_management(in_features, in_layer)

    # Execute SaveToLayerFile
    arcpy.SaveToLayerFile_management(in_layer, gdb +'\last_state')

    aprx = arcpy.mp.ArcGISProject(project_folder + "\covid_19.aprx")

    aprx.defaultGeodatabase = gdb

    aprx.save()

    insertLyr = arcpy.mp.LayerFile(gdb + "\last_state.lyrx")

    m = aprx.listMaps('Covid 19 Cases per Capita (States)')[0]

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
###########################


    # sym.updateRenderer('GraduatedColorsRenderer')
    # colorRamp = aprx.listColorRamps("Purples (Continuous)")[0]
    # sym.renderer.colorRamp = colorRamp
    # sym.renderer.classificationField = 'cases_per_capita'
    # sym.renderer.breakCount = 10
    # lyr.symbology = sym
    # aprx.saveACopy(aprx_global)
    #
    # print('Successfully updated symbology!')

    #################
    sym.updateRenderer('GraduatedColorsRenderer')
    colorRamp = aprx.listColorRamps("Purples (Continuous)")[0]
    sym.renderer.colorRamp = colorRamp
    sym.renderer.classificationField = 'updated_cases_per_capita'
    sym.renderer.breakCount = 10
    # for brk in sym.renderer.classBreaks:
    #     color = brk.symbol.color
    #     # brk.symbol.color = {'HSV': [cv, 100, 100, 100]}
    #     color['HSV'][-1] = 50
    #     # color = {'HSV': [0, 100, 100, 100]}
    #     brk.symbol.color = color

    lyr.symbology = sym

    aprx.saveACopy(aprx_global)

    print('Successfully updated symbology!')


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



remove_previous_files()
df_running_total, last_day, last_weeks = data_request(data_url)
add_data_to_map()
county_add()
set_map_symbology()
office_function()

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

# Update following variables to match
sd_fs_name = r'Covid 19 Cases per Capita _States_'
Map = r'Covid 19 Cases per Capita _States_'
portal = r'http://www.arcgis.com'
user = r'gtbaldwij5'
password = r'greenh2O!'

# Set sharing options
shrOrg = False
shrEveryone = False
shrGroups = ""

### End setting variables

# Local paths to create temporary content
relPath = os.path.dirname(prjPath)
sddraft = os.path.join(relPath, "WebUpdate.sddraft")
sd = os.path.join(relPath, "WebUpdate.sd")

#Create a new SDDraft and stage to SD
# Create a new SDDraft and stage to SD
print('Creating SD file')
arcpy.env.overwriteOutput = True
prj = arcpy.mp.ArcGISProject(prjPath)
mp = prj.listMaps()[0]
arcpy.mp.CreateWebLayerSDDraft(mp, sddraft, sd_fs_name, 'MY_HOSTED_SERVICES',
                               'FEATURE_ACCESS','',True, True)
arcpy.StageService_server(sddraft, sd)

print('Connecting to {}'.format(portal))
gis = GIS(portal, user, password)

# Find the SD, update it, publish with overwrite and set sharing and metadata
print('Searching for original SD on portal...')
sditem = gis.content.search('{} AND owner:{}'.format(sd_fs_name, user), item_type = 'Service Definition')[0]
print('Found SD:{}, ID:{}n Uploading and overwriting...'.format(sditem.title,sditem.id))
sditem.update(data=sd)
print('Overwriting existing feature service...')
fs = sditem.publish(overwrite=True)

if shrOrg or shrEveryone or shrGroups:
    print('Setting sharing options...')
    fs.share(org=shrOrg, everyone=shrEveryone, groups=shrGroups)

print('Finishing updating: {} - ID: {}'.format(fs.title, fs.id))

#####################

# Update following variables to match
sd_fs_name = r'Covid 19 Cases per Capita _Counties_'
Map = r'Covid 19 Cases per Capita _Counties_'
portal = r'http://www.arcgis.com'
user = r'gtbaldwij5'
password = r'greenh2O!'

# Set sharing options
shrOrg = False
shrEveryone = False
shrGroups = ""

### End setting variables

# Local paths to create temporary content
relPath = os.path.dirname(prjPath)
sddraft = os.path.join(relPath, "WebUpdate.sddraft")
sd = os.path.join(relPath, "WebUpdate.sd")

#Create a new SDDraft and stage to SD
# Create a new SDDraft and stage to SD
print('Creating SD file')
arcpy.env.overwriteOutput = True
prj = arcpy.mp.ArcGISProject(prjPath)
mp = prj.listMaps()[1]
arcpy.mp.CreateWebLayerSDDraft(mp, sddraft, sd_fs_name, 'MY_HOSTED_SERVICES',
                               'FEATURE_ACCESS','',True, True)
arcpy.StageService_server(sddraft, sd)

print('Connecting to {}'.format(portal))
gis = GIS(portal, user, password)

# Find the SD, update it, publish with overwrite and set sharing and metadata
print('Searching for original SD on portal...')
sditem = gis.content.search('{} AND owner:{}'.format(sd_fs_name, user), item_type = 'Service Definition')[0]
print('Found SD:{}, ID:{}n Uploading and overwriting...'.format(sditem.title,sditem.id))
sditem.update(data=sd)
print('Overwriting existing feature service...')
fs = sditem.publish(overwrite=True)

if shrOrg or shrEveryone or shrGroups:
    print('Setting sharing options...')
    fs.share(org=shrOrg, everyone=shrEveryone, groups=shrGroups)

print('Finishing updating: {} - ID: {}'.format(fs.title, fs.id))

##########################

# Update following variables to match
sd_fs_name = r'Grant Thornton Offices'
Map = r'Grant Thornton Offices'
portal = r'http://www.arcgis.com'
user = r'gtbaldwij5'
password = r'greenh2O!'

# Set sharing options
shrOrg = False
shrEveryone = False
shrGroups = ""

### End setting variables

# Local paths to create temporary content
relPath = os.path.dirname(prjPath)
sddraft = os.path.join(relPath, "WebUpdate.sddraft")
sd = os.path.join(relPath, "WebUpdate.sd")

#Create a new SDDraft and stage to SD
# Create a new SDDraft and stage to SD
print('Creating SD file')
arcpy.env.overwriteOutput = True
prj = arcpy.mp.ArcGISProject(prjPath)
mp = prj.listMaps()[2]
arcpy.mp.CreateWebLayerSDDraft(mp, sddraft, sd_fs_name, 'MY_HOSTED_SERVICES',
                               'FEATURE_ACCESS','',True, True)
arcpy.StageService_server(sddraft, sd)

print('Connecting to {}'.format(portal))
gis = GIS(portal, user, password)

# Find the SD, update it, publish with overwrite and set sharing and metadata
print('Searching for original SD on portal...')
sditem = gis.content.search('{} AND owner:{}'.format(sd_fs_name, user), item_type = 'Service Definition')[0]
print('Found SD:{}, ID:{}n Uploading and overwriting...'.format(sditem.title,sditem.id))
sditem.update(data=sd)
print('Overwriting existing feature service...')
fs = sditem.publish(overwrite=True)

if shrOrg or shrEveryone or shrGroups:
    print('Setting sharing options...')
    fs.share(org=shrOrg, everyone=shrEveryone, groups=shrGroups)

print('Finishing updating: {} - ID: {}'.format(fs.title, fs.id))

print('Done!')
