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

### Set Global Parameters
arcpy.env.workspace = # r"Path\To\Geodatabase.gdb"
gdb = # r"Path\To\Geodatabase.gdb"
project_folder = # r"Path\To\Project\Folder"
aprx_global = # r"Path\To\Project.aprx"
print(arcpy.env.workspace)
data_url = 'https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_confirmed_usafacts.csv'



def remove_previous_files():
    print('Removing previous files...')

    os.remove( gdb + r'\last_state.lyrx')
    os.remove( gdb + r'\last_county.lyrx')
    os.remove( gdb + r'\last_office.lyrx')
    os.remove( gdb + r'\covid19_last_day.csv')
    os.remove( gdb + r'\county_covid19_total.csv')
    os.remove( gdb + r'\county_covid19_last_day.csv')
    os.remove( gdb + r'\county_covid19_last_weeks.csv')
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








def add_data_to_map():

    print('Beginning to add state level data to geodatabase...')

    arcpy.TableToTable_conversion("county_covid19_total.csv", arcpy.env.workspace, "county_total", field_mapping = "FIRST")

    df_last_day = df_running_total[['countyFIPS', 'County Name', 'State', 'stateFIPS', last_day]]

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

    df_last_weeks = df_running_total[['countyFIPS', 'County Name', 'State', 'stateFIPS', base, day_1, day_2, day_3, day_4,
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
    df_last_weeks_day = df_last_weeks[['countyFIPS', 'County Name', 'State', 'stateFIPS', 'total_weeks']]

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
            x[2] = x[0]/x[1]
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
    sym.renderer.classificationField = 'updated_cases_per_capita'
    sym.renderer.breakCount = 10
    for brk in sym.renderer.classBreaks:
        color = brk.symbol.color
        color['HSV'][-1] = 50
        brk.symbol.color = color

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
            x[2] = x[0]/x[1]
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

    sym.updateRenderer('GraduatedColorsRenderer')
    sym.renderer.classificationField = 'updated_cases_per_capita'
    sym.renderer.breakCount = 10
    for brk in sym.renderer.classBreaks:
        color = brk.symbol.color
        color['HSV'][-1] = 50
        brk.symbol.color = color

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

    sym.updateRenderer('GraduatedColorsRenderer')
    sym.renderer.classificationField = 'updated_cases_per_capita'
    sym.renderer.breakCount = 10
    for brk in sym.renderer.classBreaks:
        color = brk.symbol.color
        # brk.symbol.color = {'HSV': [cv, 100, 100, 100]}
        color['HSV'][-1] = 50
        # color = {'HSV': [0, 100, 100, 100]}
        brk.symbol.color = color

    lyr.symbology = sym

    aprx.saveACopy(aprx_global)

    print('Successfully updated symbology!')






remove_previous_files()
df_running_total, last_day, last_weeks = data_request(data_url)
add_data_to_map()
county_add()
set_map_symbology()
office_function()

print('Done!')
