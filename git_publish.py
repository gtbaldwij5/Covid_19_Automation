#Author: Jack Baldwin


import sys, os, tempfile, json, logging, arcpy, fnmatch, shutil, subprocess, arcgis
from arcgis.gis import GIS
from arcgis.mapping import WebMap

### Start setting variables
# Set the path to the project
prjPath =# r"Path\To\Project.aprx"

# Update following variables to match
sd_fs_name = # r'Service Definition Feature Service Name'
Map = # r'Web Map Feature Service Name'
portal = r'http://www.arcgis.com'
user = # r'USERNAME'
password = # r'PASSWORD'

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




# search_response = gis.content.search('{} AND owner:{}'.format(Map, user), item_type = 'Web Map')[0]
#
# print('Found SD:{}, ID:{}n Uploading and overwriting...'.format(search_response.title,search_response.id))
#
# print(search_response)
#
# state_map = WebMap(search_response)
#
# for layer in state_map.layers:
#     print(layer.title)
#
# state_map.remove_layer(state_map.layers[0])
#
#
# state_item = gis.content.get(fs.id)
#
# state_map.add_layer(state_item, options={'title':'statesLyr'})
#
# wm = state_map.publish(overwrite=True)
#
# if shrOrg or shrEveryone or shrGroups:
#     print('Setting sharing options...')
#     wm.share(org=shrOrg, everyone=shrEveryone, groups=shrGroups)
#
# print('Finishing updating: {} - ID: {}'.format(wm.title, wm.id))
