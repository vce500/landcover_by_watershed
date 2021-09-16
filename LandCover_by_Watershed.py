"""
NLCD LANDCOVER, BY WATERSHED

This tool tabulates the percent of land cover types within each watershed polygon.

It enables the user to remap the standard NLCD land cover types to more dissolved
classes. This is done separately in a CSV that the user must point to.

TODO:
Implement intermediate datasets that remain in memory. Issues arise with maintaining
the raster attributes (class names), which means the cursors won't work when reading
the RasterToPolygon output. Additionally, the land cover mapping won't work without
the original class names. Runtime for 90 watersheds in Anne Arundel County

VEdwards 20210908
"""
#---------------------------------------------------------------------------------------#
# IMPORTS
import arcpy as ap
import csv
from datetime import datetime

import sys
import traceback

#---------------------------------------------------------------------------------------#
# SETTINGS
ap.env.overwriteOutput = True

# NOT YET IMPLEMENTED, CURRENTLY ALL INTERMEDIATE WILL SAVE.
save_intermediate = False
#---------------------------------------------------------------------------------------#
# INPUTS
# USUALLY UPDATED:
# Geodatabase for all outputs.
# r'C:\Users\VinceE\Desktop\NLCD_Watershed\NLCD_LandCover_Watershed\NLCD_LandCover_Watershed.gdb'
outgdb = ap.GetParameter(0)

# Path to feature class of watersheds. Can be nested, or not.
# r'C:\Users\VinceE\Desktop\NLCD_Watershed\NLCD_LandCover_Watershed\NLCD_LandCover_Watershed.gdb\Example_Watershed'
wshed_poly = ap.GetParameter(1)

# CSV that maps all NLCD classes to new grouped land cover classes.
# For example, various forest types can all be mapped to a singular "Forest" class.
# r'C:\Users\VinceE\Desktop\NLCD_Watershed\NLCD_Cover_Mapping.csv'
cover_mapping_csv = ap.GetParameterAsText(2)

# USUALLY UNCHANGED:
# URL to Esri's hosted NLCD dataset; NLCD class name field; dissolved class field.
# r'https://landscape10.arcgis.com/arcgis/services/USA_NLCD_Land_Cover/ImageServer'
# {Esri_ImageService:'ClassName', NLCD Download: 'NLCD_Land_Cover_Class'}
# 'NewClass'
nlcd_rast = ap.GetParameterAsText(3)
nlcd_class_field = ap.GetParameterAsText(4)
new_class_field = ap.GetParameterAsText(5)

#---------------------------------------------------------------------------------------#
# DERIVED INPUTS
today = datetime.today().strftime('%Y%m%d')

# Read a CSV to get the new land cover classification groupings.
with open(cover_mapping_csv, mode='r') as infile:
    reader = csv.reader(infile)
    class_map = {row[0]:row[1] for row in reader}

"""
if save_intermediate:
    wspace = outgdb
else:
    wspace = 'memory'
"""
#---------------------------------------------------------------------------------------#
# MAIN
try:
    # Buffer the watersheds and then clip the NLCD raster.
    # This ensures full cell coverage from the NLCD raster.
    wshed_buff = ap.analysis.Buffer(
                        in_features=wshed_poly,
                        out_feature_class=fr'memory\wshed_buffer_500ft',
                        buffer_distance_or_field='500 FEET')

    nlcd_clip = ap.management.Clip(
                        in_raster=nlcd_rast,
                        rectangle='#',
                        out_raster=fr'{outgdb}\Temp_NLCD_WShed_Clip',
                        in_template_dataset=wshed_buff,
                        nodata_value='#',
                        clipping_geometry='ClippingGeometry')
    ap.AddMessage('NLCD dataset clipped to watersheds.')

    # Convert the clipped NLCD to a polygon feature class.
    nlcd_poly = ap.conversion.RasterToPolygon(
                        in_raster=nlcd_clip,
                        out_polygon_features=fr'{outgdb}\Temp_NLCD_Polygon',
                        simplify='NO_SIMPLIFY',
                        raster_field=nlcd_class_field,
                        create_multipart_features='MULTIPLE_OUTER_PART')
    ap.AddMessage('NLCD raster converted to multipart polygon feature class.')

    # Add a field that will contain the new class name.
    ap.management.AddField(
                        in_table=nlcd_poly,
                        field_name=new_class_field,
                        field_type='TEXT',
                        field_length=50)

    # Using mapping dictionary, calculate new class name from original NLCD.
    with ap.da.UpdateCursor(nlcd_poly, [nlcd_class_field, new_class_field]) as ucurs:
        for old_class, new_class in ucurs:
            new_class = class_map[old_class]
            ucurs.updateRow([old_class, new_class])
    ap.AddMessage('NLCD classes have been grouped to new values.')

    # Create a summary table that contains a row for each land cover type and
    #       the percent cover for each feature class shape.
    # A JOIN_ID is also added to the watersheds so that the table can be joined.
    shed_sum = ap.analysis.SummarizeWithin(
                        in_polygons=wshed_poly,
                        in_sum_features=nlcd_poly,
                        out_feature_class=fr'{outgdb}\Temp_Watershed_Summary',
                        keep_all_polygons='KEEP_ALL',
                        sum_fields='#',
                        sum_shape='ADD_SHAPE_SUM',
                        shape_unit='SQUAREFEET',
                        group_field=new_class_field,
                        add_min_maj='NO_MIN_MAJ',
                        add_group_percent='ADD_PERCENT',
                        out_group_table=fr'{outgdb}\Temp_Watershed_Stat_Table')
    ap.AddMessage('Percent cover by type and by watershed have been computed.')

    # Join the statistics table back the watersheds. Each single watershed will
    #       become multiple rows, as each watershed likely has more than one
    #       land cover type.
    shed_stats_table = ap.management.AddJoin(
                        in_layer_or_view=shed_sum,
                        in_field='Join_ID',
                        join_table=fr'{outgdb}\Temp_Watershed_Stat_Table',
                        join_field='Join_ID')

    # Copy the temporarily joined tables to a new feature class.
    # There will be lots of duplicate shapes, because there will be a different
    #       shape for each land cover type.
    # The attributes of this table is what the user will most likely copy to Excel.
    # This step isn't necessary, it just makes it easier to visually inpect the
    #       results when the geometry is reattached, versus looking only at a table.
    input_wshedfc_name = ap.da.Describe(wshed_poly)['baseName']
    shed_stat_table = ap.conversion.FeatureClassToFeatureClass(
                        in_features=shed_stats_table,
                        out_path=outgdb,
                        out_name=f'Watershed_{input_wshedfc_name}_Statistics_{today}')
    ap.AddMessage('Generating clean output.')

    # Delete field that are not necessary.
    ap.management.DeleteField(
                        in_table=shed_stat_table,
                        drop_field=['sum_Area_SQUAREFEET', 'Polygon_Count',
                                    'Join_ID', 'OBJECTID', 'Join_ID_1',
                                    'sum_Area_SQUAREFEET_1', 'Polygon_Count_1'])
    ap.AddMessage('Removing superfluous fields.')

except KeyError:
    ap.AddError('An NLCD class in the data was not found in the CSV, so '
                'reclassficiation could not occur. Ensure CSV is up to date.')
except:
    etype, evalue, tback = sys.exc_info()
    tback_info = traceback.format_tb(tback)[0]
    err_msg = (f"Traceback Info:\n{tback_info}\n{etype.__name__}: {evalue}")
    ap.AddError(err_msg)
