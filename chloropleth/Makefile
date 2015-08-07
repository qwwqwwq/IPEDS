all: lower_48_contour.topojson

#lower_48: lower_48_contour.topojson

#cb_2014_us_state_20m.zip:
#	wget "http://www2.census.gov/geo/tiger/GENZ2014/shp/cb_2014_us_state_20m.zip"

#gebco_08_rev_elev_A1_grey_geo.tif:
#	wget "http://eoimages.gsfc.nasa.gov/images/imagerecords/73000/73934/gebco_08_rev_elev_A1_grey_geo.tif"

#gebco_08_rev_elev_B1_grey_geo.tif:
#	wget "http://eoimages.gsfc.nasa.gov/images/imagerecords/73000/73934/gebco_08_rev_elev_B1_grey_geo.tif"

lower_48_dem.tif:
#	gdalwarp gebco_08_rev_elev_AB_grey_geo.tif gebco_08_rev_elev_B1_grey_geo.tif gebco_08_rev_elev_A1_grey_geo.tif
#	gdal_translate -projwin -130.946280992 53.1446280992 -59.9876033058 20.7892561983 gebco_08_rev_elev_AB_grey_geo.tif lower_48_full_dem.tif
#	gdalwarp -tr 0.03 -0.03 -r average lower_48_full_dem.tif lower_48_small_dem.tif
	gdalwarp -overwrite -of GTiff -cutline us_lower_48_outline.shp -crop_to_cutline lower_48_dem_s_reclass.tif lower_48_dem_s_reclass_c.tif
	gdalwarp -t_srs EPSG:5070 lower_48_dem_s_reclass_c.tif lower_48_dem.tif


#cb_2014_us_state_20m.shp: cb_2014_us_state_20m.zip
#	unzip cb_2014_us_state_20m.zip
#	touch cb_2014_us_state_20m.shp

#cb_2014_us_state_20m_reproj.shp: cb_2014_us_state_20m.shp
#	ogr2ogr -t_srs EPSG:4326 cb_2014_us_state_20m_reproj.shp cb_2014_us_state_20m.shp

#cb_2014_us_state_20m_reproj_48.shp: cb_2014_us_state_20m_reproj.shp
#	ogr2ogr -where "STUSPS != 'AK' AND STUSPS != 'HI' AND STUSPS != 'PR'" cb_2014_us_state_20m_reproj_48.shp cb_2014_us_state_20m_reproj.shp

level1.shp: lower_48_dem.tif
	gdal_calc.py -A lower_48_dem.tif --outfile=level1.tif --calc="0*(A>0)" --NoDataValue=0
	gdal_polygonize.py level1.tif -f "ESRI Shapefile" level1.shp contour elevation

level5.shp: lower_48_dem.tif
	gdal_calc.py -A lower_48_dem.tif --outfile=level5.tif --calc="5*(A>5)" --NoDataValue=0
	gdal_polygonize.py level5.tif -f "ESRI Shapefile" level5.shp contour elevation

level10.shp: lower_48_dem.tif
	gdal_calc.py -A lower_48_dem.tif --outfile=level10.tif --calc="10*(A>10)" --NoDataValue=0
	gdal_polygonize.py level10.tif -f "ESRI Shapefile" level10.shp contour elevation

level20.shp: lower_48_dem.tif
	gdal_calc.py -A lower_48_dem.tif --outfile=level20.tif --calc="20*(A>20)" --NoDataValue=0
	gdal_polygonize.py level20.tif -f "ESRI Shapefile" level20.shp contour elevation

level30.shp: lower_48_dem.tif
	gdal_calc.py -A lower_48_dem.tif --outfile=level30.tif --calc="30*(A>30)" --NoDataValue=0
	gdal_polygonize.py level30.tif -f "ESRI Shapefile" level30.shp contour elevation

level40.shp: lower_48_dem.tif
	gdal_calc.py -A lower_48_dem.tif --outfile=level40.tif --calc="40*(A>40)" --NoDataValue=0
	gdal_polygonize.py level40.tif -f "ESRI Shapefile" level40.shp contour elevation

level50.shp: lower_48_dem.tif
	gdal_calc.py -A lower_48_dem.tif --outfile=level50.tif --calc="50*(A>50)" --NoDataValue=0
	gdal_polygonize.py level50.tif -f "ESRI Shapefile" level50.shp contour elevation

levels.shp: level1.shp level5.shp level10.shp level20.shp level30.shp level40.shp level50.shp
	ogr2ogr levels.shp level1.shp
	ogr2ogr -update -append levels.shp level5.shp
	ogr2ogr -update -append levels.shp level10.shp
	ogr2ogr -update -append levels.shp level20.shp
	ogr2ogr -update -append levels.shp level30.shp
	ogr2ogr -update -append levels.shp level40.shp
	ogr2ogr -update -append levels.shp level50.shp

lower_48_contour.topojson: levels.shp
	topojson -p -o lower_48_contour.topojson --width 720 levels.shp
