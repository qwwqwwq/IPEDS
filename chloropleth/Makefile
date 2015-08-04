all: levels.shp

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
	gdalwarp -overwrite -of GTiff -cutline us_lower_48_outline.shp -crop_to_cutline gebco_08_rev_elev_AB_grey_geo_resampled.tif lower_48_dem_s.tif
	gdalwarp -t_srs EPSG:5070 lower_48_dem_s.tif lower_48_dem.tif


#cb_2014_us_state_20m.shp: cb_2014_us_state_20m.zip
#	unzip cb_2014_us_state_20m.zip
#	touch cb_2014_us_state_20m.shp

#cb_2014_us_state_20m_reproj.shp: cb_2014_us_state_20m.shp
#	ogr2ogr -t_srs EPSG:4326 cb_2014_us_state_20m_reproj.shp cb_2014_us_state_20m.shp

#cb_2014_us_state_20m_reproj_48.shp: cb_2014_us_state_20m_reproj.shp
#	ogr2ogr -where "STUSPS != 'AK' AND STUSPS != 'HI' AND STUSPS != 'PR'" cb_2014_us_state_20m_reproj_48.shp cb_2014_us_state_20m_reproj.shp

level1.shp: lower_48_dem.tif
	gdal_calc.py -A lower_48_dem.tif --outfile=level1.tif --calc="1*(A>0)" --NoDataValue=0
	gdal_polygonize.py level1.tif -f "ESRI Shapefile" level1.shp contour elevation

level3.shp: lower_48_dem.tif
	gdal_calc.py -A lower_48_dem.tif --outfile=level3.tif --calc="3*(A>3)" --NoDataValue=0
	gdal_polygonize.py level3.tif -f "ESRI Shapefile" level3.shp contour elevation

level7.shp: lower_48_dem.tif
	gdal_calc.py -A lower_48_dem.tif --outfile=level7.tif --calc="7*(A>7)" --NoDataValue=0
	gdal_polygonize.py level7.tif -f "ESRI Shapefile" level7.shp contour elevation

level10.shp: lower_48_dem.tif
	gdal_calc.py -A lower_48_dem.tif --outfile=level10.tif --calc="10*(A>10)" --NoDataValue=0
	gdal_polygonize.py level10.tif -f "ESRI Shapefile" level10.shp contour elevation

level20.shp: lower_48_dem.tif
	gdal_calc.py -A lower_48_dem.tif --outfile=level20.tif --calc="20*(A>20)" --NoDataValue=0
	gdal_polygonize.py level20.tif -f "ESRI Shapefile" level20.shp contour elevation

levels.shp: level1.shp level3.shp level7.shp level10.shp level20.shp
	ogr2ogr levels.shp level1.shp
	ogr2ogr -update -append levels.shp level3.shp
	ogr2ogr -update -append levels.shp level7.shp
	ogr2ogr -update -append levels.shp level10.shp
	ogr2ogr -update -append levels.shp level20.shp


#levels_clipped.shp: levels.shp cb_2014_us_state_20m_reproj_48.shp
#	ogr2ogr -clipsrc cb_2014_us_state_20m_reproj_48.shp levels_clipped.shp levels.shp

lower_48_contour.topojson: levels.shp
	topojson -p -o lower_48_contour.topojson --projection 'd3.geo.albers()' --width 720 levels.shp
