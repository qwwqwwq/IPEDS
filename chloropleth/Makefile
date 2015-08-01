all: north_america lower_48

lower_48: lower_48_contour.topojson

north_america: north_america_contour.topojson

cb_2014_us_state_20m.zip:
	wget "http://www2.census.gov/geo/tiger/GENZ2014/shp/cb_2014_us_state_20m.zip"

gebco_08_rev_elev_A1_grey_geo.tif:
	wget "http://eoimages.gsfc.nasa.gov/images/imagerecords/73000/73934/gebco_08_rev_elev_A1_grey_geo.tif"

gebco_08_rev_elev_B1_grey_geo.tif:
	wget "http://eoimages.gsfc.nasa.gov/images/imagerecords/73000/73934/gebco_08_rev_elev_B1_grey_geo.tif"

cb_2014_us_state_20m.shp: cb_2014_us_state_20m.zip
	unzip cb_2014_us_state_20m.zip
	touch cb_2014_us_state_20m.shp

cb_2014_us_state_20m_reproj.shp: cb_2014_us_state_20m.shp
	ogr2ogr -t_srs EPSG:4326 cb_2014_us_state_20m_reproj.shp cb_2014_us_state_20m.shp

cb_2014_us_state_20m_reproj_48.shp: cb_2014_us_state_20m_reproj.shp
	ogr2ogr -where "STUSPS != 'AK' AND STUSPS != 'HI' AND STUSPS != 'PR'" cb_2014_us_state_20m_reproj_48.shp cb_2014_us_state_20m_reproj.shp

gebco_08_rev_elev_AB_grey_geo_cropped.tif: gebco_08_rev_elev_AB_grey_geo.tif cb_2014_us_state_20m_reproj_48.shp
	gdalwarp -overwrite -of GTiff -cutline cb_2014_us_state_20m_reproj_48.shp -crop_to_cutline \
		gebco_08_rev_elev_AB_grey_geo.tif \
		gebco_08_rev_elev_AB_grey_geo_cropped.tif

level1.shp: gebco_08_rev_elev_AB_grey_geo_cropped
    gdal_calc.py -A gebco_08_rev_elev_AB_grey_geo_cropped.tif --outfile=level1.tif --calc="1*(A>0)" --NoDataValue=0
    gdal_polygonize.py level1.tif -f "ESRI Shapefile" level1.shp level_1 elevation

level3.shp: gebco_08_rev_elev_AB_grey_geo_cropped
    gdal_calc.py -A gebco_08_rev_elev_AB_grey_geo_cropped.tif --outfile=level3.tif --calc="3*(A>3)" --NoDataValue=0
    gdal_polygonize.py level3.tif -f "ESRI Shapefile" level3.shp level_3 elevation

level7.shp: gebco_08_rev_elev_AB_grey_geo_cropped
    gdal_calc.py -A gebco_08_rev_elev_AB_grey_geo_cropped.tif --outfile=level7.tif --calc="7*(A>7)" --NoDataValue=0
    gdal_polygonize.py level7.tif -f "ESRI Shapefile" level7.shp level_7 elevation

level10.shp: gebco_08_rev_elev_AB_grey_geo_cropped
    gdal_calc.py -A gebco_08_rev_elev_AB_grey_geo_cropped.tif --outfile=level10.tif --calc="10*(A>10)" --NoDataValue=0
    gdal_polygonize.py level10.tif -f "ESRI Shapefile" level10.shp level_10 elevation

level20.shp: gebco_08_rev_elev_AB_grey_geo_cropped
    gdal_calc.py -A gebco_08_rev_elev_AB_grey_geo_cropped.tif --outfile=level20.tif --calc="20*(A>20)" --NoDataValue=0
    gdal_polygonize.py level20.tif -f "ESRI Shapefile" level20.shp level_20 elevation

lower_48_contour.topojson: level1.shp level3.shp level7.shp level10.shp level20.shp
    topojson -p -o lower_48_contour --projection 'd3.geo.albers()' --width 720 \
        level1.shp level3.shp level7.shp level10.shp level20.shp
