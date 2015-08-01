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

gebco_08_rev_elev_AB_grey_geo.tif: gebco_08_rev_elev_A1_grey_geo.tif gebco_08_rev_elev_B1_grey_geo.tif
	gdalwarp gebco_08_rev_elev_A1_grey_geo.tif gebco_08_rev_elev_B1_grey_geo.tif gebco_08_rev_elev_AB_grey_geo.tif

gebco_08_rev_elev_AB_grey_geo_resampled.tif: gebco_08_rev_elev_AB_grey_geo.tif
	gdalwarp -tr 0.05 -0.05 -r average gebco_08_rev_elev_AB_grey_geo.tif gebco_08_rev_elev_AB_grey_geo_resampled.tif

gebco_08_rev_elev_AB_grey_geo_resampled_1.tif: gebco_08_rev_elev_AB_grey_geo.tif
	gdalwarp -tr 0.1 -0.1 -r average gebco_08_rev_elev_AB_grey_geo.tif gebco_08_rev_elev_AB_grey_geo_resampled_1.tif

gebco_08_rev_elev_AB_grey_geo_resampled_cropped.tif: gebco_08_rev_elev_AB_grey_geo_resampled.tif cb_2014_us_state_20m_reproj_48.shp
	gdalwarp -overwrite -of GTiff -cutline cb_2014_us_state_20m_reproj_48.shp -crop_to_cutline \
		gebco_08_rev_elev_AB_grey_geo_resampled.tif \
		gebco_08_rev_elev_AB_grey_geo_resampled_cropped.tif

gebco_08_rev_elev_AB_grey_geo_cropped.tif: gebco_08_rev_elev_AB_grey_geo.tif cb_2014_us_state_20m_reproj_48.shp
	gdalwarp -overwrite -of GTiff -cutline cb_2014_us_state_20m_reproj_48.shp -crop_to_cutline \
		gebco_08_rev_elev_AB_grey_geo.tif \
		gebco_08_rev_elev_AB_grey_geo_cropped.tif

full_res_lower_48/contour.shp: gebco_08_rev_elev_AB_grey_geo_cropped.tif
	gdal_contour -a elevation -fl 1 3 5 7 10 20 30 40 50 75 100 gebco_08_rev_elev_AB_grey_geo_cropped.tif full_res_lower_48

full_res_lower_48_contour.topojson: full_res_lower_48/contour.shp
	topojson -s -p -o full_res_lower_48_contour.topojson --projection 'd3.geo.albers()' --width 720 full_res_lower_48/contour.shp

lower_48/contour.shp: gebco_08_rev_elev_AB_grey_geo_resampled_cropped.tif
	gdal_contour -a elevation -fl 1 3 5 7 10 20 30 40 50 75 100 gebco_08_rev_elev_AB_grey_geo_resampled_cropped.tif lower_48

lower_48_contour.topojson: lower_48/contour.shp
	topojson -p -o lower_48_contour.topojson --projection 'd3.geo.albers()' --width 720 lower_48/contour.shp

north_america/contour.shp: gebco_08_rev_elev_AB_grey_geo_resampled_1.tif
	gdal_contour -a elevation -fl 1 3 5 7 10 20 30 50 75 100 gebco_08_rev_elev_AB_grey_geo_resampled_1.tif north_america

north_america_contour.topojson: north_america/contour.shp
	topojson -p -o north_america_contour.topojson --projection 'd3.geo.cylindricalStereographic()' --width 720 north_america/contour.shp


