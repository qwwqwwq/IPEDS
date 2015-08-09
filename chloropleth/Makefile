all: gis/lower_48_contour.topojson

gis/cb_2014_us_state_20m.zip:
	wget -O gis/cb_2014_us_state_20m.zip "http://www2.census.gov/geo/tiger/GENZ2014/shp/cb_2014_us_state_20m.zip"

gis/gebco_08_rev_elev_A1_grey_geo.tif:
	wget -O gis/gebco_08_rev_elev_A1_grey_geo.tif "http://eoimages.gsfc.nasa.gov/images/imagerecords/73000/73934/gebco_08_rev_elev_A1_grey_geo.tif"

gis/gebco_08_rev_elev_B1_grey_geo.tif:
	wget -O gis/gebco_08_rev_elev_B1_grey_geo.tif "http://eoimages.gsfc.nasa.gov/images/imagerecords/73000/73934/gebco_08_rev_elev_B1_grey_geo.tif"

ipeds_data/hd2013.csv:
    wget -O ipeds_data/HD2013.zip "http://nces.ed.gov/ipeds/datacenter/data/HD2013.zip"
    cd ipeds_data
    unzip HD2013.zip
    touch hd2013.csv
    cd ..

java/target/pairwise-1.0-SNAPSHOT.jar:
    cd java
    mvn clean package
    cd ..

ipeds_data/institution_coordinates_data.csv: hd2013.csv
    python py/bin/gen_map_dataset.py ipeds_data/institution_coordinates_data.csv

gis/lower_48_dem.tif: us_lower_48_states.shp
	gdalwarp gis/gebco_08_rev_elev_AB_grey_geo.tif gis/gebco_08_rev_elev_B1_grey_geo.tif gis/gebco_08_rev_elev_A1_grey_geo.tif
	gdalwarp -tr 0.05 -0.05 -r average gis/gebco_08_rev_elev_AB_grey_geo.tif gis/gebco_08_rev_elev_AB_grey_geo_resample.tif
	gdalwarp -overwrite -of GTiff -cutline gis/us_lower_48_states.shp \
		-crop_to_cutline gis/gebco_08_rev_elev_AB_grey_geo_resample.tif \
		gis/gebco_08_rev_elev_AB_grey_geo_resample_crop.tif
	gdalwarp -t_srs EPSG:5070 gis/gebco_08_rev_elev_AB_grey_geo_resample_crop.tif gis/lower_48_dem.tif

gis/raster_coordinates_data.csv: lower_48_dem.tif
    python py/geotiff_to_csv.py gis/lower_48_dem.tif gis/raster_coordinates_data.csv

gis/raster_coordinates_reclassed.csv: gis/raster_coordinates_data.csv ipeds_data/institution_coordinates_data.csv java/target/pairwise-1.0-SNAPSHOT.jar
    python py/pairwise_distance_calc.py \
    	--raster_csv gis/raster_coordinates_data.csv \
    	--institution_csv ipeds_data/institution_coordinates_data.csv
    	--jar java/target/pairwise-1.0-SNAPSHOT.jar

gis/cb_2014_us_state_20m.shp: gis/cb_2014_us_state_20m.zip
	unzip cb_2014_us_state_20m.zip
	touch cb_2014_us_state_20m.shp

gis/cb_2014_us_state_20m_reproj.shp: gis/cb_2014_us_state_20m.shp
	ogr2ogr -t_srs EPSG:5070 gis/cb_2014_us_state_20m_reproj.shp gis/cb_2014_us_state_20m.shp

gis/us_lower_48_states.shp: gis/cb_2014_us_state_20m_reproj.shp
	ogr2ogr -where "STUSPS != 'AK' AND STUSPS != 'HI' AND STUSPS != 'PR'" gis/cb_2014_us_state_20m_reproj_48.shp gis/us_lower_48_states.shp

gis/levels.shp: lower_48_dem_reclassed.tif
	for level in 1 2 3 4 5 7 9 10 12 15 20 25 30 35 40 50 ; do \
        gdal_calc.py -A gis/lower_48_dem_reclassed.tif --outfile=gis/level${level}.tif --calc="${level}*(A>${level})" --NoDataValue=0 && \
        gdal_polygonize.py gis/level${level}.tif -f "ESRI Shapefile" gis/level${level}.shp contour elevation && \
        ogr2ogr -update -append gis/levels.shp gis/level${level}.shp ; \
    done

gis/lower_48_contour.topojson: levels.shp
	topojson -p -o gis/lower_48_contour.topojson --width 720 gis/levels.shp
