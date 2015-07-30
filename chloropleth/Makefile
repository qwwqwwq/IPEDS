cb_2014_us_state_20m.zip:
    wget "http://www2.census.gov/geo/tiger/GENZ2014/shp/cb_2014_us_state_20m.zip"

gebco_08_rev_elev_A1_grey_geo.tif:
    wget "http://eoimages.gsfc.nasa.gov/images/imagerecords/73000/73934/gebco_08_rev_elev_A1_grey_geo.tif"

gebco_08_rev_elev_A2_grey_geo.tif:
    wget "http://eoimages.gsfc.nasa.gov/images/imagerecords/73000/73934/gebco_08_rev_elev_B1_grey_geo.tif"

cb_2014_us_state_20m.shp: cb_2014_us_state_20m.zip
    unzip cb_2014_us_state_20m.zip
    touch cb_2014_us_state_20m.shp

cb_2014_us_state_20m_48.shp: cb_2014_us_state_20m.shp
    ogr2ogr -where "STUSPS != 'AK' AND STUSPS != 'HI' AND STUSPS != 'PR'" cb_2014_us_state_20m_48.shp cb_2014_us_state_20m.shp

gebco_08_rev_elev_AB_grey_geo.tif: gebco_08_rev_elev_A1_grey_geo.tif gebco_08_rev_elev_B1_grey_geo.tif
    gdalwarp gebco_08_rev_elev_A1_grey_geo.tif gebco_08_rev_elev_B1_grey_geo.tif gebco_08_rev_elev_AB_grey_geo.tif

gebco_08_rev_elev_AB_grey_geo_resampled.tif: gebco_08_rev_elev_AB_grey_geo.tif
    gdalwarp -tr 0.1 -0.1 -r average gebco_08_rev_elev_AB_grey_geo.tif gebco_08_rev_elev_AB_grey_geo_resampled.tif

gebco_08_rev_elev_AB_grey_geo_48_resampled_mercador.tif: gebco_08_rev_elev_AB_grey_geo_resampled.tif
    gdalwarp -t_srs EPSG:3857 gebco_08_rev_elev_AB_grey_geo_48_resampled.tif gebco_08_rev_elev_AB_grey_geo_48_resampled_mercador.tif

gebco_08_rev_elev_AB_grey_geo_48_resampled_mercador_cropped.tif: gebco_08_rev_elev_AB_grey_geo_48_resampled_mercador.tif cb_2014_us_state_20m_48.shp
    gdalwarp -overwrite -of GTiff -cutline cb_2014_us_state_20m_48.shp -crop_to_cutline \
        gebco_08_rev_elev_AB_grey_geo_48_resampled_mercador.tif \
        gebco_08_rev_elev_AB_grey_geo_48_resampled_mercador_cropped.tif

gebco_fl/contour.shp: gebco_08_rev_elev_AB_grey_geo_48_resampled_mercador.tif
    gdal_contour -a elevation  -fl 1 3 8 12 18 27 40 57 75 100 \
        gebco_08_rev_elev_AB_grey_geo_48_resampled_mercador.tif gebco_fl

contour.topojson: gebco_fl/contour.shp
    topojson -p -o contour.topojson  --width 780 gebco_fl/contour.shp