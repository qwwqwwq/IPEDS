import csv
import gdal
import numpy
import argparse

from itertools import product

def coords(x_size, y_size, x_min, y_min, x_step, y_step):
    cartesian_iterator = product(numpy.arange(x_size), numpy.arange(y_size))
    for x, y in cartesian_iterator:
        yield (x_min + (x * x_step), y_min + (y * y_step))

def geotiff_to_csv(geotiff, output):
    i = gdal.Open(geotiff)
    x_size = i.RasterXSize
    y_size = i.RasterYSize
    x_min, x_step_x, x_step_y, y_min, y_step_x, y_step_y = i.GetGeoTransform()

    with open(output, 'w') as of:
        writer = csv.writer(of)
        for coord in coords(x_size, y_size, x_min, y_min, x_step_x, y_step_y):
            writer.writerow(coord)

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('geotiff_file')
    parser.add_argument('output_file')
    return parser.parse_args()

if __name__ == '__main__':
    args = get_args()
    geotiff_to_csv(args.geotiff_file, args.output_file)




