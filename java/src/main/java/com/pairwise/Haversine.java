package com.pairwise;

/**
 * Calculate great circle distance between two lat/long coordinates
 */
public class Haversine {

    private static final Double EARTH_RADIUS = 6371.0;

    /**
     * Distance between two points measured in kilometers
     */
    public static Double dist(Coordinate a, Coordinate b) {
        Double dLongitude = Math.toRadians(b.getLongitude()) - Math.toRadians(a.getLongitude());
        Double dLatitude = Math.toRadians(b.getLatitude()) - Math.toRadians(a.getLatitude());

        double arc = Math.pow(Math.sin(dLatitude / 2), 2) +
                Math.cos(Math.toRadians(a.getLatitude())) *
                Math.cos(Math.toRadians(b.getLatitude())) *
                Math.pow(Math.sin(dLongitude / 2), 2);
        double c = 2 * Math.asin(Math.sqrt(arc));
        return c * EARTH_RADIUS;
    }
}
