"""
Coordinate conversion utilities.

OSGB36 (British National Grid) to WGS84 (GPS) conversion.
"""

import math
from typing import Tuple, Optional

from .config import GRID_SIZE


def osgb36_to_wgs84(easting: float, northing: float) -> Tuple[Optional[float], Optional[float]]:
    """
    Convert OSGB36 coordinates (British National Grid) to WGS84 (lat/lon).
    
    This is an approximate conversion using the Helmert transformation.
    Accuracy is typically within a few meters for UK locations.
    
    Args:
        easting: OSGB36 easting in meters
        northing: OSGB36 northing in meters
        
    Returns:
        Tuple of (latitude, longitude) in decimal degrees, or (None, None) on error
    """
    try:
        E = float(easting)
        N = float(northing)
    except (ValueError, TypeError):
        return None, None
    
    # Airy 1830 ellipsoid parameters
    a = 6377563.396  # Semi-major axis
    b = 6356256.909  # Semi-minor axis
    F0 = 0.9996012717  # Scale factor on central meridian
    
    # True origin
    lat0 = math.radians(49)   # 49°N
    lon0 = math.radians(-2)   # 2°W
    
    # False origin
    N0 = -100000
    E0 = 400000
    
    # Eccentricity squared
    e2 = 1 - (b * b) / (a * a)
    n = (a - b) / (a + b)
    
    # Iterative latitude calculation
    lat = lat0
    M = 0
    
    max_iterations = 100
    for _ in range(max_iterations):
        lat_prev = lat
        
        # Meridional arc
        M = b * F0 * (
            (1 + n + 5/4 * n**2 + 5/4 * n**3) * (lat - lat0)
            - (3*n + 3*n**2 + 21/8 * n**3) * math.sin(lat - lat0) * math.cos(lat + lat0)
            + (15/8 * n**2 + 15/8 * n**3) * math.sin(2*(lat - lat0)) * math.cos(2*(lat + lat0))
            - (35/24 * n**3) * math.sin(3*(lat - lat0)) * math.cos(3*(lat + lat0))
        )
        
        lat = (N - N0 - M) / (a * F0) + lat
        
        if abs(lat - lat_prev) < 1e-10:
            break
    
    # Calculate transverse radius of curvature
    sin_lat = math.sin(lat)
    cos_lat = math.cos(lat)
    tan_lat = math.tan(lat)
    
    nu = a * F0 / math.sqrt(1 - e2 * sin_lat**2)
    rho = a * F0 * (1 - e2) / ((1 - e2 * sin_lat**2)**1.5)
    eta2 = nu / rho - 1
    
    sec_lat = 1 / cos_lat
    dE = E - E0
    
    # Taylor series coefficients
    VII = tan_lat / (2 * rho * nu)
    VIII = tan_lat / (24 * rho * nu**3) * (5 + 3*tan_lat**2 + eta2 - 9*tan_lat**2*eta2)
    IX = tan_lat / (720 * rho * nu**5) * (61 + 90*tan_lat**2 + 45*tan_lat**4)
    
    X = sec_lat / nu
    XI = sec_lat / (6 * nu**3) * (nu/rho + 2*tan_lat**2)
    XII = sec_lat / (120 * nu**5) * (5 + 28*tan_lat**2 + 24*tan_lat**4)
    XIIA = sec_lat / (5040 * nu**7) * (61 + 662*tan_lat**2 + 1320*tan_lat**4 + 720*tan_lat**6)
    
    # Calculate latitude and longitude
    lat_final = lat - VII*dE**2 + VIII*dE**4 - IX*dE**6
    lon_final = lon0 + X*dE - XI*dE**3 + XII*dE**5 - XIIA*dE**7
    
    return math.degrees(lat_final), math.degrees(lon_final)


def determine_region_from_coords(easting: float, northing: float) -> Optional[str]:
    """
    Determine region identifier from coordinates using grid squares.
    
    Creates a region code like "E480000_N240000" based on GRID_SIZE.
    
    Args:
        easting: OSGB36 easting in meters
        northing: OSGB36 northing in meters
        
    Returns:
        Region string or None on error
    """
    try:
        e = int(float(easting))
        n = int(float(northing))
        
        # Round down to nearest grid square
        e_grid = (e // GRID_SIZE) * GRID_SIZE
        n_grid = (n // GRID_SIZE) * GRID_SIZE
        
        return f"E{e_grid}_N{n_grid}"
    except (ValueError, TypeError):
        return None


def safe_float(value) -> Optional[float]:
    """
    Safely convert a value to float.
    
    Handles None, empty strings, and comma-separated numbers.
    
    Args:
        value: Value to convert
        
    Returns:
        Float value or None on failure
    """
    if value is None:
        return None
    
    try:
        s = str(value).strip()
        if s == "" or s.lower() in ("null", "none", "nan"):
            return None
        
        # Remove commas (thousand separators)
        s = s.replace(",", "")
        
        return float(s)
    except (ValueError, TypeError):
        return None


def safe_int(value) -> Optional[int]:
    """
    Safely convert a value to int.
    
    Args:
        value: Value to convert
        
    Returns:
        Int value or None on failure
    """
    f = safe_float(value)
    if f is None:
        return None
    return int(f)
