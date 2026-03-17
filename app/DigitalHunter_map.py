import matplotlib.pyplot as plt
import geopandas as gpd


def plot_map_with_geometry(coords,
                           shapefile_path="ne_50m_admin_0_countries.shp"):
    countries = gpd.read_file(shapefile_path)

    fig, ax = plt.subplots(figsize=(8, 10))

    xmin, xmax, ymin, ymax = 34, 36, 29.5, 34
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    ax.set_aspect('equal', adjustable='datalim')

    aoi = countries.cx[xmin:xmax, ymin:ymax]
    aoi.plot(ax=ax, color="lightgray", edgecolor="white")

    for idx, row in aoi.iterrows():
        centroid = row.geometry.centroid
        ax.annotate(text=row['ADMIN'], xy=(centroid.x, centroid.y),
                    ha='center', fontsize=8, color='black')

    if len(coords) == 1:
        x, y = coords[0]
        ax.plot(x, y, marker='o', color='red', markersize=8, label='Point')
    elif len(coords) > 1:
        x_coords, y_coords = zip(*coords)
        ax.plot(x_coords, y_coords, color='blue', linewidth=2, label='Path')
        ax.scatter(x_coords, y_coords, color='red', s=30)  # הצגת הנקודות על הקו

    plt.title('Map View')
    plt.legend()
    plt.show()


# if __name__ == '__main__':
#     # my_points = [(35.08, 33.09)]
#     my_points = [(35.0, 32.0), (35.2, 32.5), (35.5, 33.0)]
#
#     plot_map_with_geometry(my_points)