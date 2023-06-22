"""Generate the STAC Catalog."""
from geojson import Point, Feature, FeatureCollection, dump
import geojson
import geopandas as gpd
import requests
from pathlib import Path
import os
from s3fs import S3FileSystem
from dotenv import load_dotenv
import click
from shapely.ops import transform
from shapely.geometry import Polygon, LineString, Point
import pandas as pd
import numpy as np
import dask_geopandas
import dask.dataframe as dd
import dask.array as da
import geoparquet as gpq
from osgeo import gdal
from sklearn.preprocessing import OrdinalEncoder
import json


class ProcessGeoFiles:
    
    def __init__(self,
                 input_url: str=None,
                 input_options: dict=None,
                 bbox: list[int]=[-1413232,6288190,-365497,6914553],
                 output: str = None,
                 output_format: str = 'parquet',
                 bucket: str = 'haig-fras',
                 local_path: str='data/') -> None:
        
        """
            ProcessGeoFiles: class for download and convert geojson to different file formats

            Args:
                input_url (str, optional): input url if you want to download data from wms service.
                    Defaults to None.
                input_option (dict, optional): input dict for wms service. Defaults to None.
                bbox (list[int], optional): the limits that your data will be clipped. It can be overwrite
                    in the clip_data function. Format: [latmin, lonmin, latmax, lonmax].
                    Default to [-1413232,6288190,-365497,6914553].
                output (str, optional): output name of your file. Defaults to None.
                output_format (str, optional): output format of your files. Defaults to parquet.
                bucket (str, optional): bucket name, Default to haig-fras.
                local_path (str, optional): local path of your data.

            Returns:
                None
        """

        self.input_url = input_url
        self.input_options = input_options
        self.local_path = local_path
        self.output = output
        self.output_format = output_format

        self.bbox = bbox


        self.bucket = bucket

        load_dotenv()

        self.__jasmin_api_url = os.environ.get("JASMIN_API_URL")
        self.__jasmin_token = os.environ.get("JASMIN_TOKEN")
        self.__jasmin_secret = os.environ.get("JASMIN_SECRET")

    def download_wfs(self):

        """
            download_wfs: class for download wfs data based on your input url and options
            
            The output will be a geojson file
        """

        print('downloading data')

        r = requests.get(self.input_url, params=self.input_options)

        with open(f'{self.local_path}{self.output}.geojson', 'w') as f:
            dump(r.json(), f)

        print('data downloaded')

        with open(f'{self.local_path}{self.output}.geojson') as f:
            gj = geojson.load(f)

        for item in gj['features']:
            try:
                del item["properties"]['geom_200']
                del item["properties"]['geom_400']
                del item["properties"]['geom_800']
            except:
                print("no less resolution geom avaiable")

        with open(f'{self.local_path}{self.output}.geojson', 'w') as f:
            dump(gj, f)

    def clip_data(self,
                  round_polygons:int=None,
                  drop_columns:list[str] =[],
                  encode_values:bool=False) -> None:


        """
            clip_data: Clip geojson or geoparquet data. The output file will be saved in
                output format defined in the class instantiate.

            Args:
                round_polygons (int, optional): number of digits you want to round polygons.
                    Set to None if you not want to round polygons. Defaults to None.
                drop_columns (list, optional): list of column names that you want to drop in
                    the final file. Default to [].
                encode_values (bool, optional): a boolean value that represents if you want
                    to ordinal encoder string values to numbers. Default to False.

            Returns:
                None
        """


        print('opening geojson')
        df = gpd.read_file(f'{self.local_path}{self.output}.geojson')
        print('data opened')

        new_df = gpd.clip(gdf=df, mask=self.bbox, keep_geom_type=False)
        print('data clipped')

        if round_polygons != None:
            new_df = new_df.explode(index_parts=False)
            new_df.geometry = new_df.geometry.apply(lambda x: ProcessGeoFiles._round_coordinates(geom=x, ndigits=round_polygons))
            print('data rounded')

        self.file_clipped = f'clipped_{self.output}'

        self.df = new_df

        new_df.drop(columns=drop_columns, inplace=True)

        self.encode_values = encode_values
        if self.encode_values:
            self.encoder = {}
            columns = new_df.drop(columns='geometry').columns
            for column in columns:
                ordinal_encoder = OrdinalEncoder(categories = [list(new_df[column].unique())])
                ordinal_encoder.fit(new_df[[column]])
                new_df[column] = ordinal_encoder.transform(new_df[[column]])
                self.encoder[column] = ([list(new_df[column].unique())], [list(range(0,len(ordinal_encoder.categories[0])))])

            self.file_clipped = f'encoded_{self.output}'

            json_object = json.dumps(self.encoder, indent = 2)
            with open(f'{self.local_path}{self.file_clipped}.json', 'w') as f:
                f.write(json_object)


        new_df.to_file(f'{self.local_path}{self.file_clipped}.geojson', driver='GeoJSON')

        if self.output_format == 'parquet':
            gdf = gpd.read_file(f'{self.local_path}{self.file_clipped}.geojson')
            gdf.to_parquet(f'{self.local_path}{self.file_clipped}.{self.output_format}')


        print(f'{self.file_clipped}.{self.output_format} saved!')


    def convert_to_geotiff(self, nodata_value:float=-9999, pixel_size:float=0.01):

        """
            convert_to_geotiff: convert the geojson/geoparquet file to geotiff/COG.

            Args:
                nodata_value (float, optional): value that represent no data values
                pixel_size(float, optional): pixel size of your output file.

            Returns:
                None
        """


        if not self.encode_values:
            new_df = gpd.read_file(f'{self.local_path}{self.output}.geojson')
            self.encoder = {}
            columns = new_df.drop(columns='geometry').columns
            for column in columns:
                ordinal_encoder = OrdinalEncoder(categories = [list(new_df[column].unique())])
                ordinal_encoder.fit(new_df[[column]])
                new_df[column] = ordinal_encoder.transform(new_df[[column]])
                self.encoder[column] = ([list(new_df[column].unique())], [list(range(0,len(ordinal_encoder.categories[0])))])

            self.file_clipped = f'encoded_{self.output}'

            json_object = json.dumps(self.encoder, indent = 2)
            with open(f'{self.local_path}{self.file_clipped}.json', 'w') as f:
                f.write(json_object)

            new_df.to_file(f'{self.local_path}{self.file_clipped}.geojson', driver='GeoJSON')
            if self.output_format == 'parquet':
                gdf = gpd.read_file(f'{self.local_path}{self.file_clipped}.geojson')
                gdf.to_parquet(f'{self.local_path}{self.file_clipped}.{self.output_format}')

        self.pixel_size = pixel_size  # about 25 metres(ish) use 0.001 if you want roughly 100m
        self.nodata_value = nodata_value

        vector_fn = f'{self.local_path}{self.file_clipped}.geojson'

        # Filename of the raster Tiff that will be created
        raster_fn = f'{self.local_path}{self.file_clipped}.tif'

        # Open the data source and read in the extent
        source_ds = gdal.OpenEx(vector_fn)

        gdal.Rasterize(raster_fn,
                    source_ds,
                    format='GTIFF',
                    outputType=gdal.GDT_Byte,
                    creationOptions=["COMPRESS=LZW", "BIGTIFF=YES"],
                    noData=self.nodata_value,
                    # initValues=' ',
                    xRes=self.pixel_size,
                    yRes=-self.pixel_size,
                    allTouched=True,
                    burnValues=1)

        print(f'Saved {raster_fn} file')


    def convert_to_grid(self, spacing=10000, upload_jasmin=False):
        
        """
            convert_to_grid: convert geojson/geoparquet file to grid format (zarr).

            Args:
                spacing (int, optional): spacing of your grid data in meters. Default to 10000.
                upload_jasmin (bool, optional): if true, zarr file will be uploaded in jasmin.

            Returns:
                None
        """

        
        self.spacing = spacing

        if self.output_format == 'geoparquet':
            gdf = gpd.read_geoparquet('../data/{self.file_clipped}.{self.output_file}')
        else:
            gdf = gpd.read_file('../data/{self.file_clipped}.{self.output_file}')

        grid_points = self._create_grid(gdf)

        grid_points = dd.from_pandas(grid_points, npartitions=10)

        result_values = grid_points.apply(lambda x: ProcessGeoFiles._verify_points_in_polygon(x=x, gdf=gdf))
        result_values = result_values.compute()

        none_values = [None]*len(gdf.iloc[0].values)
        gdf = gdf.append(pd.DataFrame([none_values],index=[-1],columns=gdf.columns))

        result_values[result_values.isna()]=int(-1)
        result_values = result_values.astype(int)

        columns = gdf.drop(columns='geometry')

        ds = xr.Dataset(
            coords=dict(
                lon=('lon', self.dx),
                lat=('lat', self.dy),
            )
        )
        compressor = zarr.Blosc(cname="zlib", clevel=2, shuffle=1)
        encoding_settings = {}

        for column in columns:
            da_name = column
            da = xr.DataArray(
                gdf[column].loc[result_values].to_numpy().reshape(len(self.dx), len(self.dy)),
                name=da_name,
                dims=('lon', 'lat')
            )
            ds[da_name] = da
            encoding_settings[da_name] = {'_FillValue': '', 'compressor': compressor, 'chunks': (2500, 1000, 1000)}


        ds.to_zarr(f'{self.local_path}{self.output}', mode='w', encoding=encoding_settings, consolidated=True)

        if upload_jasmin:
            pass



    def upload_jasmine(self):

        """
            Upload file to jasmin
        """

        try:
            s3 = S3FileSystem(anon=False,
                            key=self.__jasmin_token,
                            secret=self.__jasmin_secret,
                            client_kwargs={"endpoint_url": self.__jasmin_api_url})

            remote_path = f"s3://{self.bucket}/geojson/{self.file_clipped}"
            with s3.open(remote_path, mode="wb", s3=dict(profile="default")) as remote_file:
                with open(f'data/{self.file_clipped}', mode="rb") as local_file:
                    remote_file.write(local_file.read())

            print("upload done")

        except Exception as e:
            print(e)
            raise


    def _create_grid(self, gdf):
        
        """
            create_grid: create grid based on your geojson/geoparquet file limits and
                description.

            Args:
                gdf (GeoDataFrame): geodataframe variable that will be used to convert to grid.

            Returns:
                grid: x,y grid
        """

        max_values = gdf.bounds.max()
        min_values = gdf.bounds.min()

        min_x = min_values.minx
        min_y = min_values.miny
        max_x = max_values.maxx
        max_y = max_values.maxy

        self.dx = nselfarange(min_x, max_x, self.spacing)
        self.dy = nselfarange(min_y, max_y, self.spacing)
        xv, yv = nselfmeshgrid(self.dx, self.dy, indexing='ij')

        xv_reshape = xv.reshape(xv.shape[0]*xv.shape[1])
        yv_reshape = yv.reshape(yv.shape[0]*yv.shape[1])

        df = pd.DataFrame(xv_reshape.T, columns=['x'])
        df['y'] = yv_reshape.T

        res = df.apply(lambda x: Point((x.x, x.y)), axis=1)

        return res

    def _round_coordinates(geom, ndigits=1):

        """
            round_coordinates: round the coordinates of features

            Args:
                ndigits (int, optional): number of digits your data will be rounded.

            Returns:
                rounded geom data
        """


        def _round_coords(x, y, z=None):
            x = round(x, ndigits)
            y = round(y, ndigits)

            if z is not None:
                z = round(x, ndigits)
                return (x,y,z)
            else:
                return (x,y)

        return transform(_round_coords, geom)

    def _verify_points_in_polygon(x, gdf):

        """
            verify_points_in_polygon: verify if grid points are inside a polygon

            Args:
                gdf (GeoDataFrame): geodataframe variable that will be used to convert to grid.

            Returns:
                values if the data are included inside the polygon
        """

        try:
            value = gdf[x.within(gdf.geometry)]
            if value.empty:
                return
            else:
                return value.index[0]

        except Exception as e:
            print(e)
            return
