from rasterio.enums import Resampling
import rioxarray
import numpy as np
from geojson import Point, Feature, FeatureCollection, dump

from pathlib import Path
import os
from s3fs import S3FileSystem
from dotenv import load_dotenv
import click

load_dotenv()

class ProcessBathymetryGeoJson:
    
    def __init__(self,
                 input: str = "ETOPO_2022_v1_60s_N90W180_bed.nc",
                 bbox=[20, 60, -30, 20],
                 downscale_factor: float = 1/8,
                 output: str = "bathymetry",
                 bucket: str = 'haig-fras') -> None:
        
        """
            ProcessBathymetryGeoJson: class for convert netcdf to geotiff

            Args:
                input (str): input file that will be used to convert the data.
                bbox (list[int], optional): the limits that your data will be clipped. It can be overwrite
                    in the clip_data function. Format: [latmin, lonmin, latmax, lonmax].
                    Default to [-1413232,6288190,-365497,6914553].
                output (str, optional): output name of your file. Defaults to None.
                bucket (str, optional): bucket name, Default to haig-fras.

            Returns:
                None
        """

        self.input = input
        self.bbox = bbox
        self.output = output
        self.downscale_factor = downscale_factor
        self.bucket = bucket

        self.__jasmin_api_url = os.environ.get("JASMIN_API_URL")
        self.__jasmin_token = os.environ.get("JASMIN_TOKEN")
        self.__jasmin_secret = os.environ.get("JASMIN_SECRET")

    def open_clip_data(self):
        
        """
            open_clip_data: open the input file, clip it and save in class
        """

        self.xds = rioxarray.open_rasterio(f"data/{self.input}")
        print(f"Xarray shape before clip: {self.xds.shape}")

        # print(self.xds.rio.crs)
        self.crs = self.xds.rio.crs

        self.xds = self.xds.copy().where(self.xds.y > self.bbox[0], drop=True) \
                        .where(self.xds.y < self.bbox[1], drop=True) \
                        .where(self.xds.x > self.bbox[2], drop=True) \
                        .where(self.xds.x < self.bbox[3], drop=True) \

        print(f"Xarray shape after clip: {self.xds.shape}")

    def downscale_data(self):

        """
            downscale_data: downscale data to make the file smaller
        """


        print(f"Xarray shape before downsample: {self.xds.shape}")

        new_width = self.xds.rio.width * self.downscale_factor
        new_height = self.xds.rio.height * self.downscale_factor
        self.xds.rio.write_crs(self.crs, inplace=True)

        self.xds = self.xds.rio.reproject(self.xds.rio.crs,
                                     shape=(int(new_height),int(new_width)),
                                     resampling=Resampling.bilinear)

        print(f"Xarray shape after downsample {self.downscale_factor} times: {self.xds.shape}")

    def save_geojson_data(self):

        """
            save_geojson_data: save your clipped and downscale file to output geojson
        """

        features = []
        for i, x in enumerate(self.xds.x):
            for j, y in enumerate(self.xds.y):
                features.append(Feature(geometry=Point((round(float(x),1), round(float(y), 1))),
                                        properties={"d": int(self.xds[0,j,i])})
                                )

        self.feature_collection = FeatureCollection(features)

        with open(f"data/{self.output}.geojson", 'w') as f:
            dump(self.feature_collection, f)

    def save_asc_data(self):

        """
            save_asc_data: save your clipped and downscale file to output asc format
        """


        header = f"ncols     {int(self.xds.rio.height)}\n"
        header += f"nrows    {int(self.xds.rio.width)}\n"
        header += f"xllcorner {round(float(self.xds.x[0]), 2)}\n"
        header += f"yllcorner {round(float(self.xds.y[0]), 2)}\n"
        header += f"cellsize {round(float(self.xds.y[0] - self.xds.y[1]), 2)}\n"
        header += f"NODATA_value {-9999}"

        np.savetxt(f"data/{self.output}.asc",
                   self.xds.to_numpy()[0].astype(int),
                   header=header,
                   fmt="%1.0f",
                   comments='')

    def save_tif_data(self):
        """
            save_tif_data: save your clipped and downscale file to output tif
        """

        self.xds.rio.to_raster(f"data/{self.output}.tif")

    def upload_jasmine(self, extension="asc"):

        """ Upload reprojected cog files to jasmin

        Args:
            extension (str): output format of your saved data.
        """

        try:
            s3 = S3FileSystem(anon=False,
                            key=self.__jasmin_token,
                            secret=self.__jasmin_secret,
                            client_kwargs={"endpoint_url": self.__jasmin_api_url})

            output = f"{self.output}.{extension}"

            remote_path = f"s3://{self.bucket}/asc/{output}"
            with s3.open(remote_path, mode="wb", s3=dict(profile="default")) as remote_file:
                with open(f'data/{output}', mode="rb") as local_file:
                    remote_file.write(local_file.read())

            print("upload done")

        except Exception as e:
            print(e)
            raise
