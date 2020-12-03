import os
import boto3
import pandas as pd
import re
import arcpy
import zipfile


class jge_Catalog:
    """
    class for the jge catalog data management
    """

    def __init__(self, table="jge-catalog-test-03"):
        self.__table = table  # dynamodb table holding the jge catalog
        self.__ds = self.__queryCatalog__()  # returns a list of dicts.
        # read catalog features
        self.__Features = self.__getCatalogFeatures__(self.__ds)
        # read catalog metadata
        self.__Metadata = self.__getCatalogMetadata__(self.__ds)
        self.FeaturesCount = self.__Features.shape[0]  # num. of features
        return

    def getCatalogName(self):
        """
        return catalog table name
        """
        return self.__table

    def __queryCatalog__(self):
        """
        Query the Catalog and return an iterable dataset
        """
        # Creating the DynamoDB Table Resource
        dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
        table = dynamodb.Table(self.__table)

        response = table.scan()
        list_dict = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            list_dict.extend(response['Items'])

        return list_dict

    def __getCatalogMetadata__(self, dataset):
        """
        return a dataframe with the following columns:
         (1) feature-name
         (2) s3-versionID
         (3) fields (metadata)
        """
        df = pd.DataFrame(columns=['feature-name', 's3-versionID', 'fields'])
        for ds in dataset:
            row_data = {
                'feature-name': ds['feature-name'],
                's3-versionID': ds['s3-versionID'],
                'fields': ds['fields'][0]
            }
            df = df.append(row_data, ignore_index=True)
        return df

    def __getCatalogFeatures__(self, list_dict):
        """
        Return a dataframe with the following columns:
         (1) feature-name
         (2) s3-versionID
         (3) feature-type
         (4) s3-file-gdb-zip-location
        """
        df = pd.DataFrame(columns=['feature-name', 's3-versionID', 'feature-type', 's3-file-gdb-zip-location'])
        for dict_entry in list_dict:
            row_data = {
                'feature-name': dict_entry['feature-name'],
                's3-versionID': dict_entry['s3-versionID'],
                'feature-type': dict_entry['feature-type'],
                's3-file-gdb-zip-location': dict_entry['s3-file-gdb-zip-location']
            }
            df = df.append(row_data, ignore_index=True)
        return df

    def getFeatures(self):
        '''
        Return all Features in the catalog as a DataFrame
        '''
        return self.__Features

    def getMetaData(self, featureName, versionID):
        '''
        Return metadata of a featureName and versionID
        '''
        df = self.__Metadata
        cond1 = (df['feature-name'] == featureName)
        cond2 = (df['s3-versionID'] == versionID)
        result = []
        try:
            result = df.loc[cond1 & cond2, 'fields'].reset_index(drop=True)
            result = result.values
        except IndexError as ie:
            print(f'Non existent Index: {versionID} -- {ie}')
        except KeyError as ke:
            print(f'Non existent Key: {versionID} -- {ke}')
        return result

    def getObjectsByVersionID(self, versionID):
        """
        By using versionID which is unique, returns a dictionary
        with Feature Name and Feature Type members
        """
        df = self.__Features
        db = {}
        res = df.loc[df['s3-versionID'] == versionID,
                     ['feature-name',
                      'feature-type',
                      's3-file-gdb-zip-location']]
        if len(res) == 0:
            res = db
        else:
            for i in list(res.index):
                # check if the key (versionID) in db. if it's in, append
                if (versionID) in db:
                    db[versionID].append(res.loc[i].values)
                # else new a key
                else:
                    db[versionID] = [res.loc[i].values]
        return db

    def getObjectsByFeatureType(self, featureType):
        """
        By using featureType, returns a dictionary
        with Feature Name and s3 versionID members
        """
        df = self.__Features
        db = {}
        res = df.loc[df['feature-type'] == featureType,
                     ['feature-name',
                      's3-versionID',
                      's3-file-gdb-zip-location']]
        if len(res) == 0:
            res = db
        else:
            for i in list(res.index):
                # check if the key (featureType) in db. if it's in, append
                if (featureType) in db:
                    db[featureType].append(res.loc[i].values)
                # else new a key
                else:
                    db[featureType] = [res.loc[i].values]
        return db

    def __extractBucketAndKey__(self, s3_location):
        pattern = r's3:\/\/(.*?)\/(.*)'
        bucket_and_key = re.compile(pattern)
        m = bucket_and_key.match(s3_location)
        bucket, key = m[1], m[2]
        return bucket, key

    def __download_s3_zip__(self, versionID):
        """
        Download a zipped gdb file from the s3 bucket but
        referred in the catalog and unzip the .zip into a
        temp folder for further processing/data extraction
        """
        # step 1: search for the zip in dynamodb using versionID
        df = self.getFeatures()
        c1 = (df['s3-versionID'] == versionID)
        s3_key = df.loc[c1, ['s3-file-gdb-zip-location']].head(1).values
        bucket, key = self.__extractBucketAndKey__(s3_key[0][0])
        # step 2: extract the zip into a temp folder
        if not (os.path.exists('temp') and os.path.isdir('temp')):
            try:
                os.mkdir('temp')
            except OSError as e:
                print(f'*** error *** {e}')
        s3 = boto3.resource('s3')
        temp_folder = 'temp/'
        temp_file = os.path.split(key)[1]
        file2download = temp_folder+temp_file
        s3.meta.client.download_file(bucket, key, file2download)
        cur_dir = os.getcwd()
        os.chdir(temp_folder)
        file_name = os.path.abspath(temp_file)
        zip_ref = zipfile.ZipFile(file_name)  # create zipfile object
        zip_ref.extractall('.')  # extract file to dir
        zip_ref.close()  # close file
        os.remove(file_name)  # delete zipped file
        # step 3: return temp folder and gdb filename
        file_dir, file_zip = os.path.split(file_name)
        file_gdb = os.path.join(file_dir, os.path.splitext(file_zip)[0])
        os.chdir(cur_dir)
        return file_gdb

    def __compileDataset__(self, abs_path_file_gdb, versionID):
        dataset = [{}]
        arcpy.env.workspace = abs_path_file_gdb

        # compile all Raster Datasets stored in the gdb
        rasters = arcpy.ListRasters("*", "ALL")
        for i, in_raster in enumerate(rasters):
            try:
                in_raster_arr = arcpy.RasterToNumPyArray(in_raster)
                if in_raster not in dataset[0]:
                    dataset[0][in_raster] = {
                        's3-versionID': versionID,
                        'feature-type': 'Raster Dataset',
                        'shape': in_raster_arr.shape,
                        'columns': [],
                        'data': [in_raster_arr]
                    }
            except arcpy.ExecuteError:
                print(f"File {in_raster} couldn't be read")

        # compile all Feature Classes stored in the gdb
        vectors = arcpy.ListFeatureClasses('*', 'All')
        for i, in_vector in enumerate(vectors):
            try:
                # to get a handle of the columns
                desc = arcpy.Describe(in_vector)
                fields = [field.name for field in desc.fields]
                in_vector_arr = arcpy.da.TableToNumPyArray(
                    in_vector, fields, null_value='-', skip_nulls=True)
                if in_vector not in dataset[0]:
                    dataset[0][in_vector] = {
                        's3-versionID': versionID,
                        'feature-type': 'Feature Class',
                        'shape': in_vector_arr.shape,
                        'columns': fields,
                        'data': [in_vector_arr]
                    }
            except arcpy.ExecuteError:
                print(f"File {in_vector} couldn't be read")
        arcpy.env.workspace = None
        return dataset

    def retrieveDataset(self, versionID):
        """
        Extract all Datasets (Raster and Vector) from a
        particular versionID. It returns a dictionary:
        dataset = [
            {
                'rasterName': {
                's3-versionID': 'versionID1',
                'feature-type': 'Raster Dataset',
                'shape': (),
                'columns': [],
                'data': []
                },
                'vectorName': {
                    's3-versionID': 'versionID9999',
                    'feature-type': 'Feature Class',
                    'shape': (),
                    'columns': [],
                    'data': []
                }
            }
        ]
        This is implemented for convenience as every
        geoDatabase may contain several raster and vector
        files. It uses the arcPy library.
        """
        # file_gdb is the absolute path of the gdb
        file_gdb = self.__download_s3_zip__(versionID)

        # compile all objects from the file_gdb
        dataset = self.__compileDataset__(file_gdb, versionID)
        arcpy.management.Delete(file_gdb)

        return dataset

    def dumps(self):
        """
        Dump the entire catalog as a JSON structure
        """
        # call the query catalog
        ds = self.__queryCatalog__()
        return ds

# class ds_Catalog:
#     'class for the ds models catalog management'

#     def __init__(self, table='ds-model-catalog-test-01'):
#         self.__table = table  # dynamodb table holding the ds catalog
#         return

#     def getCatalogName(self):
#         """
#         return catalog table name
#         """
#         return self.__table

#     def dumps(self):
#         """
#         Dumps the entire catalog as a JSON structure
#         Returns an interable dataset
#         """
#         # Creating the DynamoDB Table Resource
#         dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
#         table = dynamodb.Table(self.__table)

#         response = table.scan()
#         dataset = response['Items']

#         while 'LastEvaluatedKey' in response:
#             response = table.scan(
#                 ExclusiveStartKey=response['LastEvaluatedKey']
#             )
#             dataset.extend(response['Items'])

#         return dataset
