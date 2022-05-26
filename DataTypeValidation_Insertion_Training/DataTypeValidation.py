import shutil
from os import listdir
import os
import pandas as pd
import pymongo
from application_logging.logger import App_Logger


class dBOperation:
    """
      This class shall be used for handling all the SQL operations.

      Written By: Sayan Saha
      Version: 1.0
      Revisions: None

      """
    def __init__(self):
        try:
            self.path = 'Training_Database/'
            self.badFilePath = "Training_Raw_files_validated/Bad_Raw"
            self.goodFilePath = "Training_Raw_files_validated/Good_Raw"
            self.logger = App_Logger()
            self.client = pymongo.MongoClient(
                "mongodb+srv://mice:mice@mice-protein.4qbu1.mongodb.net/?retryWrites=true&w=majority")
        except Exception as e:
            raise e





    def insertIntoCollectionGoodData(self,collection):

        """
                               Method Name: insertIntoCollectionGoodData
                               Description: This method inserts the Good data files from the Good_Raw folder into the
                                            above created table.
                               Output: None
                               On Failure: Raise Exception

                                Written By: Sayan Saha
                               Version: 1.0
                               Revisions: None

        """
        dataset_db='training_dataset'
        logging_db='training_logs'
        logging_collection='DbInsertLog'

        goodFilePath= self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        database = self.client[dataset_db]
        collection = database[collection]


        for file in onlyfiles:
            try:
                df=pd.read_csv(goodFilePath+'/'+file)
                for i, row in df.iterrows():
                    collection.insert_one(dict(row))

                self.logger.log(logging_db, logging_collection, 'INFO', " %s: File loaded successfully!!" % file)

            except Exception as e:

                self.logger.log(logging_db, logging_collection, 'ERROR', "Error while inserting into table: %s " % e)
                shutil.move(goodFilePath+'/' + file, badFilePath)
                self.logger.log(logging_db, logging_collection, 'ERROR', "File Moved to Training_Raw_files_validated/Bad_Raw Successfully %s" % file)



    def selectingDatafromcollectionintocsv(self, collection):

        """
                               Method Name: selectingDatafromcollectionintocsv
                               Description: This method exports the data in GoodData table as a CSV file. in a given location.
                                            above created .
                               Output: None
                               On Failure: Raise Exception

                                Written By: Sayan Saha
                               Version: 1.0
                               Revisions: None

        """
        self.fileFromDb = 'Training_FileFromDB/'
        self.fileName = 'InputFile.csv'
        dataset_db='training_dataset'
        logging_db='training_logs'
        logging_collection='ExportToCsv'
        try:

            #Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            database=self.client[dataset_db]
            collection=database[collection]
            df = pd.DataFrame(collection.find({}, {'_id': 0}))
            df.to_csv(self.fileFromDb + self.fileName, header=True, index=None)


            self.logger.log(logging_db, logging_collection, 'INFO', "File exported successfully!!!")

        except Exception as e:
            self.logger.log(logging_db, logging_collection, 'ERROR', "File exporting failed. Error : %s" %e)
            raise e





